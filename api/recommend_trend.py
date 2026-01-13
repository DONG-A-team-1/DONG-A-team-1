"""
트렌드 추천 전용 API (MMR)
2026.01.13

1. 트렌드 기사 중 제목 이상치 필터
2. 유사한 기사는 하나만
"""


import numpy as np
from sklearn.metrics.pairwise import cosine_similarity # 문장의 의미가 얼마나 비슷한지 계산하는 함수

# 시간 관련 처리용 (최근 3일 기사 같은 거 계산할 때 필요)
from datetime import timedelta, timezone

from util.elastic import es
from util.logger import Logger
logger = Logger().get_logger(__name__)

# 한국 시간대 (UTC+9)
KST = timezone(timedelta(hours=9))
# 기사 데이터가 들어있는 ES 인덱스 이름
ARTICLE_INDEX = "article_data"


# MMR 알고리즘 (비슷한 기사 제거용)
def mmr_rerank(candidates, embeddings, scores, top_k, lambda_=0.55):
    """
    MMR(Maximal Marginal Relevance) 알고리즘
    - 점수가 높은 기사 위주
    - 이미 고른 기사와 너무 비슷한 내용이면 점수를 깎아서
    - 서로 다른 내용의 기사들이 골고루 나오게 만드는 방법

    예시:
    - A 기사: 삼성 주가 상승
    - B 기사: 삼성 주가 또 상승
    → 둘 다 점수가 높아도 하나만
    """

    # 최종으로 선택된 기사들
    selected = []
    # 선택된 기사들의 인덱스 번호
    selected_idx = []
    # 아직 선택되지 않은 기사 인덱스들
    remaining = list(range(len(candidates)))

    # 원하는 개수(top_k)만큼 반복
    while len(selected) < top_k and remaining:
        mmr_scores = []

        for i in remaining:
            # 해당 기사의 중요도 점수 (트렌드 + 신뢰도)
            relevance = scores[i]
            # 아직 아무 기사도 선택 안 했으면
            if not selected_idx:
                diversity_penalty = 0.0
            else:
                # 이미 뽑힌 기사들과의 '내용 유사도' 계산
                sim = cosine_similarity(
                    embeddings[i].reshape(1, -1),
                    np.array([embeddings[j] for j in selected_idx])
                ).max()
                # 비슷할수록 패널티 증가
                diversity_penalty = sim
            """
            # MMR 점수 계산 relevance는 높을수록 좋고 diversity_penalty는 낮을수록 좋음
            “기사는 중요하지만 이미 보여준 기사와 비슷하면 패널티 점수 부여
            """
            mmr_score = lambda_ * relevance - (1 - lambda_) * diversity_penalty
            mmr_scores.append((mmr_score, i))

        # 가장 점수가 높은 기사 선택
        _, best_idx = max(mmr_scores)

        selected.append(candidates[best_idx])
        selected_idx.append(best_idx)
        remaining.remove(best_idx)

    return selected


# 트렌드 추천 메인 함수
def recommend_trend_articles(limit: int = 20):
    """
    메인 트렌드 기사 추천 함수
    ------------------
    1. 최근 3일간 기사만 가져오고
    2. 트렌드 점수가 높은 기사들을 후보로 모은 뒤
    3. 제목이 너무 이상한 기사들을 제거하고
    4. MMR로 비슷한 기사 제거
    5. 최종 트렌드 기사 리스트 반환
    """

    logger.info("[TREND-RECOMMEND] start")

    # 제목 필터 설정

    # 제목이 너무 짧으면 이상함 -> 한겨레
    MIN_TITLE_LEN = 12

    # 이런 단어가 들어가면 트렌드 기사로 부적절하다고 판단, 나중에 임의로 채우자
    BAN_WORDS = []

    # 1. 후보 기사 조회 (최근 3일 + 트렌드 점수 높은 순)
    res = es.search(
        index=ARTICLE_INDEX,
        size=100,  # 우선 상위 100개만 가져옴
        query={
            "range": {
                "collected_at": {"gte": "now-3d"}  # 최근 3일
            }
        },
        sort=[
            {"article_label.trend_score": {"order": "desc"}}
        ],
        _source=[
            "article_id",
            "article_title",
            "article_label.trend_score",
            "article_label.article_trust_score",
            "article_embedding",
            "article_img",
            "press",
            "collected_at"
        ]
    )

    hits = res.get("hits", {}).get("hits", [])

    if not hits:
        logger.warning("[TREND-RECOMMEND] no candidates")
        return []

    logger.info("[TREND-RECOMMEND] raw candidates=%d", len(hits))

    # 2. 후보 기사 정제 + 점수 계산
    candidates = []
    embeddings = []
    scores = []

    for h in hits:
        source = h.get("_source", {})
        label = source.get("article_label", {})

        title = source.get("article_title", "").strip()

        # 1. 제목이 너무 짧으면 제외
        if len(title) < MIN_TITLE_LEN:
            continue

        # 2. 금칙어가 들어가 있으면 제외
        if any(bad in title for bad in BAN_WORDS):
            continue

        # 3. 기사 내용 임베딩이 없으면 제외
        emb = source.get("article_embedding")
        if emb is None:
            continue

        # 트렌드 점수
        trend_score = label.get("trend_score", 0.0)

        # 기사 신뢰도 점수
        trust_score = label.get("article_trust_score", 0.0)

        # 최종 점수 계산
        # 트렌드를 더 중요하게 봄 (70% 비중)
        final_score = 0.7 * trend_score + 0.3 * trust_score

        candidates.append(h)
        embeddings.append(np.array(emb))
        scores.append(final_score)

    if not candidates:
        logger.warning("[TREND-RECOMMEND] no candidates after filtering")
        return []

    logger.info("[TREND-RECOMMEND] filtered candidates=%d", len(candidates))

    # 3. MMR 적용 (비슷한 기사 제거)
    reranked = mmr_rerank(
        candidates=candidates,
        embeddings=embeddings,
        scores=scores,
        top_k=limit,
        lambda_=0.55  # 값이 낮을수록 다양성 강조
    )

    # 4. 프론트 형식으로 변환
    results = []
    for h in reranked:
        source = h["_source"]
        label = source.get("article_label", {})

        results.append({
            "article_id": source.get("article_id"),
            "title": source.get("article_title"),
            "trend_score": label.get("trend_score", 0.0),
            "trust_score": label.get("article_trust_score", 0.0),
            "collected_at": source.get("collected_at"),
            "image": source.get("article_img"),
            "press": source.get("press")
        })

    logger.info("[TREND-RECOMMEND] done count=%d", len(results))
    return results

"""
트렌드 추천 전용 API (MMR) - 메인 콘텐츠
2026.01.13

설계 원칙
1. 트렌드 점수가 존재하는 기사만 대상
2. 제목이 기사답지 않게 너무 짧으면 제거
3. 유사 기사 과도 중복 방지 (MMR)
4. 최소 N개(min_k)는 무조건 보장 (서비스 안전장치) -> 데이터가 쌓이지 않았을 때 유사도 신경 안씀
"""

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from datetime import timedelta, timezone

from util.elastic import es
from util.logger import Logger

logger = Logger().get_logger(__name__)

# 한국 시간대 (UTC + 9)
KST = timezone(timedelta(hours=9))

# 기사 인덱스
ARTICLE_INDEX = "article_data"


# -------------------------------------------------
# MMR (Maximal Marginal Relevance)
# -------------------------------------------------
def mmr_rerank(
    candidates,
    embeddings,
    scores,
    top_k,
    lambda_=0.55,
    min_k=5,
    sim_threshold=0.9
):
    """
    MMR 알고리즘 (서비스 안정 버전)

    핵심 아이디어
    - relevance (트렌드+신뢰도)는 높게
    - 이미 선택된 기사와 너무 비슷하면 패널티
    - 만약 적합 기사가 없으면 미적합 기사로 최소 5개는 무조건 채우게

    파라미터
    - lambda_        : 점수 vs 다양성 균형
    - min_k          : 최소 보장 기사 수 - 현재 5 설정
    - sim_threshold  : 이 이상이면 '거의 같은 기사'
    """

    selected = []
    selected_idx = []
    remaining = list(range(len(candidates)))

    while len(selected) < top_k and remaining:
        mmr_scores = []

        for i in remaining:
            relevance = scores[i]

            if not selected_idx:
                diversity_penalty = 0.0
            else:
                sim = cosine_similarity(
                    embeddings[i].reshape(1, -1),
                    np.array([embeddings[j] for j in selected_idx])
                ).max()

                diversity_penalty = sim

                # 최소 개수 확보 전에는 유사도 컷 금지
                if diversity_penalty > sim_threshold and len(selected) >= min_k:
                    continue

            mmr_score = lambda_ * relevance - (1 - lambda_) * diversity_penalty
            mmr_scores.append((mmr_score, i))

        if not mmr_scores:
            break

        _, best_idx = max(mmr_scores)
        selected.append(candidates[best_idx])
        selected_idx.append(best_idx)
        remaining.remove(best_idx)

    return selected


# 트렌드 추천 메인 함수
def recommend_trend_articles(limit: int = 20):
    """
    트렌드 기사 추천 메인
    1. 최근 3일 기사 중
    2. article_label.trend_score 가 존재하는 기사만
    3. 제목 / 임베딩 기본 필터
    4. 트렌드+신뢰도 점수로 정렬
    5. MMR로 과도한 중복 제거 (최소 개수 보장) 5개
    """

    logger.info("[TREND-RECOMMEND] start")

    # 제목 필터 기준
    MIN_TITLE_LEN = 12
    BAN_WORDS = []  # 필요 시 확장

    # 1. ES 후보 기사 조회
    res = es.search(
        index=ARTICLE_INDEX,
        size=100,
        query={
            "bool": {
                "must": [
                    {"range": {"collected_at": {"gte": "now-1d"}}},
                    # {"exists": {"field": "article_label.trend_score"}},
                    {"term": {"status": 5}}
                ]
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
            "reporter", # 해정 추가 트렌드 메인
            "article_label", # 해정 추가 카테고리 가져와야해서
            "press",
            "collected_at"
        ]
    )

    hits = res.get("hits", {}).get("hits", [])
    if not hits:
        logger.warning("[TREND-RECOMMEND] no raw candidates")
        return []

    logger.info("[TREND-RECOMMEND] raw candidates=%d", len(hits))

    # 2. 후보 기사 정제
    candidates = []
    embeddings = []
    scores = []

    for h in hits:
        src = h.get("_source", {})
        label = src.get("article_label", {})

        title = src.get("article_title", "").strip()
        if len(title) < MIN_TITLE_LEN:
            continue

        if any(bad in title for bad in BAN_WORDS):
            continue

        emb = src.get("article_embedding")
        if not emb:
            continue

        trend_score = label.get("trend_score", 0.0)
        # trust_score = label.get("article_trust_score", 0.0)

        # 트렌드 점수로 컷하지 않음 (정렬만 사용)
        final_score =  trend_score

        candidates.append(h)
        embeddings.append(np.array(emb))
        scores.append(final_score)

    if not candidates:
        logger.warning("[TREND-RECOMMEND] no candidates after filtering")
        return []

    logger.info("[TREND-RECOMMEND] filtered candidates=%d", len(candidates))

    # 3. MMR 적용 (최소 개수 보장)
    reranked = mmr_rerank(
        candidates=candidates,
        embeddings=embeddings,
        scores=scores,
        top_k=limit,
        lambda_=0.55,
        min_k=5,
        sim_threshold=0.9
    )

    # 4. 프론트 전달 포맷
    results = []
    for h in reranked:
        src = h["_source"]
        label = src.get("article_label", {})

        results.append({
            "article_id": src.get("article_id"),
            "title": src.get("article_title"),
            "trend_score": label.get("trend_score", 0.0),
            "trust_score": label.get("article_trust_score", 0.0),
            "collected_at": src.get("collected_at"),
            "image": src.get("article_img"),
            "press": src.get("press"),
            "reporter": src.get("reporter", ""),  # 추가
            "category": label.get("category", "기타") # 카테고리 추가 해정
        })

    logger.info("[TREND-RECOMMEND] done count=%d", len(results))
    return results
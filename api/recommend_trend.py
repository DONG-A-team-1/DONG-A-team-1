"""
트렌드 추천 전용 API (MMR)
2026.01.13
- 계산된 트렌드 점수를 어떻게 보여줄지 결정
- 점수 계산 파이프라인과 분리됨

목표
1. 트렌드 기사만 고른다
2. 제목 이상한 기사 제거
3. 거의 같은 기사 x
"""

import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from datetime import timedelta, timezone
from util.elastic import es
from util.logger import Logger

logger = Logger().get_logger(__name__)

# 한국 시간대 (UTC + 9)
KST = timezone(timedelta(hours=9))

# 기사 데이터가 들어있는 ES 인덱스
ARTICLE_INDEX = "article_data"


# MMR (Maximal Marginal Relevance) 알고리즘
def mmr_rerank(candidates, embeddings, scores, top_k, lambda_=0.55):
    """
    MMR 알고리즘 설명
    기사 고르기 문제를 이렇게 생각하면 됨:
    - 점수 높은 기사 = 중요한 기사
    - 이미 뽑은 기사랑 너무 비슷하면 = 굳이 또 보여줄 필요 없음
    그래서
    - 중요도는 높게
    - 이미 뽑은 기사와 비슷하면 벌점

    lambda 값
    - 1에 가까울수록: 점수
    - 0에 가까울수록: 다양성
    """

    selected = []          # 최종 선택된 기사
    selected_idx = []      # 선택된 기사들의 인덱스
    remaining = list(range(len(candidates)))  # 아직 안 뽑힌 기사들

    while len(selected) < top_k and remaining:
        mmr_scores = []

        for i in remaining:
            # 이 기사가 얼마나 중요한지 (트렌드 + 신뢰도)
            relevance = scores[i]

            # 아직 아무 기사도 선택 안 했으면
            if not selected_idx:
                diversity_penalty = 0.0
            else:
                # 이미 뽑힌 기사들과의 "내용 유사도" 계산
                sim = cosine_similarity(
                    embeddings[i].reshape(1, -1),
                    np.array([embeddings[j] for j in selected_idx])
                ).max()

                diversity_penalty = sim

                # 너무 비슷한 기사면 아예 제외 (복붙 기사 방지 핵심)
                if diversity_penalty > 0.9:
                    continue

            # MMR 점수 공식
            mmr_score = lambda_ * relevance - (1 - lambda_) * diversity_penalty
            mmr_scores.append((mmr_score, i))

        if not mmr_scores:
            break

        # 가장 점수 높은 기사 선택
        _, best_idx = max(mmr_scores)
        selected.append(candidates[best_idx])
        selected_idx.append(best_idx)
        remaining.remove(best_idx)

    return selected


# 트렌드 추천 메인 함수
def recommend_trend_articles(limit: int = 20):
    """
    1. 최근 3일 기사 중
    2. 트렌드 점수가 실제로 존재하는 기사만
    3. 제목 이상치 제거
    4. 트렌드 점수 너무 낮은 기사 제거
    5. MMR 유사 기사 제거
    """

    logger.info("[TREND-RECOMMEND] start")

    # 제목 필터 기준

    # 제목이 너무 짧으면 기사 같지 않음
    MIN_TITLE_LEN = 12

    # 필요하면 나중에 추가할 금칙어 목록
    BAN_WORDS = []

    # 1. ES에서 후보 기사 조회
    res = es.search(
        index=ARTICLE_INDEX,
        size=100,
        query={
            "bool": {
                "must": [
                    # 최근 3일 기사만
                    {"range": {"collected_at": {"gte": "now-3d"}}},
                    # 트렌드 점수가 실제로 존재하는 기사만
                    {"exists": {"field": "article_label.trend_score"}}
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
            "press",
            "collected_at"
        ]
    )

    hits = res.get("hits", {}).get("hits", [])
    if not hits:
        logger.warning("[TREND-RECOMMEND] no candidates")
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

        # 제목 길이 필터
        if len(title) < MIN_TITLE_LEN:
            continue

        # 금칙어 필터
        if any(bad in title for bad in BAN_WORDS):
            continue

        emb = src.get("article_embedding")
        if emb is None:
            continue

        trend_score = label.get("trend_score", 0.0)
        trust_score = label.get("article_trust_score", 0.0)

        # 트렌드 점수가 너무 낮으면 제외
        if trend_score < 0.05:
            continue

        # 최종 점수 (트렌드 비중 높게)
        final_score = 0.7 * trend_score + 0.3 * trust_score

        candidates.append(h)
        embeddings.append(np.array(emb))
        scores.append(final_score)

    if not candidates:
        logger.warning("[TREND-RECOMMEND] no candidates after filtering")
        return []

    logger.info("[TREND-RECOMMEND] filtered candidates=%d", len(candidates))

    # 3. MMR 적용
    reranked = mmr_rerank(
        candidates=candidates,
        embeddings=embeddings,
        scores=scores,
        top_k=limit,
        lambda_=0.55
    )

    # 4. 프론트 전달용 결과 정리
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
            "press": src.get("press")
        })

    logger.info("[TREND-RECOMMEND] done count=%d", len(results))
    return results
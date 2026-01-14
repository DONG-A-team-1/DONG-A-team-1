from datetime import datetime, timezone, timedelta
from util.elastic import es
from util.database import SessionLocal
from sqlalchemy import text
from util.logger import Logger

import numpy as np
import json

"""
[안전 버전 패치 포인트 요약]

1. 추천 입력 기사 = status=5 만 사용
2. article_label 항상 .get() 접근
3. 트렌드 / 신뢰도 점수 None / 누락 방어
4. 새 환경에서도 KeyError 발생 불가
"""

logger = Logger().get_logger(__name__)
KST = timezone(timedelta(hours=9))


# -------------------------------------------------
# 유저 임베딩 업데이트 (기존 유지)
# -------------------------------------------------
def update_user_embedding(user_id):
    db = SessionLocal()

    article_row = db.execute(
        text("""
            SELECT sd.article_id, ps.preference_score
            FROM session_data sd
            JOIN preference_score ps ON ps.session_id = sd.session_id
            WHERE sd.user_id = :uid
            ORDER BY ps.occurred_at DESC
            LIMIT 1
        """),
        {"uid": user_id}
    ).fetchone()

    if not article_row:
        return

    article_id, preference_score = article_row
    preference_score = float(preference_score)

    resp = es.search(
        index="article_data",
        body={
            "_source": ["article_embedding"],
            "query": {"term": {"article_id": article_id}}
        }
    )

    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        return

    article_embedding = hits[0]["_source"].get("article_embedding")
    if not article_embedding or len(article_embedding) != 768:
        raise RuntimeError(f"Invalid article_embedding article_id={article_id}")

    old_emb = np.asarray(article_embedding, dtype=np.float32)

    resp = es.search(
        index="user_embeddings",
        body={
            "_source": ["embedding"],
            "query": {"term": {"user_id": user_id}}
        }
    )

    hits = resp.get("hits", {}).get("hits", [])

    if not hits:
        es.index(
            index="user_embeddings",
            id=user_id,
            document={
                "user_id": user_id,
                "embedding": old_emb.tolist(),
                "updated_at": datetime.now(KST)
            }
        )
        return

    user_embedding = hits[0]["_source"].get("embedding")
    if not user_embedding or len(user_embedding) != 768:
        raise RuntimeError(f"Invalid user_embedding user_id={user_id}")

    new_emb = np.asarray(user_embedding, dtype=np.float32)
    updated_embedding = 0.9 * old_emb + 0.1 * preference_score * new_emb

    es.update(
        index="user_embeddings",
        id=user_id,
        body={
            "doc": {
                "embedding": updated_embedding.tolist(),
                "updated_at": datetime.now(KST)
            }
        }
    )


# -------------------------------------------------
# 유저 기사 조회 (기존 유지)
# -------------------------------------------------
def user_articles(user_id):
    resp = es.search(
        index="user_embeddings",
        body={
            "_source": ["embedding"],
            "query": {"term": {"user_id": user_id}}
        }
    )

    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        return []

    query_vec = hits[0]["_source"]["embedding"]

    res = es.search(
        index="article_data",
        size=20,
        knn={
            "field": "article_embedding",
            "query_vector": query_vec,
            "k": 1000,
            "num_candidates": 2000,
            "filter": [
                {"term": {"status": 5}},
                {"range": {"collected_at": {"gte": "now-3d"}}}
            ]
        },
        _source=["article_id", "article_title", "collected_at"]
    )

    return [
        {
            "article_id": h["_source"].get("article_id"),
            "title": h["_source"].get("article_title"),
            "score": h["_score"],
            "collected_at": h["_source"].get("collected_at"),
        }
        for h in res.get("hits", {}).get("hits", [])
    ]


def recommend_articles(user_id: str, limit: int = 20):
    """
    유저별 추천 기사 생성 (안전 버전)

    - user_embeddings 인덱스 없을 때도 절대 에러 안 남
    - status=5 기사만 추천
    - article_label 누락 완전 방어
    """

    # -------------------------------------------------
    # 1. 유저 임베딩 존재 여부 확인 (🔥 핵심)
    # -------------------------------------------------
    if not es.indices.exists(index="user_embeddings"):
        has_user_embedding = False
        user_hits = []
    else:
        resp = es.search(
            index="user_embeddings",
            body={
                "_source": ["embedding"],
                "query": {"term": {"user_id": user_id}}
            }
        )
        user_hits = resp.get("hits", {}).get("hits", [])
        has_user_embedding = len(user_hits) > 0

    # -------------------------------------------------
    # 2. 후보 기사 조회
    # -------------------------------------------------
    if has_user_embedding:
        query_vec = user_hits[0]["_source"]["embedding"]

        res = es.search(
            index="article_data",
            size=100,
            knn={
                "field": "article_embedding",
                "query_vector": query_vec,
                "k": 100,
                "num_candidates": 500,
                "filter": [
                    {"term": {"status": 5}},
                    {"range": {"collected_at": {"gte": "now-3d"}}}
                ]
            },
            _source=[
                "article_id",
                "article_title",
                "article_label",
                "collected_at",
                "article_img"
            ]
        )
    else:
        res = es.search(
            index="article_data",
            size=100,
            query={
                "bool": {
                    "must": [
                        {"term": {"status": 5}},
                        {"range": {"collected_at": {"gte": "now-3d"}}}
                    ]
                }
            },
            sort=[{"article_label.trend_score": {"order": "desc"}}],
            _source=[
                "article_id",
                "article_title",
                "article_label",
                "collected_at",
                "article_img"
            ]
        )

    hits = res.get("hits", {}).get("hits", [])
    if not hits:
        return []
    filtered_hits = []

    for h in hits:
        src = h.get("_source", {})
        title = src.get("article_title", "").strip()

        # 1. 제목 너무 짧은 경우 제거
        if len(title) < 12:
            continue

        filtered_hits.append(h)

    hits = filtered_hits

    if not hits:
        return []
    # -------------------------------------------------
    # 3. 점수 범위 계산
    # -------------------------------------------------
    trend_scores = []
    trust_scores = []

    for h in hits:
        label = h["_source"].get("article_label", {})
        trend_scores.append(label.get("trend_score", 0.0))
        trust_scores.append(label.get("article_trust_score", 0.0))

    trend_min, trend_max = min(trend_scores), max(trend_scores)
    trust_min, trust_max = min(trust_scores), max(trust_scores)

    if has_user_embedding:
        emb_scores = [h["_score"] for h in hits]
        emb_min, emb_max = min(emb_scores), max(emb_scores)
    else:
        emb_min = emb_max = None

    def normalize(v, mn, mx):
        if mx == mn:
            return 0.0
        return (v - mn) / (mx - mn)

    # -------------------------------------------------
    # 4. 최종 점수 계산
    # -------------------------------------------------
    ranked = []

    for h in hits:
        src = h["_source"]
        label = src.get("article_label", {})

        trend = normalize(label.get("trend_score", 0.0), trend_min, trend_max)
        trust = normalize(label.get("article_trust_score", 0.0), trust_min, trust_max)

        if has_user_embedding:
            emb = normalize(h["_score"], emb_min, emb_max)
            final_raw = 0.4 * emb + 0.4 * trend + 0.2 * trust
        else:
            final_raw = 0.7 * trend + 0.3 * trust

        ranked.append({
            "article_id": src.get("article_id"),
            "title": src.get("article_title", ""),
            "article_img": src.get("article_img"),
            "final_score": int(round(final_raw * 100)),
            "collected_at": src.get("collected_at")
        })

    ranked.sort(key=lambda x: x["final_score"], reverse=True)
    return ranked[:limit]


if __name__ == "__main__":
    logger.info(
        json.dumps(
            recommend_articles("test_user"),
            ensure_ascii=False,
            indent=2
        )
    )
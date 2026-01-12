
from datetime import datetime, timezone, timedelta
from util.elastic import es
from util.database import SessionLocal
from sqlalchemy import text
from util.logger import Logger

import numpy as np
import json

"""
이 코드는 Elasticsearch의 dense_vector 필드를 `_source`를 통해 직접 읽는 구조

Elasticsearch 공식 API 스펙상 dense_vector는 항상 _source로 읽을 수 있다고 보장되지 않음

ES 버전 업그레이드, reindex, cluster 설정 변경 시 이 코드가 동작하지 않을 수 있음
무작정 리팩토링하거나 구조 변경하지 말 것. 문제 발생 시에만 구조 변경 필요
"""


logger = Logger().get_logger(__name__)
KST = timezone(timedelta(hours=9))

def update_user_embedding(user_id):
    result =[]
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

    body = {
        "_source": ["article_embedding"],
        "query": {
            "term": {
                "article_id": article_row[0]
            }
        }
    }
    logger.info(article_row[0])
    preference_score = float(article_row[1])

    resp = es.search(index="article_data", body=body)
    hits = resp.get("hits", {}).get("hits", [])
    article_embedding = hits[0]["_source"].get("article_embedding")
    if not article_embedding or len(article_embedding) != 768: # 빈 벡터 / None / 차원 깨짐 나오면 런타임에러 뜨게
        raise RuntimeError(
            f"Invalid article_embedding for article_id={article_row[0]}"
        )

    old_emb = np.asarray(article_embedding, dtype=np.float32)

    # logger.info(old_emb[:10])

    body = {
        "_source": ["embedding"],
        "query": {
            "term": {
                "user_id": user_id
            }
        }
    }
    resp = es.search(index="user_embeddings",  body=body)
    hits = resp.get("hits", {}).get("hits", [])
    # logger.info(hits)

    if not hits:
        logger.info("최초 임베딩 생성입니다")
        es.index(index="user_embeddings", id=user_id, document= {"user_id":user_id, "embedding": old_emb.tolist(),"updated_at":datetime.now(KST)})
    else:
        user_embedding = hits[0]["_source"].get("embedding") # 빈 벡터 / None / 차원 깨짐 나오면 런타임에러 뜨게
        if not user_embedding or len(user_embedding) != 768:
            raise RuntimeError(f"Invalid user_embedding for user_id={user_id}")

        new_emb = np.asarray(user_embedding, dtype=np.float32)
        updated_embedding = 0.9 * old_emb + 0.1 * preference_score * new_emb
        es.update(index="user_embeddings", id=user_id, body={"doc": {"user_id": user_id, "embedding": updated_embedding.tolist(), "updated_at": datetime.now(KST)}})
        # logger.info(updated_embedding[:10])

def update_session_embedding(session_id):
    db = SessionLocal()

    # 1. 세션 기준으로 article_id, preference_score, user_id 가져오기
    row = db.execute(
        text("""
            SELECT sd.user_id, sd.article_id, ps.preference_score
            FROM session_data sd
            JOIN preference_score ps ON ps.session_id = sd.session_id
            WHERE sd.session_id = :sid
            ORDER BY ps.occurred_at DESC
            LIMIT 1
        """),
        {"sid": session_id}
    ).fetchone()

    if not row:
        logger.warning("session_id=%s 에 대한 기록이 없습니다", session_id)
        return

    user_id, article_id, preference_score = row
    preference_score = float(preference_score)

    logger.info("session_id=%s user_id=%s article_id=%s", session_id, user_id, article_id)

    # 2. 기사 임베딩 가져오기 (new signal)
    resp = es.search(
        index="article_data",
        body={
            "_source": ["article_embedding"],
            "size": 1,
            "query": {"term": {"article_id": article_id}}
        }
    )
    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        logger.warning("article_embedding 없음 article_id=%s", article_id)
        return

    article_emb = np.asarray(hits[0]["_source"]["article_embedding"], dtype=np.float32)

    # 3. 기존 유저 임베딩 조회 (user_id 기준)
    resp = es.search(
        index="user_embeddings",
        body={
            "_source": ["embedding"],
            "size": 1,
            "query": {"term": {"user_id": user_id}}
        }
    )
    hits = resp.get("hits", {}).get("hits", [])

    # 4. 최초 생성 vs 업데이트
    if not hits:
        logger.info("최초 유저 임베딩 생성 user_id=%s", user_id)
        es.index(
            index="user_embeddings",
            id=user_id,
            document={
                "user_id": user_id,
                "embedding": article_emb.tolist(),
                "updated_at": datetime.now(KST)
            }
        )
        return

    user_emb = np.asarray(hits[0]["_source"]["embedding"], dtype=np.float32)

    # 5. 가중 업데이트 (EMA + preference)
    alpha = 0.1 * preference_score
    alpha = min(max(alpha, 0.0), 1.0)  # 안전 클램프

    updated_embedding = (1 - alpha) * user_emb + alpha * article_emb

    # (선택) L2 정규화
    norm = np.linalg.norm(updated_embedding)
    if norm > 0:
        updated_embedding /= norm

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

    logger.info("user_embedding updated user_id=%s session_id=%s", user_id, session_id)

def user_articles(user_id):
    body = {
        "_source": ["embedding"],
        "size": 1,
        "query": {"term": {"user_id": user_id}}
    }

    resp = es.search(index="user_embeddings", body=body)
    hits = resp["hits"]["hits"]
    if not hits:
        raise ValueError("No document found for user_id")

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
                {"range": {"collected_at": {"gte": "now-3d"}}}
            ]
        },
        source=["article_id", "article_title","collected_at"]
    )

    related_articles = [
        {
            "article_id": h["_source"].get("article_id"),
            "title": h["_source"].get("article_title"),
            "score": h["_score"],
            "collected_at": h["_source"].get("collected_at"),
        }
        for h in res.get("hits", {}).get("hits", [])
    ]
    return related_articles

def recommend_articles(user_id: str, limit: int = 20):
    """
    유저별 추천 기사 생성 함수

    기본 개념
    - 유저 임베딩이 있으면 개인화 추천
    - 없으면 콜드스타트 추천

    점수 정책
    - 개인화:
        final = 임베딩 0.4 + 트렌드 0.4 + 신뢰도 0.2
    - 콜드스타트:
        final = 트렌드 0.7 + 신뢰도 0.3

    점수 범위
    - 내부 계산: 0.0 ~ 1.0
    - 외부 반환: 0 ~ 100 정수
    """

    # -------------------------------------------------
    # 1. 유저 임베딩 존재 여부 확인
    # -------------------------------------------------
    resp = es.search(
        index="user_embeddings",
        body={
            "_source": ["embedding"],
            "size": 1,
            "query": {
                "term": {"user_id": user_id}
            }
        }
    )

    user_hits = resp.get("hits", {}).get("hits", [])
    has_user_embedding = len(user_hits) > 0

    logger.info(
        "[RECOMMEND] user_id=%s has_user_embedding=%s",
        user_id, has_user_embedding
    )

    # -------------------------------------------------
    # 2. 추천 후보 기사 조회
    # -------------------------------------------------
    if has_user_embedding:
        # 개인화 추천: 유저 임베딩 기반 KNN 검색
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
                    {"range": {"collected_at": {"gte": "now-30d"}}}
                ]
            },
            source=[
                "article_id",
                "article_title",
                "article_label.trend_score",
                "article_label.article_trust_score",
                "collected_at"
            ]
        )
    else:
        # 콜드스타트 추천: 트렌드 기준 정렬
        res = es.search(
            index="article_data",
            size=100,
            query={
                "range": {"collected_at": {"gte": "now-30d"}}
            },
            sort=[
                {"article_label.trend_score": {"order": "desc"}}
            ],
            source=[
                "article_id",
                "article_title",
                "article_label.trend_score",
                "article_label.article_trust_score",
                "collected_at"
            ]
        )

    hits = res.get("hits", {}).get("hits", [])
    if not hits:
        logger.warning("[RECOMMEND] no article candidates")
        return []

    # -------------------------------------------------
    # 3. 점수 정규화 함수
    # -------------------------------------------------
    def normalize(value, min_v, max_v):
        if max_v == min_v:
            return 0.0
        return (value - min_v) / (max_v - min_v)

    # -------------------------------------------------
    # 4. 트렌드 / 신뢰도 점수 범위 계산
    # -------------------------------------------------
    trend_scores = [
        h["_source"]["article_label"].get("trend_score", 0.0)
        for h in hits
    ]
    trust_scores = [
        h["_source"]["article_label"].get("article_trust_score", 0.0)
        for h in hits
    ]

    trend_min, trend_max = min(trend_scores), max(trend_scores)
    trust_min, trust_max = min(trust_scores), max(trust_scores)

    # 임베딩 점수 범위는 개인화일 때만 사용
    if has_user_embedding:
        embedding_scores = [h["_score"] for h in hits if "_score" in h]
        emb_min, emb_max = min(embedding_scores), max(embedding_scores)
    else:
        emb_min, emb_max = None, None

    # -------------------------------------------------
    # 5. 최종 점수 계산
    # -------------------------------------------------
    ranked = []

    for h in hits:
        src = h["_source"]
        article_id = src["article_id"]

        trend = normalize(
            src["article_label"].get("trend_score", 0.0),
            trend_min, trend_max
        )
        trust = normalize(
            src["article_label"].get("article_trust_score", 0.0),
            trust_min, trust_max
        )

        if has_user_embedding:
            emb = normalize(h["_score"], emb_min, emb_max)
            final_raw = (
                0.4 * emb +
                0.4 * trend +
                0.2 * trust
            )
        else:
            emb = None
            final_raw = (
                0.7 * trend +
                0.3 * trust
            )

        final_score = int(round(final_raw * 100))

        # 점수 계산 로그 (디버깅 핵심)
        logger.info(
            "[RECOMMEND_SCORE] user=%s article=%s emb=%s trend=%.4f trust=%.4f final=%d",
            user_id,
            article_id,
            f"{emb:.4f}" if emb is not None else "None",
            trend,
            trust,
            final_score
        )

        ranked.append({
            "article_id": article_id,
            "title": src.get("article_title", ""),
            "final_score": final_score,
            "trend_score": round(trend, 4),
            "trust_score": round(trust, 4),
            "collected_at": src.get("collected_at")
        })

    # -------------------------------------------------
    # 6. 정렬 및 개수 제한
    # -------------------------------------------------
    ranked.sort(key=lambda x: x["final_score"], reverse=True)
    return ranked[:limit]





if __name__ == '__main__':
    # update_user_embedding("lyj_0428")
    logger.info(json.dumps(user_articles("lyj_0428"),ensure_ascii=False,indent=4))


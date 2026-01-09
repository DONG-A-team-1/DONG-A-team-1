
from datetime import datetime, timezone, timedelta
from util.elastic import es
from util.database import SessionLocal
from sqlalchemy import text
from util.logger import Logger

import numpy as np
import json

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
        user_embedding = hits[0]["_source"].get("embedding")
        new_emb = np.asarray(user_embedding, dtype=np.float32)
        updated_embedding = 0.9 * old_emb + 0.1 * preference_score * new_emb
        es.update(index="user_embeddings", id=user_id, body={"doc": {"user_id": user_id, "embedding": updated_embedding.tolist(), "updated_at": datetime.now(KST)}})
        # logger.info(updated_embedding[:10])

def update_session_embedding(session_id):
    db = SessionLocal()

    # 1️⃣ 세션 기준으로 article_id, preference_score, user_id 가져오기
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

    # 2️⃣ 기사 임베딩 가져오기 (new signal)
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

    # 3️⃣ 기존 유저 임베딩 조회 (user_id 기준)
    resp = es.search(
        index="user_embeddings",
        body={
            "_source": ["embedding"],
            "size": 1,
            "query": {"term": {"user_id": user_id}}
        }
    )
    hits = resp.get("hits", {}).get("hits", [])

    # 4️⃣ 최초 생성 vs 업데이트
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

    # 5️⃣ 가중 업데이트 (EMA + preference)
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

if __name__ == '__main__':
    # update_user_embedding("lyj_0428")
    logger.info(json.dumps(user_articles("lyj_0428"),ensure_ascii=False,indent=4))


from util.database import SessionLocal , engine
from sqlalchemy import Table, MetaData
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.dialects.mysql import insert
from typing import Any, Dict, List
from elasticsearch import helpers
from util.elastic import es


metadata = MetaData()

article_data_tbl = Table(
    "article_data", metadata, autoload_with=engine
)

article_label_tbl = Table(
    "article_label", metadata, autoload_with=engine
)

topic_polarity_tbl = Table(
    "topic_polarity", metadata, autoload_with=engine
)

article_polarity_tbl = Table(
    "article_polarity", metadata, autoload_with=engine
)


def upsert_article(article_list):
    to_article_data =[]
    to_article_label =[]
    db = SessionLocal()

    query = {
        "_source": ["article_id", "article_title", "article_content","url","article_label"],
        "size": 1000,
        "query": {
            "terms": {"article_id": article_list}
        }
    }

    resp = es.search(index="article_data", body=query)
    hits = resp.get("hits", {}).get("hits", [])

    if not hits:
        return

    for h in hits:
        src = h.get("_source", {})
        article_id = src.get("article_id")
        if not article_id:
            continue

        article_id = src.get("article_id") or ""
        title = src.get("article_title") or ""
        url = src.get("url") or ""
        article_content = src.get("article_content") or ""
        label = src.get("article_label") or {}
        article_category = label.get("category") or ""
        article_trust_score = label.get("article_trust_score") or 0

        to_article_data.append({"article_id":article_id,"article_title":title,"article_url":url,"article_length":len(article_content)})
        to_article_label.append({"article_id":article_id,"article_category":article_category,"article_trust_score":article_trust_score})

    try:
        # --- article_data bulk upsert ---
        if to_article_data:
            stmt = insert(article_data_tbl).values(to_article_data)

            # PK(article_id) 제외하고 업데이트
            update_cols = {
                c.name: stmt.inserted[c.name]
                for c in article_data_tbl.columns
                if c.name != "article_id"
            }

            stmt = stmt.on_duplicate_key_update(**update_cols)
            db.execute(stmt)

        # --- article_label bulk upsert ---
        if to_article_label:
            stmt2 = insert(article_label_tbl).values(to_article_label)

            update_cols2 = {
                c.name: stmt2.inserted[c.name]
                for c in article_label_tbl.columns
                if c.name != "article_id"
            }

            stmt2 = stmt2.on_duplicate_key_update(**update_cols2)
            db.execute(stmt2)

        db.commit()
        return len(to_article_data), len(to_article_label)

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _existing_article_ids(db, ids) -> set[str]:
    if not ids:
        return set()
    rows = db.execute(
        select(article_data_tbl.c.article_id).where(article_data_tbl.c.article_id.in_(ids))
    ).fetchall()
    return {r[0] for r in rows}

def upsert_topic_polarity(topic_docs, *, fmt_prefix: str):
    to_topic = []
    to_article = []
    db = SessionLocal()

    try:
        for t in topic_docs or []:
            topic_id = str(t.get("topic_id") or "").strip()
            if not topic_id:
                continue

            topic_id = f"{fmt_prefix}_{topic_id}"
            calculated_at = t.get("calculated_at")
            if isinstance(calculated_at, str):
                calculated_at = datetime.fromisoformat(calculated_at.replace("Z", "+00:00"))

            to_topic.append({
                "topic_id": topic_id,
                "topic_name": t.get("topic_name", ""),
                "topic_rank": int(t.get("topic_rank") or 0),
                "calculated_at": calculated_at ,
            })

            def emit(items, pol):
                for a in items or []:
                    aid = str(a.get("article_id") or "").strip()
                    if not aid:
                        continue
                    to_article.append({
                        "article_id": aid,
                        "topic_id": topic_id,        # 컬럼 없으면 제거
                        "polarity": pol,
                        "intensity": float(a.get("intensity") or 0.0),
                    })

            emit(t.get("positive_articles"), "positive")
            emit(t.get("negative_articles"), "negative")
            emit(t.get("neutral_articles"), "neutral")

        # 1) topic upsert
        if to_topic:
            stmt = insert(topic_polarity_tbl).values(to_topic)
            update_cols = {
                c.name: stmt.inserted[c.name]
                for c in topic_polarity_tbl.columns
                if c.name not in ("es_id",)
            }
            db.execute(stmt.on_duplicate_key_update(**update_cols))

        # 2) article rows 중 DB에 존재하는 article_id만 남기기 (FK 방어)
        if to_article:
            ids = [r["article_id"] for r in to_article]
            exists = _existing_article_ids(db, ids)
            to_article = [r for r in to_article if r["article_id"] in exists]

        # 3) article upsert
        if to_article:
            stmt2 = insert(article_polarity_tbl).values(to_article)
            update_cols2 = {
                c.name: stmt2.inserted[c.name]
                for c in article_polarity_tbl.columns
                if c.name not in ("article_id", "topic_id")  # PK 구성에 맞게 조정
            }
            db.execute(stmt2.on_duplicate_key_update(**update_cols2))

        db.commit()
        return len(to_topic), len(to_article)

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def set_article_topic_polarity_single(
    topic_rows: List[Dict[str, Any]],
    *,
    index_name: str = "article_data",
    chunk_size: int = 500,
):
    """
    기사 1개당 topic_polarity 1개만 유지하는 방식.
    -> article_label.topic_polarity 를 길이 1짜리 배열로 '통째로 덮어씀'
    전제: ES _id == article_id
    """
    actions = []
    for r in topic_rows:
        aid = str(r.get("article_id") or "").strip()
        tid = str(r.get("topic_id") or "").strip()
        if not aid or not tid:
            continue

        item = {
            "topic_id": tid,
            "stance": str(r.get("stance") or "미정"),
            "intensity": float(r.get("intensity") or 0.0),
        }

        actions.append({
            "_op_type": "update",
            "_index": index_name,
            "_id": aid,
            "doc": {
                "article_label": {
                    "topic_polarity": [item]
                }
            }
        })

    if actions:
        helpers.bulk(es, actions, chunk_size=chunk_size, request_timeout=120)



if __name__ == "__main__":
    query = {
        "_source": ["article_id"],
        "size": 3000,
    }

    resp = es.search(index="article_data", body=query)
    hits = resp.get("hits", {}).get("hits", [])

    article_list = []
    for h in hits:
        src = h.get("_source", {})
        article_id = src.get("article_id")
        if not article_id:
            continue
        else:
            article_list.append(article_id)
    upsert_article(article_list)
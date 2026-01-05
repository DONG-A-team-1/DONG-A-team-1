from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Set

from sqlalchemy import Table, MetaData, select
from sqlalchemy.dialects.mysql import insert

from util.database import SessionLocal, engine
from util.elastic import es
from elasticsearch import helpers


metadata = MetaData()

article_data_tbl = Table("article_data", metadata, autoload_with=engine)
article_label_tbl = Table("article_label", metadata, autoload_with=engine)
topic_polarity_tbl = Table("topic_polarity", metadata, autoload_with=engine)
article_polarity_tbl = Table("article_polarity", metadata, autoload_with=engine)


# -------------------------
# 공통 유틸
# -------------------------
def _parse_iso_dt(x: Any) -> Any:
    """topic_docs의 calculated_at(iso str) -> datetime"""
    if x is None:
        return None
    if isinstance(x, datetime):
        return x
    if isinstance(x, str):
        # "Z" 들어오면 fromisoformat이 못 읽어서 보정
        return datetime.fromisoformat(x.replace("Z", "+00:00"))
    return x


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _existing_article_ids(db, ids: List[str]) -> Set[str]:
    """FK 방어용: DB article_data에 존재하는 article_id만 남기기"""
    if not ids:
        return set()
    rows = db.execute(
        select(article_data_tbl.c.article_id).where(article_data_tbl.c.article_id.in_(ids))
    ).fetchall()
    return {r[0] for r in rows}


# -------------------------
# ES -> DB: article_data/article_label upsert
# -------------------------
def upsert_article(article_list: List[str]):
    to_article_data = []
    to_article_label = []
    db = SessionLocal()

    query = {
        "_source": ["article_id", "article_title", "article_content", "url", "article_label"],
        "size": 1000,
        "query": {"terms": {"article_id": article_list}},
    }

    resp = es.search(index="article_data", body=query)
    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        return

    for h in hits:
        src = h.get("_source", {})
        article_id = (src.get("article_id") or "").strip()
        if not article_id:
            continue

        title = src.get("article_title") or ""
        url = src.get("url") or ""
        article_content = src.get("article_content") or ""
        if isinstance(article_content, list):
            article_content = " ".join(article_content)

        label = src.get("article_label") or {}
        article_category = label.get("category") or ""
        article_trust_score = _safe_float(label.get("article_trust_score"), 0.0)

        to_article_data.append({
            "article_id": article_id,
            "article_title": title,
            "article_url": url,
            "article_length": len(article_content),
        })
        to_article_label.append({
            "article_id": article_id,
            "article_category": article_category,
            "article_trust_score": article_trust_score,
        })

    try:
        # --- article_data bulk upsert ---
        if to_article_data:
            stmt = insert(article_data_tbl).values(to_article_data)
            update_cols = {
                c.name: stmt.inserted[c.name]
                for c in article_data_tbl.columns
                if c.name != "article_id"
            }
            db.execute(stmt.on_duplicate_key_update(**update_cols))

        # --- article_label bulk upsert ---
        if to_article_label:
            stmt2 = insert(article_label_tbl).values(to_article_label)
            update_cols2 = {
                c.name: stmt2.inserted[c.name]
                for c in article_label_tbl.columns
                if c.name != "article_id"
            }
            db.execute(stmt2.on_duplicate_key_update(**update_cols2))

        db.commit()
        return len(to_article_data), len(to_article_label)

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# -------------------------
# topic_docs -> DB: topic_polarity / article_polarity upsert
# -------------------------
def upsert_topic_polarity(topic_docs: List[Dict[str, Any]], *, fmt_prefix: str):
    """
    topic_docs:
      - topic_id: "0" 같은 클러스터 id (문자열/정수 가능)
      - topic_name
      - calculated_at (iso str)
      - (NEW) rank or topic_rank  -> DB 컬럼 topic_rank에 저장
      - positive_articles / negative_articles / neutral_articles: intensity 포함
    """
    to_topic: List[Dict[str, Any]] = []
    to_article: List[Dict[str, Any]] = []

    db = SessionLocal()
    try:
        for t in topic_docs or []:
            raw_tid = str(t.get("topic_id") or "").strip()
            if not raw_tid:
                continue

            topic_id = f"{fmt_prefix}_{raw_tid}"
            calculated_at = _parse_iso_dt(t.get("calculated_at"))

            # ✅ NEW: rank/topic_rank 둘 다 지원
            topic_rank = _safe_int(t.get("topic_rank"), None)
            if topic_rank is None:
                topic_rank = _safe_int(t.get("rank"), 0)

            to_topic.append({
                "topic_id": topic_id,
                "topic_name": t.get("topic_name", "") or "",
                "topic_rank": int(topic_rank),     # ✅ DB: topic_rank 컬럼
                "calculated_at": calculated_at,
            })

            def emit(items, pol: str):
                for a in items or []:
                    aid = str(a.get("article_id") or "").strip()
                    if not aid:
                        continue
                    to_article.append({
                        "article_id": aid,
                        "topic_id": topic_id,   # FK/PK 구성에 맞게 유지
                        "polarity": pol,
                        "intensity": _safe_float(a.get("intensity"), 0.0),
                    })

            emit(t.get("positive_articles"), "positive")
            emit(t.get("negative_articles"), "negative")
            emit(t.get("neutral_articles"), "neutral")

        # 1) topic_polarity upsert
        if to_topic:
            stmt = insert(topic_polarity_tbl).values(to_topic)

            # topic_polarity_tbl의 PK/UK에 따라 다르지만,
            # 보통 topic_id가 PK라고 가정 (es_id는 자동/다른키로 쓰는 경우 제외)
            update_cols = {
                c.name: stmt.inserted[c.name]
                for c in topic_polarity_tbl.columns
                if c.name not in ("es_id",)  # 유지 (너 기존 로직)
            }
            db.execute(stmt.on_duplicate_key_update(**update_cols))

        # 2) article_polarity FK 방어: DB에 존재하는 article_id만 유지
        if to_article:
            ids = [r["article_id"] for r in to_article]
            exists = _existing_article_ids(db, ids)
            to_article = [r for r in to_article if r["article_id"] in exists]

        # 3) article_polarity upsert
        if to_article:
            stmt2 = insert(article_polarity_tbl).values(to_article)

            # PK가 (article_id, topic_id) 라고 가정하고,
            # polarity/intensity만 업데이트 하도록
            update_cols2 = {
                c.name: stmt2.inserted[c.name]
                for c in article_polarity_tbl.columns
                if c.name not in ("article_id", "topic_id")
            }
            db.execute(stmt2.on_duplicate_key_update(**update_cols2))

        db.commit()
        return len(to_topic), len(to_article)

    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# -------------------------
# topic_rows -> ES: article_data.article_label.topic_polarity 단일 유지
# -------------------------
def set_article_topic_polarity_single(
    topic_rows: List[Dict[str, Any]],
    *,
    index_name: str = "article_data",
    chunk_size: int = 500,
):
    """
    기사 1개당 topic_polarity 1개만 유지
    -> article_label.topic_polarity 를 길이 1짜리 배열로 통째로 덮어씀
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
            "intensity": _safe_float(r.get("intensity"), 0.0),
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


# -------------------------
# 로컬 테스트용
# -------------------------
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
        if article_id:
            article_list.append(article_id)

    upsert_article(article_list)

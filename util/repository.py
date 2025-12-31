from util.database import SessionLocal , engine
from util.elastic import es
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.mysql import insert


metadata = MetaData()

article_data_tbl = Table(
    "article_data", metadata, autoload_with=engine
)

article_label_tbl = Table(
    "article_label", metadata, autoload_with=engine
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
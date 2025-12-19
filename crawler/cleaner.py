from util.elastic import es
from util.text_cleaner import clean_article_text

def clean_articles(article_ids: list[str]):
    body = {
        "_source": ["article_id", "article_title", "article_content"],
        "size": len(article_ids),
        "query": {"terms": {"article_id": article_ids}}
    }

    resp = es.search(index="article_raw", body=body)

    article_list = [h["_source"] for h in resp["hits"]["hits"]]

    for article in article_list:
        article_id = article.get("article_id")
        title_raw = article.get("article_title")
        
        if title_raw is None:
            title_raw = ""
            es.delete(index="article_raw", id=article_id)
        elif isinstance(title_raw, list):
            title_raw = " ".join(str(x) for x in title_raw if x)

        content_raw = article.get("article_content")
        if content_raw is None:
            content_raw = ""
            es.delete(index="article_raw", id=article_id)
        elif isinstance(content_raw, list):
            content_raw = " ".join(str(x) for x in content_raw if x)

        title = clean_article_text(str(title_raw).strip())
        content = clean_article_text(str(content_raw).strip())

        cleaned_article = {
            "article_id": article_id,
            "article_title": title,
            "article_content": content
        }
        es.update(index="article_data", id=article_id, body={"doc": cleaned_article})
        
    return None


def delete_null():

    query = {
        "_source": ["article_id"],
        "size":500,
        "query": {
            "range": {
                "collected_at": {
                    "gte": f"now-2h",
                    "lte": "now"
                }
            }
        }
    }

    ids = set()
    resp = es.search(index="article_raw", body=query)

    for h in resp["hits"]["hits"]:
        ids.add(h["_source"]["article_id"])

    resp2 = es.search(index="article_data", body=query)
    for h in resp2["hits"]["hits"]:
        data_id = h["_source"]["article_id"]
        if data_id not in ids:
            es.delete(index="article_data", id=data_id)




if __name__ == "__main__":
    delete_null()


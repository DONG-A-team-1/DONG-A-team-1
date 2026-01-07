import json
from util.elastic import es

def similar_articles(article_id):
    body = {
        "_source": ["article_embedding"],
        "size": 1,
        "query": {"term": {"article_id": article_id}}
    }

    resp = es.search(index="article_data", body=body)
    hits = resp["hits"]["hits"]
    if not hits:
        raise ValueError("No document found for article_id")

    query_vec = hits[0]["_source"]["article_embedding"]
    res = es.search(
        index="article_data",
        size=5,
        knn={
            "field": "article_embedding",
            "query_vector": query_vec,  # list[float], len=768
            "k": 1000,
            "num_candidates": 2000
        },
        source=["article_id", "article_title"]
    )

    related_articles = [
        {
            "article_id": h["_source"].get("article_id"),
            "score": h["_score"]
        }
        for h in res["hits"]["hits"]
    ]
    return related_articles

# if __name__ == "__main__":
    # similar_articles(article_id)



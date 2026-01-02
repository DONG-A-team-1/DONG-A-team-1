def search_articles(search_type: str, query: str, size: int = 20):
    """
    기사 검색
    search_type: all | title | content | keywords
    """

    # 검색 조건 분기
    if search_type == "title":
        es_query = {
            "match": {
                "title": query
            }
        }

    elif search_type == "content":
        es_query = {
            "match": {
                "content": query
            }
        }

    elif search_type == "keywords":
        es_query = {
            "match": {
                "keywords": query
            }
        }

    else:  # all (제목 + 본문)
        es_query = {
            "multi_match": {
                "query": query,
                "fields": ["title", "content"]
            }
        }

    # ES 검색 실행
    response = es.search(
        index=INDEX_NAME,
        size=size,
        query=es_query
    )

    hits = response["hits"]["hits"]

    articles = []
    for hit in hits:
        source = hit["_source"]
        source["id"] = hit["_id"]
        articles.append(source)

    return {
        "success": True,
        "total": response["hits"]["total"]["value"],
        "articles": articles
    }
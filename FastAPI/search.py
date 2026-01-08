from elasticsearch import Elasticsearch

es = Elasticsearch([{"host": "localhost", "port": 9200}])


def search_articles(search_type, query, size):
    """
    search_type: "all", "title", "content", "keywords"
    query: 검색어
    size: 결과 개수
    """

    # 검색 타입에 따라 쿼리 구성
    if search_type == "all":
        # 제목 + 본문 검색
        es_query = {
            "bool": {
                "should": [
                    {"match": {"title": query}},
                    {"match": {"content": query}}
                ]
            }
        }
    elif search_type == "title":
        # 제목만 검색
        es_query = {"match": {"title": query}}

    elif search_type == "content":
        # 본문만 검색
        es_query = {"match": {"content": query}}

    elif search_type == "keywords":
        # 키워드 검색
        es_query = {"match": {"keywords": query}}

    else:
        # 기본값: 제목+본문
        es_query = {
            "bool": {
                "should": [
                    {"match": {"title": query}},
                    {"match": {"content": query}}
                ]
            }
        }

    # Elasticsearch 쿼리 실행
    response = es.search(
        index="articles",  # 인덱스 이름
        body={
            "query": es_query,
            "size": size
        }
    )

    # 결과 포맷팅
    articles = []
    for hit in response['hits']['hits']:
        source = hit['_source']
        articles.append({
            "article_id": hit['_id'],
            "title": source.get('title', ''),
            "content": source.get('content', ''),
            "category": source.get('category', ''),
            "image": source.get('image', ''),
            "source": source.get('source', ''),
            "published_date": source.get('published_date', ''),
            "trustScore": source.get('trustScore', 0)
        })

    return {
        "success": True,
        "total": response['hits']['total']['value'],
        "articles": articles
    }
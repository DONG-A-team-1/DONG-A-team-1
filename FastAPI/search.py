from util.elastic import  es


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

def es_search_articles(search_type, query, size):
    # (쿼리 구성 로직은 동일하게 유지하되, 필드명만 매핑에 맞게 수정)
    if not query:
        es_query = {"match_all": {}}
    elif search_type == "all":
        es_query = {
            "bool": {
                "should": [
                    {"match": {"article_title": query}},
                    {"match": {"article_content": query}}
                ]
            }
        }
    elif search_type == "title":
        es_query = {"match": {"article_title": query}}
    elif search_type == "content":
        es_query = {"match": {"article_content": query}}
    else:
        es_query = {"match": {"keywords": query}}

    # Elasticsearch 쿼리 실행
    response = es.search(
        index="article_data",  # 인덱스명 매핑에 맞게 변경
        body={
            "query": es_query,
            "size": size,
            "sort": [{"upload_date": {"order": "desc"}}] # 최신순 정렬 권장
        }
    )

    articles = []
    for hit in response['hits']['hits']:
        source = hit['_source']
        label = source.get('article_label', {}) # 중첩 필드 안전하게 가져오기

        # 가공 로직: 신뢰도와 트렌드 점수는 scaled_float이므로 가독성 있게 정수화(반올림)
        raw_trust = label.get('article_trust_score', 0)
        raw_trend = label.get('trend_score', 0)

        articles.append({
            "article_id": hit['_id'],             # ES 문서 ID
            "title": source.get('article_title', ''),
            "content": source.get('article_content', ''),
            "image": source.get('article_img', ''),
            "reporter": source.get('reporter', '기자 미상'), # 기자명
            "source": source.get('press', '언론사 미상'),    # 언론사명
            "published_date": source.get('upload_date', ''), # yyyyMMdd
            "category": label.get('category', '일반'),      # 카테고리
            "trendScore": round(raw_trend * 100) if raw_trend <= 1 else round(raw_trend),
            "trustScore": round(raw_trust * 100) if raw_trust <= 1 else round(raw_trust)
        })

    return {
        "success": True,
        "total": response['hits']['total']['value'],
        "articles": articles
    }
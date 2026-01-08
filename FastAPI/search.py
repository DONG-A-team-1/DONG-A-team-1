from util.elastic import es  # util 폴더의 elastic.py
from util.text_cleaner import yyyymmdd_to_iso

# FastAPI/search.py (또는 해당 파일)

def search_articles(search_type: str, query: str, size: int = 20):
    """기사 검색"""

    # 검색 타입별 쿼리 생성
    if search_type == "all" or search_type == "title_body":
        # 제목 + 본문 검색
        es_query = {
            "bool": {
                "should": [
                    {"match": {"article_title": query}},
                    {"match": {"article_content": query}}
                ]
            }
        }

    elif search_type == "title":
        # 제목만 검색
        es_query = {"match": {"article_title": query}}

    elif search_type == "content" or search_type == "body":
        # 본문만 검색
        es_query = {"match": {"article_content": query}}

    elif search_type == "keywords" or search_type == "keyword":
        # 키워드 검색
        es_query = {"match": {"keywords.raw": query}} # keywords=메인필드 , 정확한 일치만 검색
        # 서브필드 kewords.raw=타입 text 분석기 nori로 유연한 검색 가능함.

    else:
        # 기본: 제목 + 본문
        es_query = {
            "bool": {
                "should": [
                    {"match": {"article_title": query}},
                    {"match": {"article_content": query}}
                ]
            }
        }

    # ES 검색 실행
    body = {
        "_source": [
            "article_id",
            "press",
            "upload_date",
            "article_title",
            "article_content",
            "article_img",
            "url",
            "article_label",
            "keywords"
        ],
        "size": size,
        "query": es_query,
        "sort": [
            {"upload_date": {"order": "desc"}}
        ]
    }

    resp = es.search(index="article_data", body=body)
    hits = resp.get("hits", {}).get("hits", [])

    # 결과 포맷팅
    articles = []
    for hit in hits:
        src = hit.get("_source", {})
        label = src.get("article_label") or {}

        # trustScore 안전하게 처리
        raw_score = label.get("article_trust_score")

        if raw_score is None:
            trust_score = 0
        else:
            raw_score = float(raw_score)

            if raw_score <= 1:
                # 0~1 확률형
                trust_score = round(raw_score * 100)
            elif raw_score <= 100:
                # 이미 퍼센트
                trust_score = round(raw_score)
            else:
                # 0~4095 같은 raw 모델 점수
                trust_score = round((raw_score / 4095) * 100)

        articles.append({
            "article_id": src.get("article_id"),
            "title": src.get("article_title", ""),
            "content": src.get("article_content", ""),
            "image": src.get("article_img"),
            "category": label.get("category"),
            "source": src.get("press"),
            "upload_date": yyyymmdd_to_iso(src.get("upload_date")),
            "trustScore": trust_score,
            "keywords": src.get("keywords", [])
        })

    return {
        "success": True,
        "query": query,
        "search_type": search_type,
        "total": resp['hits']['total']['value'],
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
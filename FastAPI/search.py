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

        # trustScore 안전하게 처리`
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

# 임베딩 기반 연관 기사
def es_search_related_by_embedding(article_id: str, size: int = 4):
    """
    기사 임베딩 cosine similarity 기반 연관 기사
    - 너무 유사한 기사 제거
    - 동일 언론사 중복 제거
    """

    # -------------------------------
    # 1. 기준 기사 임베딩 조회
    # -------------------------------
    base = es.search(
        index="article_data",
        body={
            "size": 1,
            "_source": ["article_embedding"],
            "query": {"term": {"article_id": article_id}}
        }
    )

    hits = base["hits"]["hits"]
    if not hits:
        return []

    query_vector = hits[0]["_source"]["article_embedding"]

    # -------------------------------
    # 2. KNN 검색 (후보는 넉넉히)
    # -------------------------------
    res = es.search(
        index="article_data",
        size=50,  # ⭐ 후보를 넉넉히 가져온다
        knn={
            "field": "article_embedding",
            "query_vector": query_vector,
            "k": 50,
            "num_candidates": 200,
            "filter": [
                {"range": {"collected_at": {"gte": "now-7d"}}}
            ]
        },
        _source=[
            "article_id",
            "article_title",
            "article_img",
            "press",
            "article_label"
        ]
    )

    # -------------------------------
    # 3. 필터 설정값
    # -------------------------------
    SIMILARITY_MAX = 0.90   # 너무 비슷하면 제거
    SIMILARITY_MIN = 0.75   # 너무 안 비슷하면 제거

    articles = []
    used_press = set()

    # -------------------------------
    # 4. 후보 필터링
    # -------------------------------
    for h in res["hits"]["hits"]:
        src = h["_source"]

        # 자기 자신 제외
        if src["article_id"] == article_id:
            continue

        similarity = h["_score"]  # cosine similarity

        # ✅ 유사도 컷
        if similarity > SIMILARITY_MAX:
            continue
        if similarity < SIMILARITY_MIN:
            continue

        press = src.get("press")

        # ✅ 동일 언론사 중복 방지
        if press in used_press:
            continue

        label = src.get("article_label", {})

        articles.append({
            "article_id": src["article_id"],
            "title": src["article_title"],
            "image": src.get("article_img"),
            "source": press,
            "trustScore": int(label.get("article_trust_score", 0) * 100)
        })

        used_press.add(press)
        if len(articles) >= size:
            break
    return articles

# 메인 트렌딩 기사
def es_search_trending_articles(size=5):
    """
    최근 3일 기사 중 트렌드 점수 기준 상위 기사
    """

    res = es.search(
        index="article_data",
        body={
            "size": size,
            "query": {
                "range": {
                    "collected_at": {"gte": "now-3d"}
                }
            },
            "sort": [
                {"article_label.trend_score": {"order": "desc"}},
                {"collected_at": {"order": "desc"}}
            ],
            "_source": [
                "article_id",
                "article_title",
                "article_img",
                "press",
                "article_label"
            ]
        }
    )

    articles = []
    for h in res["hits"]["hits"]:
        src = h["_source"]
        label = src.get("article_label", {})

        articles.append({
            "article_id": src["article_id"],
            "title": src["article_title"],
            "image": src.get("article_img"),
            "source": src.get("press"),
            "category": label.get("category"),
            "trustScore": int(label.get("article_trust_score", 0) * 100)
        })

    return {"success": True, "articles": articles}

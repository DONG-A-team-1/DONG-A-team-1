# FastAPI/article.py

from util.elastic import es
from fastapi import HTTPException
from util.logger import Logger
from util.text_cleaner import yyyymmdd_to_iso
from labeler.find_related import similar_articles

logger = Logger().get_logger(__name__)


def ensure_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


def get_article_from_es(article_id, SOURCE_FIELDS):
    body = {
        "_source": SOURCE_FIELDS,
        "size": 10,
        "query": {"terms": {"article_id": ensure_list(article_id)}}
    }

    resp = es.search(index="article_data", body=body)
    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        raise HTTPException(status_code=404, detail="article not found")

    result = []
    for h in hits:
        src = h.get("_source", {})
        result.append(src)
    return result


def get_article(article_id: str):
    """개별 기사 조회"""
    source_fields = [
        "article_id",
        "press",
        "reporter",
        "upload_date",
        "article_title",
        "article_content",
        "article_img",
        "url",
        "collected_at",
        "article_label"
    ]
    data = None
    source = get_article_from_es(article_id, SOURCE_FIELDS=source_fields)
    for src in source:
        data = {
            "article_id": article_id,
            "press": src.get("press"),
            "reporter": src.get("reporter"),
            "upload_date": yyyymmdd_to_iso(src.get("upload_date")),
            "article_title": src.get("article_title") or "",
            "article_content": src.get("article_content") or "",
            "article_img": src.get("article_img"),
            "url": src.get("url")
        }
    article = data
    return article


def get_related(article_id: str):
    """연관 기사 조회"""
    related = similar_articles(article_id)
    id_list = [d["article_id"] for d in related[1:4]]

    source_fields = [
        "article_id",
        "upload_date",
        "article_title",
        "article_img",
        "article_label",
    ]

    docs = get_article_from_es(id_list, SOURCE_FIELDS=source_fields)

    if isinstance(docs, HTTPException):
        raise docs

    score_map = {d["article_id"]: d.get("score") for d in related[:3]}

    result = []
    for src in docs:
        label = src.get("article_label") or {}

        result.append({
            "article_id": src.get("article_id"),
            "article_title": src.get("article_title") or "",
            "article_img": src.get("article_img"),
            "upload_date": yyyymmdd_to_iso(src.get("upload_date")),
            "category": label.get("category")
        })
    logger.info(result)
    return result


# ========== 여기에 새로 추가! ==========
def get_articles_by_category(category_name: str, size: int = 45, page: int = 1, sort_type: str = "latest"):
    category_map = {"사회": "사회/경제/산업", "정치": "정치", "국제": "국제", "지역": "지역", "문화": "문화", "스포츠": "스포츠"}
    es_category = category_map.get(category_name, category_name)

    # 1. 상단 카드용: 트렌드 점수 높은 순 고정 (무조건 4개)
    trend_body = {
        "size": 4,
        "query": {"term": {"article_label.category": es_category}},
        "sort": [{"article_label.trend_score": {"order": "desc"}}]
    }
    trend_resp = es.search(index="article_data", body=trend_body)

    # 2. 하단 리스트용: 사용자 선택 정렬 (size만큼)
    if sort_type == "reliability":
        sort_criteria = [{"article_label.article_trust_score": {"order": "desc"}}]
    elif sort_type == "oldest":
        sort_criteria = [{"upload_date": {"order": "asc"}}]
    else:
        sort_criteria = [{"upload_date": {"order": "desc"}}]

    list_body = {
        "size": size,
        "from": (page - 1) * size,
        "query": {"term": {"article_label.category": es_category}},
        "sort": sort_criteria
    }
    list_resp = es.search(index="article_data", body=list_body)

    # 데이터 포맷팅 함수 (중복 제거용)
    def format_hits(hits):
        DEFAULT_IMG = "/static/newspalette.png"
        result = []
        for hit in hits:
            src = hit.get("_source", {})
            label = src.get("article_label") or {}
            trust_val = label.get("article_trust_score", 0)
            result.append({
                "article_id": src.get("article_id"),
                "title": src.get("article_title", ""),
                "content": src.get("article_content", ""),
                "image": src.get("article_img") or DEFAULT_IMG,
                "source": src.get("press"),
                "upload_date": yyyymmdd_to_iso(src.get("upload_date")),
                "trustScore": int(float(trust_val)) if trust_val else 0,
                "category": label.get("category")
            })
        return result

    return {
        "success": True,
        "trend_articles": format_hits(trend_resp['hits']['hits']),  # 트렌드 4개
        "list_articles": format_hits(list_resp['hits']['hits']),  # 정렬된 리스트
        "total": list_resp['hits']['total']['value']
    }
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
def get_articles_by_category(category_name: str, size: int = 20, page: int = 1):
    """카테고리별 기사 조회"""

    # 카테고리 매핑
    category_map = {
        "사회": "사회/경제/산업",
        "정치": "정치",
        "국제": "국제",
        "지역": "지역",
        "문화": "문화",
        "스포츠": "스포츠"
    }

    es_category = category_map.get(category_name, category_name)

    # Elasticsearch 쿼리
    body = {
        "_source": [
            "article_id",
            "press",
            "upload_date",
            "article_title",
            "article_content",
            "article_img",
            "url",
            "article_label"
        ],
        "size": size,
        "query": {
            "term": {
                "article_label.category": es_category
            }
        },
        "sort": [
            {"upload_date": {"order": "desc"}}
        ]
    }

    # ES 검색 실행
    resp = es.search(index="article_data", body=body)
    hits = resp.get("hits", {}).get("hits", [])

    # 결과 포맷팅
    articles = []
    for hit in hits:
        src = hit.get("_source", {})
        label = src.get("article_label") or {}

        # trustScore 안전하게 처리
        trust_score = label.get("article_trust_score", 0)
        if trust_score:
            trust_score = int(float(trust_score) * 100)  # 0.9 → 90
        else:
            trust_score = 0

        articles.append({
            "article_id": src.get("article_id"),
            "title": src.get("article_title", ""),
            "content": src.get("article_content", ""),
            "image": src.get("article_img"),
            "category": label.get("category"),
            "source": src.get("press"),
            "upload_date": yyyymmdd_to_iso(src.get("upload_date")),
            "trustScore": trust_score
        })

    logger.info(f"카테고리 '{category_name}' 기사 {len(articles)}개 조회")

    return {
        "success": True,
        "category": category_name,
        "total": resp['hits']['total']['value'],
        "articles": articles
    }
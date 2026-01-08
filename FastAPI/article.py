from util.elastic import es
from fastapi import HTTPException
from util.logger import Logger
from util.text_cleaner import  yyyymmdd_to_iso
from labeler.find_related import similar_articles

logger = Logger().get_logger(__name__)

def ensure_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


def get_article_from_es(article_id, SOURCE_FIELDS, max=10):
    logger.info(f"get article from es: {ensure_list(article_id)}")
    body = {
        "_source": SOURCE_FIELDS,
        "size": max,
        "query": {"terms": {"article_id": ensure_list(article_id)}}
    }

    resp = es.search(index="article_data", body=body)
    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        raise HTTPException(status_code=404, detail="article not found")

    result =[]
    for h in hits:
        src = h.get("_source", {})
        result.append(src)
    return result

def get_article(article_id: str):
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
    data=None
    source = get_article_from_es(article_id,SOURCE_FIELDS=source_fields)
    for src in source:
        data = {
            # === top-level ===
            "article_id": article_id,
            "press": src.get("press"),
            "reporter": src.get("reporter"),

            "upload_date": yyyymmdd_to_iso(src.get("upload_date")),

            "article_title": src.get("article_title") or "",
            "article_content": src.get("article_content") or "",

            "article_img": src.get("article_img"),
            "url": src.get("url")
        }
    article=data
    # 너희 함수로
    # logger.info(article)
    return article

def get_related(article_id: str):
    related = similar_articles(article_id)  # [{'article_id','score'}, ...]

    # top3 id만
    id_list = [d["article_id"] for d in related[1:4]]

    # ✅ dot notation 말고 article_label 통째로 받기
    source_fields = [
        "article_id",
        "upload_date",
        "article_title",
        "article_img",
        "article_label",
    ]

    docs = get_article_from_es(id_list, SOURCE_FIELDS=source_fields)

    # get_article_from_es가 HTTPException을 "return"하는 구조면 방어
    if isinstance(docs, HTTPException):
        raise docs

    # score 매핑(선택)
    score_map = {d["article_id"]: d.get("score") for d in related[:3]}

    # ✅ 여기서만 정리(날짜 + category 평탄화)
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
from util.elastic import es
from fastapi import HTTPException
from util.logger import Logger
from .article import get_article_from_es
from util.text_cleaner import yyyymmdd_to_iso
import numpy as np


logger = Logger().get_logger(__name__)

def get_topic_from_es():
    body = {
        "size": 10,
        "sort": [
        {"calculated_at": {"order": "desc"}},
        {"rank": {"order": "asc"}}
        ]
    }
    try:
        resp = es.search(index="topic_polarity", body=body)
    except Exception as e:
        logger.exception("ES search failed")
        raise HTTPException(status_code=500, detail=str(e))

    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        raise HTTPException(status_code=404, detail="topics not found")

    result =[]
    for h in hits:
        src = h.get("_source", {})
        result.append(src)
    logger.info(result)
    return result

def get_topic_article(id_list):
    vec_list = []
    for id_ in id_list:
        polar_vec = []
        source_fields = [
            "article_embedding"
        ]
        docs = get_article_from_es(id_[1], SOURCE_FIELDS=source_fields,max=20)
        for doc in docs:
            # logger.info(doc)
            vec=doc["article_embedding"]

            polar_vec.append(vec)
        mean_vec = np.mean(polar_vec, axis=0).tolist()
        vec_list.append(mean_vec)
    # logger.info(len(vec_list))

    all_result = []
    for id_,vec in zip(id_list,vec_list):

        query = {
            "size": 10,
            "_source": [
                "article_id",
                "upload_date",
                "article_title",
                "article_img",
                "press"
            ],
            "query": {
                "script_score": {
                    "query": {
                        "terms": {
                            "article_id": id_[1]
                        }
                    },
                    "script": {
                        "source": "cosineSimilarity(params.qv, 'article_embedding') + 1.0",
                        "params": {
                            "qv": vec
                        }
                    }
                }
            }
        }

        resp = es.search(index="article_data", body=query)
        hits = resp.get("hits", {}).get("hits", [])

        if isinstance(hits , HTTPException):
            raise hits

        result = []
        for h in hits :
            result.append({
                "article_id": h["_source"].get("article_id"),
                "article_title": h["_source"].get("article_title") or "",
                "article_img": h["_source"].get("article_img"),
                "upload_date": yyyymmdd_to_iso(h["_source"].get("upload_date")),
                "press":  h["_source"].get("press")
            })
        all_result.append(result)
    return all_result

def get_opposite_topic(article_id):

    # 1) 현재 기사에서 topic_id / stance 조회
    body = {
        "_source": ["article_label"],
        "size": 1,
        "query": {"term": {"article_id": article_id}},
    }

    resp = es.search(index="article_data", body=body)
    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        return None

    label = hits[0].get("_source", {}).get("article_label", {})
    topic = label.get("topic_polarity") or []
    if not topic:
        logger.info("성향 분류된 기사가 아닙니다")
        return None

    topic_id = str(topic[0].get("topic_id"))
    stance = topic[0].get("stance")

    # 2) 반대 관점 필드 선택
    if stance == "긍정":
        pick_field = "negative_articles"
    elif stance == "부정":
        pick_field = "positive_articles"
    else:
        pick_field = "neutral_articles"

    # 3) topic_polarity 문서 조회 (_id 기반)
    resp = es.get(index="topic_polarity", id=topic_id)
    src = resp.get("_source", {})

    topic_name = src.get("topic_name")
    picked = (src.get(pick_field) or [])[:3]   # 최대 3개

    # 4) article_id 리스트 추출
    article_ids = [a.get("article_id") for a in picked if a.get("article_id")]
    if not article_ids:
        return {
            "polar": {
                "topic_name": topic_name,
                "articles": []
            }
        }

    # 5) 기사 상세 조회 (기존 함수 사용)
    SOURCE_FIELDS = ["article_id", "article_title", "press", "article_img"]
    docs = get_article_from_es(
        article_ids,
        SOURCE_FIELDS=SOURCE_FIELDS,
        max=len(article_ids)
    )

    # 6) terms 결과는 순서 보장 안 되므로 id 기준 매핑
    by_id = {d.get("article_id"): d for d in docs}

    articles = []
    for aid in article_ids:   # 원래 picked 순서 유지
        d = by_id.get(aid, {})
        articles.append({
            "article_id": aid,
            "article_title": d.get("article_title") or "",
            "press": d.get("press") or "",
            "article_img": d.get("article_img") or ""
        })

    # 7) 프론트로 보낼 최종 결과
    return {
        "polar": {
            "topic_name": topic_name,
            "articles": articles
        }
    }


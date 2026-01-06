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
        "sort":{"calculated_at":"desc","rank":"asc"},
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


from util.elastic import es
from util.logger import Logger
import json

logger = Logger().get_logger(__name__)

# def get_admin_data():
#     result = []
#     src = es.search(index="error_log", body={"sort": {"@timestamp": {"order": "desc"}},"size":100})
#
#     for doc in src["hits"]["hits"]:
#         # logger.info(json.dumps(doc,ensure_ascii=False,indent=4))
#         result.append(doc["_source"])
#
#     logger.info(len(result))


def get_admin_articles():
    result =[]
    body = {
        "_source": ["article_id","article_title","article_content","press","reporter","published_at"],
        "size": 100,
        "sort":{
            "collected_at":"desc"
        }
    }

    src = es.search(index="article_data", body=body)
    for article in src["hits"]["hits"]:
        result.append(article["_source"])
    logger.info(json.dumps(result, indent=4, ensure_ascii=False))
    return result

if __name__ == "__main__":
    get_admin_articles()
from util.elastic import es
from util.logger import Logger
import json

logger = Logger().get_logger(__name__)

def get_admin_data():
    result = []
    src = es.search(index="error_log", body={"sort": {"@timestamp": {"order": "desc"}},"size":100})

    for doc in src["hits"]["hits"]:
        logger.info(json.dumps(doc,ensure_ascii=False,indent=4))
        result.append(doc["_source"])
    pass

if __name__ == "__main__":
    get_admin_data()
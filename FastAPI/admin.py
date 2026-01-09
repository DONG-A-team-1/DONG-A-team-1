from util.elastic import es
from util.logger import Logger
import json
from datetime import datetime, timezone, timedelta

logger = Logger().get_logger(__name__)

def format_timestamp(iso_ts: str) -> str:
    dt = datetime.fromisoformat(iso_ts)
    kst = dt.astimezone(timezone(timedelta(hours=9)))
    return kst.strftime("%Y-%m-%d %H:%M:%S")

def normalize_log_type(raw_type: str | None) -> str:
    if not raw_type:
        return "info"
    t = str(raw_type).lower()
    mapping = {
        # error 계열
        "error": "error",
        "err": "error",
        "exception": "error",
        "fatal": "error",
        "critical": "error",

        # warning 계열
        "warning": "warning",
        "warn": "warning",

        # info 계열
        "info": "info",
        "information": "info",
        "debug": "info",
        "trace": "info",

        # success 계열
        "success": "success",
        "ok": "success",
        "passed": "success",
        "complete": "success",
        "completed": "success",
    }
    return mapping.get(t, "info")

def get_admin_data():
    result = []
    src = es.search(index="info_logs", body={"sort": {"@timestamp": {"order": "desc"}},"size":1000})

    result.extend([
        {
            "id": doc["_source"].get("trace", {}).get("run_id"),
            "time": format_timestamp(doc["_source"].get("@timestamp","")),
            "type": normalize_log_type(doc["_source"].get("pipeline",{}).get("status","")),
            "source": doc["_source"].get("trace",{}).get("job_id",""),
            "message": doc["_source"].get("pipeline",{}).get("message",""),
            "stack":""
        }
        for doc in src["hits"]["hits"]
    ])
    src = es.search(index="error_log", body={"sort": {"@timestamp": {"order": "desc"}}, "size": 1000})

    result.extend([
        {
            "id": doc["_source"].get("trace", {}).get("run_id"),
            "time": format_timestamp(doc["_source"].get("@timestamp","")),
            "type": "error",
            "source": doc["_source"].get("log", {}).get("logger",""),
            "message": doc["_source"].get("message",""),
            "stack":doc["_source"].get("samples",[{"스텍 트레이스 없음"}])[0],
        }
        for doc in src["hits"]["hits"]
    ])
    return result

def get_admin_articles():
    result =[]
    body = {
        "_source": ["article_id","article_title","reporter","collected_at","article_label"],
        "size": 500,
        "sort":{
            "collected_at":"desc"
        }
    }

    src = es.search(index="article_data", body=body)
    result.extend([
        {
            "id": doc["_source"].get("article_id"),
            "title":doc["_source"].get("article_title"),
            "category":doc["_source"].get("article_label",{}).get("category"),
            "labels":[doc["_source"].get("article_label",{}).get("article_trust_score")],
            "date":format_timestamp(doc["_source"].get("collected_at")),
        }
        for doc in src["hits"]["hits"]
    ])

    return result
if __name__ == "__main__":
    get_admin_articles()
import logging
import os
import inspect
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

class Logger:

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    def get_logger(self, name):
        return logging.getLogger(name)



KST = timezone(timedelta(hours=9))

def build_error_doc(
    message: str,
    metrics: Optional[Dict[str, Any]] = None,
    samples: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    error_log data stream용 표준 에러 로그 문서 생성기
    - logger: 자동으로 file:function
    - message: 요약 메시지
    - metrics: 집계용 숫자 필드
    - samples: 에러 샘플(최대 10개)
    """

    frame = inspect.currentframe().f_back
    filename = os.path.basename(frame.f_code.co_filename)
    funcname = frame.f_code.co_name

    doc: Dict[str, Any] = {
        "@timestamp": datetime.now(KST).isoformat(),
        "log": {
            "level": "ERROR",
            "logger": f"{filename}:{funcname}"
        },
        "message": message
    }
    if metrics:
        doc["metrics"] = metrics

    if samples:
        doc["samples"] = samples[:10]

    return doc

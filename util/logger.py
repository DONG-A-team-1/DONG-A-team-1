import logging
import os
import inspect
from datetime import datetime, timezone, timedelta

class Logger:

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    def get_logger(self, name):
        return logging.getLogger(name)



KST = timezone(timedelta(hours=9))

def build_error_doc(message: str, level: str = "ERROR"):

    frame = inspect.currentframe().f_back  # 🔑 호출자 프레임
    filename = os.path.basename(frame.f_code.co_filename)
    funcname = frame.f_code.co_name

    return {
        "@timestamp": datetime.now(KST).isoformat(),
        "log": {
            "level": level,
            "logger": f"{filename}:{funcname}"
        },
        "message": message
    }

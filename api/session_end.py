from fastapi import APIRouter
from pydantic import BaseModel
from datetime import datetime, timezone

from util.elastic import es

router = APIRouter(prefix="/session", tags=["session"])


# 프론트에서 받을 END 요청
class SessionEndRequest(BaseModel):
    session_id: str   # 서버가 발급한 세션 ID


@router.post("/end")
def session_end(req: SessionEndRequest):
    """
    기사 페이지를 떠날 때 호출되는 API

    처리 내용:
    1) 세션 문서 조회
    2) 체류 시간 계산
    3) 즉시 이탈 여부 판단
    4) ES 문서 종료 처리
    """

    now = datetime.now(timezone.utc)

    # 1. 세션 조회
    res = es.get(index="session_data", id=req.session_id)
    src = res["_source"]

    started_at = src["started_at"]
    scroll_depth = src.get("scroll_depth", 0.0)

    # 2. 체류 시간 (초)
    dwell_time = (now - started_at).total_seconds()

    # 3. 즉시 이탈 여부 판단
    is_bounce = dwell_time < 5 and scroll_depth < 0.2

    # 4. ES 업데이트 (종료 확정)
    es.update(
        index="session_data",
        id=req.session_id,
        doc={
            "last_ping_at": now,
            "is_end": True
        }
    )

    return {
        "status": "ended",
        "session_id": req.session_id,
        "dwell_time": dwell_time,
        "scroll_depth": scroll_depth,
        "is_bounce": is_bounce
    }

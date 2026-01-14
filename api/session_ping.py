from fastapi import APIRouter
from pydantic import BaseModel
from datetime import datetime, timezone
from util.elastic import es
from zoneinfo import ZoneInfo

router = APIRouter(prefix="/session", tags=["session"])

# 프론트에서 보내는 ping 요청 형태
class SessionPingRequest(BaseModel):
    session_id: str        # 서버가 발급한 세션 ID
    scroll_depth: float    # 0 ~ 1 사이 스크롤 깊이


# POST /session/ping
@router.post("/ping")
def session_ping(req: SessionPingRequest):
    now = datetime.now(ZoneInfo("Asia/Seoul"))

    # 1. 기존 세션 정보 조회
    res = es.get(
        index="session_data",
        id=req.session_id
    )
    source = res["_source"]

    # 2. seq 증가
    next_seq = source["seq"] + 1

    # 3. ES 문서 업데이트
    prev_depth = source.get("scroll_depth", 0.0)
    new_depth = max(prev_depth, req.scroll_depth)

    es.update(
        index="session_data",
        id=req.session_id,
        retry_on_conflict=3,
        body={
            "doc": {
                "seq": next_seq,
                "last_ping_at": now,
                "scroll_depth": new_depth
            }
        }
    )
    return {"status": "ok"}

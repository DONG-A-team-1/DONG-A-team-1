from fastapi import APIRouter, Query
from datetime import datetime, timezone
from util.elastic import es
# from api.user_embedding import update_session_embedding
from zoneinfo import ZoneInfo

router = APIRouter(prefix="/session", tags=["session"])

@router.post("/end")
def session_end(session_id: str = Query(...)):
    """
    기사 페이지 종료 시 호출
    - RDB 저장 ❌
    - ES에 종료 신호만 기록 ⭕
    """
    now = datetime.now(ZoneInfo("Asia/Seoul"))

    # 세션 존재 여부 확인
    try:
        es.get(index="session_data", id=session_id)
    except Exception:
        return {
            "status": "not_found",
            "session_id": session_id
        }

    # 종료 신호 기록 (is_end는 batch만 만진다)
    es.update(
        index="session_data",
        id=session_id,
        retry_on_conflict=3,
        doc={
            "last_ping_at": now,
            "ended_signal": True
        }
    )

    return {
        "status": "ended_signal_received",
        "session_id": session_id
    }

#  - 기사 페이지에 유저가 들어오는 순간
#  - 서버가 session_id를 발급하고 ES(session_data)에 세션 시작 문서 저장

from fastapi import APIRouter              # FastAPI 호출용
from pydantic import BaseModel             # 요청(JSON) 자동 검증용도
from datetime import datetime, timezone    # 시간 기록용 (UTC 기준)
import uuid                                # session_id 생성용 (고유값)
from util.elastic import es                # ES객체
from zoneinfo import ZoneInfo


# 프론트에서 받을 요청 데이터 구조
class SessionStartRequest(BaseModel):
    """
    프론트엔드에서 /session/start 호출 했을 때 보내져야할 고정적인 형식
    예시:
    {
        "user_id": "jimin",
        "article_id": "01100401.20260102170115001"
    }
    user_id가 없으면 에러 / 타입 다르면 에러 → FastAPI 자동검증 설정
    """
    user_id: str
    article_id: str

# Router 설정
# prefix="/session" 이므로 실제 URL은 /session/start 가 된다
# prefix는 API 주소 앞에 자동으로 붙는 공통 주소(앞부분)
router = APIRouter(
    prefix="/session",
    tags=["session"]   # Swagger(API 문서)에서 API의 session폴더에 담아서 분류하는 용도 / API를 자동으로 보여주고 직접 눌러 테스트 할 수 있는 설명서
)

# 세션 시작 API
@router.post("/start")
def session_start(req: SessionStartRequest):
    """
    "기사 페이지에 처음 들어왔을 때" 호출
    처리 순서 예상
    1) 서버가 session_id 생성
    2) 현재 시간 기록
    3) Elasticsearch(session_data)에 세션 문서 생성
    4) session_id를 프론트에 반환
    """

    # (1) session_id 생성
    # uuid는 전 세계적으로 중복될 확률이 거의 없는 고유값
    # session_id는 반드시 서버에서 생성 필수
    session_id = str(uuid.uuid4())
    # (2) 현재 시간 (UTC 기준)
    # ES와 서버 시간 처리의 안정성을 위해 UTC 사용
    now = datetime.now(ZoneInfo("Asia/Seoul"))

    # (3) ES에 저장할 문서
    doc = {
        "session_id": session_id,     # 세션 고유 ID
        "user_id": req.user_id,       # 사용자 ID
        "article_id": req.article_id, # 기사 ID

        # ping 관련 필드
        "seq": 1,                     # 첫 번째 ping (자동 증가 예정)
        "started_at": now,            # 세션 시작 시각
        "last_ping_at": now,          # 마지막 ping 시각 (초기엔 시작 시각)
        "scroll_depth": 0.0,          # 처음엔 스크롤 안 함
        "is_end": False               # 아직 종료되지 않은 세션
    }

    # (4) ES 저장
    # id=session_id 로 지정하는 이유:
    #  - 같은 session_id로 들어오면 "update"
    #  - 새로운 session_id면 "insert"
    es.index(
        index="session_data",
        id=session_id,      # ES 문서의 _id = session_id
        document=doc
    )
    # (5) 프론트로 session_id 반환
    # 프론트는 session_id를 저장 -> 이후 ping / end API에서 계속 사용
    return {
        "session_id": session_id
    }

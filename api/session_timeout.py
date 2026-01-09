"""
세션 타임아웃 처리 + RDB 저장 배치

종료 확정 조건
1) is_end = False 이면서
2) (ended_signal = True OR last_ping_at < now - TIMEOUT)

종료 확정 시 RDB 저장 후 ES is_end = True 처리
"""

from datetime import datetime, timezone, timedelta

from api.user_embedding import update_user_embedding
from util.elastic import es
from util.database import SessionLocal
from sqlalchemy.orm import Session
from util.logger import Logger
from sqlalchemy import text

logger = Logger().get_logger(__name__)
# 설정값
# ping이 이 시간(초) 이상 없으면 세션 종료로 판단
TIMEOUT_SECONDS = 120

# 메인 배치 함수
def close_timeout_sessions():
    logger.info("-1분 간격 호출 대상 세션 정리-")
    """
    - APScheduler가 1분마다 호출 종료 대상 세션을 정리하고 DB에 반영
    """

    # (1) 현재 시각 (UTC 기준)
    now = datetime.now(timezone.utc)

    # (2) 타임아웃 기준 시각
    # now가 17:30이면 → 17:29 이전 ping은 종료 대상
    timeout_time = now - timedelta(seconds=TIMEOUT_SECONDS)

    # (3) 종료 대상 세션 조회
    query = {
        "size": 1000,
        "query": {
            "bool": {
                "must": [
                    # 아직 종료되지 않은 세션
                    {"term": {"is_end": False}}
                    ],
                "should": [
                    {"term": {"ended_signal": True}},
                    {
                        "range": {
                            "last_ping_at": {
                                "lte": timeout_time.isoformat()
                            }
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        }
    }

    # (4) ES 조회
    res = es.search(index="session_data", body=query)
    sessions = res["hits"]["hits"]

    # 종료 대상 없으면 바로 종료
    if not sessions:
        return

    # (5) DB 세션 열기
    db: Session = SessionLocal()
    updated_users = set()
    # 세션 하나씩 처리
    try:
        for s in sessions:
            src = s["_source"]

            session_id = src["session_id"]
            user_id = src["user_id"]
            article_id = src["article_id"]
            updated_users.add(user_id)
            # (6) 체류시간 계산 (초 단위)
            started_dt = datetime.fromisoformat(src["started_at"])
            ended_dt = datetime.fromisoformat(src["last_ping_at"])
            scroll_depth = src.get("scroll_depth", 0.0)

            duration = (ended_dt - started_dt).total_seconds()

            # (7) 기사 길이 조회 (기본 읽기 정도 계산용)
            article_row = db.execute(
                text("""
                SELECT article_length
                FROM article_data
                WHERE article_id = :aid
                """),
                {"aid": article_id}
            ).fetchone()

            # 기사 정보 없으면 스킵
            if not article_row:
                continue

            article_length = article_row[0]

            # (8) 기본 읽기 정도 :공식: 실제 체류 시간 / (문자 수 * 0.07)
            base_read = duration / (article_length * 0.07)
            base_read = min(base_read, 1.0)  # 최대 1 제한

            # (9) 즉시 이탈 판단
            is_bounce = (duration < 5) or (scroll_depth < 0.2)

            # (10) 최종 선호도 점수
            final_score = 0.0 if is_bounce else round(
                base_read * 0.4 + scroll_depth * 0.6, 3
            )

            # (11) DB 저장 : session_data 테이블 (최종 세션 기록)
            db.execute(
                text("""
                INSERT INTO session_data
                (session_id, article_id, user_id, started_at, ended_at, scroll_depth, is_train)
                VALUES (:sid, :aid, :uid, :start, :end, :depth, :is_train)
                ON DUPLICATE KEY UPDATE
                    ended_at = VALUES(ended_at),
                    scroll_depth = VALUES(scroll_depth),
                    is_train = VALUES(is_train)
                """),
                {
                    "sid": session_id,
                    "aid": article_id,
                    "uid": user_id,
                    "start": started_dt,
                    "end": ended_dt,
                    "depth": scroll_depth,
                    "is_train": True
                }
            )

            # preference_score db테이블
            db.execute(
                text("""
                INSERT INTO preference_score
                (session_id, preference_score, occurred_at, is_bounce)
                VALUES (:sid, :score, :at, :bounce)
                ON DUPLICATE KEY UPDATE
                    preference_score = VALUES(preference_score),
                    occurred_at = VALUES(occurred_at),
                    is_bounce = VALUES(is_bounce)
                """),
                {
                    "sid": session_id,
                    "score": final_score,
                    "at": now,
                    "bounce": is_bounce
                }
            )

            # (12) ES 세션 종료 처리
            es.update(
                index="session_data",
                id=session_id,
                retry_on_conflict=3,
                doc={
                    "is_end": True,
                    "ended_signal": False
                })

        db.commit()
    finally:
        db.close()

    for uid in updated_users:
        try:
            logger.info(f"{uid}임베딩 업데이트 성공")
            update_user_embedding(uid)
        except Exception as e:
            pass
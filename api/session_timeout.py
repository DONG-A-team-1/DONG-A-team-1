"""
세션 타임아웃 처리 + RDB 저장 배치

역할 요약
1. ES에서 is_end=False 이면서
2. last_ping_at이 일정 시간 지난 세션 조회
3. 체류시간 / 스크롤 기반 선호도 점수 계산
4. DB(session_data, preference_score)에 저장
5. ES 세션 종료 처리
"""

from datetime import datetime, timezone, timedelta
from util.elastic import es
from util.database import get_db
from sqlalchemy.orm import Session

# 설정값
# ping이 이 시간(초) 이상 없으면 세션 종료로 판단
TIMEOUT_SECONDS = 120

# 메인 배치 함수
def close_timeout_sessions():
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
                    {"term": {"is_end": False}},
                    # 마지막 ping이 timeout 이전인 세션
                    {
                        "range": {
                            "last_ping_at": {
                                "lte": timeout_time.isoformat()
                            }
                        }
                    }
                ]
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
    db: Session = get_db()

    # 세션 하나씩 처리
    for s in sessions:
        src = s["_source"]

        session_id = src["session_id"]
        user_id = src["user_id"]
        article_id = src["article_id"]
        started_at = src["started_at"]
        last_ping_at = src["last_ping_at"]
        scroll_depth = src["scroll_depth"]

        # (6) 체류시간 계산 (초 단위)
        started_dt = datetime.fromisoformat(started_at)
        ended_dt = datetime.fromisoformat(last_ping_at)
        duration = (ended_dt - started_dt).total_seconds()

        # (7) 기사 길이 조회 (기본 읽기 정도 계산용)
        article_row = db.execute(
            """
            SELECT article_length
            FROM article_data
            WHERE article_id = :aid
            """,
            {"aid": article_id}
        ).fetchone()

        # 기사 정보 없으면 스킵
        if not article_row:
            continue

        article_length = article_row[0]

        # (8) 기본 읽기 정도 :공식: 실제 체류 시간 / (문자 수 * 0.07)
        base_read = duration / (article_length * 0.07)
        base_read = min(base_read, 1.0)  # 최대 1 제한

        # (9) 즉시 이탈 판단: 체류 < 5초 AND 스크롤 < 0.2
        is_bounce = duration < 5 and scroll_depth < 0.2

        # (10) 최종 선호도 점수
        if is_bounce:
            final_score = 0.0
        else:
            final_score = round(
                base_read * 0.4 + scroll_depth * 0.6,
                3
            )

        # (11) RDB 저장 : session_data 테이블 (최종 세션 기록)
        db.execute(
            """
            INSERT INTO session_data
            (session_id, article_id, user_id, started_at, ended_at, scroll_depth, is_train)
            VALUES (:sid, :aid, :uid, :start, :end, :depth, :is_train)
            """,
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
            """
            INSERT INTO preference_score
            (session_id, preference_score, occurred_at, is_bounce)
            VALUES (:sid, :score, :at, :bounce)
            """,
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
            doc={"is_end": True}
        )

    # (13) DB 반영 확정
    db.commit()

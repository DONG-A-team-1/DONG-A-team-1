import os
import time
import logging
from datetime import datetime, timezone, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from crawler.crawler_main import crawl_bigkinds_full
from labeler.topic_polar import label_polar_entity_centered_to_topics_json
from score.trend.article_trend_pipeline import run_article_trend_pipeline

from util.elastic import es
from util.elastic_templates import build_error_doc, build_info_docs  # ✅ info_logs 추가
from api.session_timeout import close_timeout_sessions
from util.scheduler_runtime import scheduler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))


def _run_id_kst() -> str:
    return datetime.now(KST).strftime("%Y%m%d_%H")


def register_jobs():
    """✅ job 등록만 분리 (admin에서 job id로 pause/resume 하려면 id가 고정이어야 함)"""

    now = datetime.now(KST)
    scheduler.add_job(
        run_pipeline,
        IntervalTrigger(hours=1),
        id="news_full_pipeline",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
        next_run_time=now,
    )

    scheduler.add_job(
        close_timeout_sessions,
        IntervalTrigger(minutes=1),
        id="session_timeout_job",
        replace_existing=True,
        max_instances=1
    )

    scheduler.add_job(
        run_polarity,
        CronTrigger(hour=5, minute=0),
        id="polarity_daily_0500",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
        paused=True,
    )

def register_jobs_test():
    scheduler.add_job(
        close_timeout_sessions,
        IntervalTrigger(minutes=1),
        id="session_timeout_job",
        replace_existing=True,
        max_instances=1
    )

def run_pipeline():
    run_id = _run_id_kst()
    env = os.getenv("APP_ENV", "dev")

    # =========================
    # 1) Crawl stage
    # =========================
    t0 = time.monotonic()
    try:
        logger.info("[PIPELINE] 기사 크롤링 시작")
        crawl_bigkinds_full()
        logger.info("[PIPELINE] 기사 크롤링 끝")

        # ✅ info_logs (stage summary)
        es.index(
            index="info_logs",
            document=build_info_docs (
                run_id=run_id,
                job_id="news_full_pipeline",
                component="scheduler",
                stage="crawl_end",
                status="ok",
                duration_ms=int((time.monotonic() - t0) * 1000),
                message="crawl_bigkinds_full completed",
                service_name="scheduler",
                env=env,
            )
        )

    except Exception as e:
        logger.exception("[PIPELINE] crawl step failed")

        # ✅ info_logs (stage summary)
        es.index(
            index="info_logs",
            document=build_info_docs (
                run_id=run_id,
                job_id="news_full_pipeline",
                component="scheduler",
                stage="crawl_end",
                status="error",
                duration_ms=int((time.monotonic() - t0) * 1000),
                message="crawl_bigkinds_full failed",
                error_message=str(e),
                retryable=True,
                service_name="scheduler",
                env=env,
            )
        )

        # ✅ 기존 error_log 그대로 유지
        doc = build_error_doc(
            message="[PIPELINE] crawl_bigkinds_full failed",
            service_name="scheduler",
            service_environment=env,
            pipeline_run_id=run_id,
            pipeline_job="news_full_pipeline",
            pipeline_step="crawl",
            event_severity=2,
            event_outcome="failure",
            exception=e,
            context={
                "scheduler_job_id": "news_full_pipeline",
                "stage": "crawl"
            },
            tags=["scheduler", "pipeline", "crawl"]
        )
        es.index(index="error_log", document=doc)
        return

    # =========================
    # 2) Trend stage
    # =========================
    t0 = time.monotonic()
    try:
        logger.info("[PIPELINE] 기사 트렌드 분석 시작")
        run_article_trend_pipeline()
        logger.info("[PIPELINE] 기사 트렌드 분석 끝")

        # ✅ info_logs (stage summary)
        es.index(
            index="info_logs",
            document=build_info_docs (
                run_id=run_id,
                job_id="news_full_pipeline",
                component="scheduler",
                stage="trend_end",
                status="ok",
                duration_ms=int((time.monotonic() - t0) * 1000),
                message="run_article_trend_pipeline completed",
                service_name="scheduler",
                env=env,
            )
        )

    except Exception as e:
        logger.exception("[PIPELINE] trend step failed")

        # ✅ info_logs (stage summary)
        es.index(
            index="info_logs",
            document=build_info_docs (
                run_id=run_id,
                job_id="news_full_pipeline",
                component="scheduler",
                stage="trend_end",
                status="error",
                duration_ms=int((time.monotonic() - t0) * 1000),
                message="run_article_trend_pipeline failed",
                error_message=str(e),
                retryable=True,
                service_name="scheduler",
                env=env,
            )
        )

        # ✅ 기존 error_log 그대로 유지
        doc = build_error_doc(
            message="[PIPELINE] article_trend_pipeline failed",
            service_name="scheduler",
            service_environment=env,
            pipeline_run_id=run_id,
            pipeline_job="news_full_pipeline",
            pipeline_step="trend",
            event_severity=2,
            event_outcome="failure",
            exception=e,
            context={
                "scheduler_job_id": "news_full_pipeline",
                "stage": "trend"
            },
            tags=["scheduler", "pipeline", "trend"]
        )
        es.index(index="error_log", document=doc)

def run_polarity():
    run_id = _run_id_kst()
    env = os.getenv("APP_ENV", "dev")

    t0 = time.monotonic()
    try:
        logger.info("polarity pipeline start")
        label_polar_entity_centered_to_topics_json(save_as_data=True)
        logger.info("polarity pipeline done")

        # ✅ info_logs (stage summary)
        es.index(
            index="info_logs",
            document=build_info_docs (
                run_id=run_id,
                job_id="polarity_daily_0500",
                component="scheduler",
                stage="polarity_end",
                status="ok",
                duration_ms=int((time.monotonic() - t0) * 1000),
                message="label_polar_entity_centered_to_topics_json completed",
                service_name="scheduler",
                env=env,
            )
        )

    except Exception as e:
        logger.exception("polarity pipeline failed")

        # ✅ info_logs (stage summary)
        es.index(
            index="info_logs",
            document=build_info_docs (
                run_id=run_id,
                job_id="polarity_daily_0500",
                component="scheduler",
                stage="polarity_end",
                status="error",
                duration_ms=int((time.monotonic() - t0) * 1000),
                message="polarity pipeline failed",
                error_message=str(e),
                retryable=True,
                service_name="scheduler",
                env=env,
            )
        )

        # ✅ 기존 error_log 그대로 유지
        doc = build_error_doc(
            message="[PIPELINE] polarity_daily_0500 failed",
            service_name="donga-scheduler",
            service_environment=env,
            pipeline_run_id=run_id,
            pipeline_job="polarity_daily_0500",
            pipeline_step="run_polarity",
            event_severity=2,
            event_outcome="failure",
            exception=e,
            context={
                "scheduler_job_id": "polarity_daily_0500",
                "trigger": "CronTrigger(05:00 KST)",
            },
            tags=["scheduler", "pipeline", "polarity_daily_0500"]
        )
        es.index(index="error_log", document=doc)

def main():
    env = os.getenv("APP_ENV", "dev")
    scheduler = BackgroundScheduler(timezone="Asia/Seoul")

    # 프로그램 시작 시 즉시 1회 실행 (실험용)
    logger.info("initial run (startup)")

    t0 = time.monotonic()
    try:
        run_pipeline()
        es.index(
            index="info_logs",
            document=build_info_docs(
                run_id=_run_id_kst(),
                job_id="scheduler_startup",
                component="scheduler",
                stage="startup_run_pipeline_end",
                status="ok",
                duration_ms=int((time.monotonic() - t0) * 1000),
                message="startup run_pipeline completed",
                service_name="donga-scheduler",
                env=env,
            )
        )
    except Exception as e:
        es.index(
            index="info_logs",
            document=build_info_docs(
                run_id=_run_id_kst(),
                job_id="scheduler_startup",
                component="scheduler",
                stage="startup_run_pipeline_end",
                status="error",
                duration_ms=int((time.monotonic() - t0) * 1000),
                message="startup run_pipeline failed",
                error_message=str(e),
                retryable=True,
                service_name="donga-scheduler",
                env=env,
            )
        )
        # (startup 실행은 실험용이라 error_log까지 꼭 남길지 선택)
        logger.exception("startup run_pipeline failed")

    time.sleep(60)

    t0 = time.monotonic()
    try:
        run_polarity()
        es.index(
            index="info_logs",
            document=build_info_docs(
                run_id=_run_id_kst(),
                job_id="scheduler_startup",
                component="scheduler",
                stage="startup_run_polarity_end",
                status="ok",
                duration_ms=int((time.monotonic() - t0) * 1000),
                message="startup run_polarity completed",
                service_name="donga-scheduler",
                env=env,
            )
        )
    except Exception as e:
        es.index(
            index="info_logs",
            document=build_info_docs(
                run_id=_run_id_kst(),
                job_id="scheduler_startup",
                component="scheduler",
                stage="startup_run_polarity_end",
                status="error",
                duration_ms=int((time.monotonic() - t0) * 1000),
                message="startup run_polarity failed",
                error_message=str(e),
                retryable=True,
                service_name="donga-scheduler",
                env=env,
            )
        )
        logger.exception("startup run_polarity failed")

    # 1시간마다 실행
    register_jobs()

    scheduler.start()
    logger.info("scheduler started (bigkinds - 1h interval)(topic_polar - at 5:00 AM")

    # ✅ info_logs: scheduler start summary
    es.index(
        index="info_logs",
        document=build_info_docs(
            run_id=_run_id_kst(),
            job_id="scheduler",
            component="scheduler",
            stage="scheduler_started_end",
            status="ok",
            message="apscheduler started",
            service_name="donga-scheduler",
            env=env,
        )
    )

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("scheduler stopped")

        # ✅ info_logs: scheduler stop summary
        es.index(
            index="info_logs",
            document=build_info_docs(
                run_id=_run_id_kst(),
                job_id="scheduler",
                component="scheduler",
                stage="scheduler_stopped_end",
                status="warn",
                message="apscheduler stopped",
                service_name="donga-scheduler",
                env=env,
            )
        )

def test_main():
    env = os.getenv("APP_ENV", "dev")
    test_scheduler = BackgroundScheduler(timezone="Asia/Seoul")
    logger.info("[TEST] scheduler test mode start")
    # 테스트용: 1분마다 run_pipeline 실행
    test_scheduler.add_job(
        close_timeout_sessions,
        IntervalTrigger(minutes=1),
        id="test_news_full_pipeline_1min",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=60
    )
    test_scheduler.start()
    logger.info("[TEST] 1-minute interval scheduler started")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        test_scheduler.shutdown()
        logger.info("[TEST] scheduler stopped")

if __name__ == "__main__":
    main()

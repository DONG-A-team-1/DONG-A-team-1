import time
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from crawler.crawler_main import crawl_bigkinds_full
from labeler.topic_polar import label_polar_entity_centered_to_topics_json

from score.trend.article_trend_pipeline import run_article_trend_pipeline
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pipeline():
    try:
        logger.info("[PIPELINE] 기사 크롤링 시작")
        crawl_bigkinds_full()          # 0~4 단계까지 내부 처리
        logger.info("[PIPELINE] 기사 크롤링 끝")

        logger.info("[PIPELINE] 기사 트렌드 분석 시작")
        run_article_trend_pipeline()   # 5 단계 트렌드 점수 부여
        logger.info("[PIPELINE] 기사 트렌드 분석 끝")

    except Exception:
        logger.exception("[PIPELINE] failed")

def run_polarity():
    try:
        logger.info("polarity pipeline start")
        label_polar_entity_centered_to_topics_json(save_as_data=True)
        logger.info("polarity pipeline done")
    except Exception:
        logger.exception("polarity pipeline failed")

def main():
    scheduler = BackgroundScheduler(timezone="Asia/Seoul")
    # 프로그램 시작 시 즉시 1회 실행 (실험용)
    logger.info("initial run (startup)")
    run_pipeline()
    time.sleep(60)
    run_polarity()
    # 1시간마다 실행
    scheduler.add_job(
        run_pipeline,
        IntervalTrigger(hours=1),
        id="news_full_pipeline",
        replace_existing=True,
        max_instances=1,     # 겹침 방지
        coalesce=True,       # 밀린 실행 합치기
        misfire_grace_time=300
    )

    scheduler.add_job(
        run_polarity,
        CronTrigger(hour=5, minute=0),
        id="polarity_daily_0500",
        replace_existing=True,
        max_instances=1,  # 겹침 방지
        coalesce=True,
        misfire_grace_time=3600  # 1시간까지 지연 허용
    )

    scheduler.start()
    logger.info("scheduler started (bigkinds - 1h interval)(topic_polar - at 5:00 AM")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("scheduler stopped")



if __name__ == "__main__":
    main()

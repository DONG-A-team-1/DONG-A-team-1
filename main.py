import time
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from crawler.crawler_main import crawl_bigkinds_full
from labeler.topic_polar import label_polar_entity_centered_to_topics_json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_crawl():
    try:
        logger.info("crawl_bigkinds_full start")
        crawl_bigkinds_full()
        logger.info("crawl_bigkinds_full done")
    except Exception:
        logger.exception("crawl_bigkinds_full failed")

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
    run_crawl()
    time.sleep(60)
    run_polarity()
    # 1시간마다 실행
    scheduler.add_job(
        run_crawl,
        IntervalTrigger(hours=1),
        id="bigkinds_crawl",
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
    # main()
#     run_crawl()
    main()

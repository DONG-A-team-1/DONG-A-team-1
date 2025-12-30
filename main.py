import time
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from crawler.crawler_main import crawl_bigkinds_full

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_crawl():
    try:
        logger.info("crawl_bigkinds_full start")
        crawl_bigkinds_full()
        logger.info("crawl_bigkinds_full done")
    except Exception:
        logger.exception("crawl_bigkinds_full failed")

def main():
    scheduler = BackgroundScheduler(timezone="Asia/Seoul")

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

    scheduler.start()
    logger.info("scheduler started (1h interval)")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("scheduler stopped")

if __name__ == "__main__":
    # main()
    run_crawl()

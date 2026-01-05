import time
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from crawler.crawler_main import crawl_bigkinds_full
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

def main():
    scheduler = BackgroundScheduler(timezone="Asia/Seoul")
    # 프로그램 시작 시 즉시 1회 실행 (실험용)
    logger.info("initial run (startup)")
    run_pipeline()

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

    scheduler.start()
    logger.info("scheduler started (1h interval)")

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("scheduler stopped")

if __name__ == "__main__":
    main()

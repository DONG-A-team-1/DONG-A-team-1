import time
import asyncio
from datetime import datetime, timedelta, timezone
import traceback

from wordcloud.wordCloudMaker import make_wordcloud_data
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from score.trust.trust_pipline import run_trust_pipeline

from crawler.kbs_crawler import kbs_crawl
from crawler.donga_crawler import donga_crawl
from crawler.chosun_crawler import chosun_crawl
from crawler.kmib_crawler import kmib_crawl
from crawler.hani_crawler import hani_crawl
from crawler.naeil_crawl import naeil_crawl
from crawler.everyday_crawler import everyday_crawl
from crawler.hankookilbo_crawler import hankookilbo_crawl

from util.cleaner import clean_articles
from util.elastic import es
from util.logger import Logger
from util.elastic_templates import build_error_doc, build_info_docs
from util.repository import upsert_article

from labeler.create_embeddings import create_embedding
from labeler.categorizer import categorizer

logger = Logger().get_logger(__name__)
KST = timezone(timedelta(hours=9))


def crawl_bigkinds_full():  # 이건 그냥 셀레니움하기위한 셋업
    now_kst = datetime.now(KST).isoformat(timespec="seconds")
    run_id = now_kst[:13].replace("-", "").replace("T", "_")  # 예: 20260107_14
    job_id = "crawl_bigkinds_full"
    t_job0 = time.monotonic()

    print(f"[{now_kst}] 빅카인즈 전체 크롤링 시작")

    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    # options.add_argument("--headless")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    press_list = ["동아일보", "KBS", "한겨레", "조선일보", "국민일보", "내일신문", "매일신문", "한국일보"]
    all_results = []
    big_error_list = []

    success_list = []

    for press_name in press_list:
        print(f"==== {press_name} 크롤링 시작 ====")
        press_results = []
        driver.get("https://www.bigkinds.or.kr/v2/news/index.do")
        time.sleep(2)

        # 1) 언론사 선택 기능
        try:
            checkbox = driver.find_element(By.ID, press_name)
            driver.execute_script("arguments[0].click();", checkbox)
        except Exception as e:
            big_error_list.append({
                "error_type": type(e).__name__,
                "error_message": f"{press_name} : {str(e)}"
            })
            # ✅ info_logs: press selection 실패도 stage summary로 남김
            es.index(
                index="info_logs",
                document=build_info_docs(
                    run_id=run_id,
                    job_id=job_id,
                    component="crawler",
                    stage=f"{press_name}_select_end",
                    status="error",
                    duration_ms=None,
                    input_cnt=0,
                    success_cnt=0,
                    failed_cnt=1,
                    message=f"{press_name} press checkbox select failed",
                    error_message=str(e),
                    retryable=True
                )
            )
            continue
        time.sleep(1)

        # 3) 검색 클릭 기능
        try:
            search_btn = driver.find_element(
                By.CSS_SELECTOR,
                "#search-foot-div > div.foot-btn > button.btn.btn-search.news-search-btn.news-report-search-btn"
            )
            driver.execute_script("arguments[0].click();", search_btn)
        except Exception as e:
            big_error_list.append({
                "error_type": type(e).__name__,
                "error_message": f"{press_name} : {str(e)}"
            })
            # 검색 실패는 press 단위로 계속 진행 가능하니 warning으로 남김
            es.index(
                index="info_logs",
                document=build_info_docs(
                    run_id=run_id,
                    job_id=job_id,
                    component="crawler",
                    stage=f"{press_name}_search_click_end",
                    status="warn",
                    duration_ms=None,
                    input_cnt=0,
                    success_cnt=0,
                    failed_cnt=1,
                    message=f"{press_name} search click failed",
                    error_message=str(e),
                    retryable=True
                )
            )
            pass
        time.sleep(3)

        # 5) 뉴스분석 클릭 기능
        try:
            analysis_btn = driver.find_element(By.CSS_SELECTOR, "button.step-3-click")
            driver.execute_script("arguments[0].click();", analysis_btn)
        except Exception as e:
            big_error_list.append({
                "error_type": type(e).__name__,
                "error_message": f"{press_name} : {str(e)}"
            })
            # ✅ 분석 버튼 실패는 press 단위 진행 불가 → error
            es.index(
                index="info_logs",
                document=build_info_docs(
                    run_id=run_id,
                    job_id=job_id,
                    component="crawler",
                    stage=f"{press_name}_analysis_click_end",
                    status="error",
                    duration_ms=None,
                    input_cnt=0,
                    success_cnt=0,
                    failed_cnt=1,
                    message=f"{press_name} analysis click failed",
                    error_message=str(e),
                    retryable=True
                )
            )
            continue

        time.sleep(4)

        # 6) 테이블 rows 가져오기
        rows = driver.find_elements(By.CSS_SELECTOR, "#preview-wrap > table > tbody > tr")

        # ✅ BigKinds table parse 단계 타이머(press 단위)
        t_press_table0 = time.monotonic()
        parsed_cnt = 0

        for row in rows:
            row_id = row.get_attribute("id")
            row_no = row_id.split('-')[1]

            keywords_raw = driver.find_element(By.CSS_SELECTOR, f'td[id="14-{row_no}"]').text
            feature_raw = driver.find_element(By.CSS_SELECTOR, f'td[id="15-{row_no}"]').text

            org_raw = driver.find_element(By.CSS_SELECTOR, f'td[id="13-{row_no}"]').text
            person_raw = driver.find_element(By.CSS_SELECTOR, f'td[id="11-{row_no}"]').text

            keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()]
            features = [f.strip() for f in feature_raw.split(",") if f.strip()]

            org = [k.strip() for k in org_raw.split(",") if k.strip()]
            person = [k.strip() for k in person_raw.split(",") if k.strip()]

            data = {
                "press": driver.find_element(By.CSS_SELECTOR, f'td[id="2-{row_no}"]').text,
                "article_id": driver.find_element(By.CSS_SELECTOR, f'td[id="0-{row_no}"]').text,
                "upload_date": driver.find_element(By.CSS_SELECTOR, f'td[id="1-{row_no}"]').text,
                "reporter": driver.find_element(By.CSS_SELECTOR, f'td[id="3-{row_no}"]').text,
                "keywords": keywords,
                "features": features,
                "url": driver.find_element(By.CSS_SELECTOR, f'td[id="17-{row_no}"]').text,
                "collected_at": now_kst,
                "entities": {
                    "org": org,
                    "person": person
                }
            }

            all_results.append(data)
            press_results.append(data)
            parsed_cnt += 1

            # BigKinds 1차 ES 적재(덮어쓰기)
            es.index(
                index="article_data",
                document=data,
                id=data['article_id']
            )

        # ✅ press 단위: BigKinds 테이블 파싱/1차 적재 완료 요약
        es.index(
            index="info_logs",
            document=build_info_docs(
                run_id=run_id,
                job_id=job_id,
                component="crawler",
                stage=f"{press_name}_bigkinds_table_end",
                status="ok",
                duration_ms=int((time.monotonic() - t_press_table0) * 1000),
                input_cnt=parsed_cnt,
                success_cnt=parsed_cnt,
                failed_cnt=0,
                message=f"{press_name} parsed {parsed_cnt} rows and indexed to article_data"
            )
        )

        # ✅ press 단위: 언론사별 원문 크롤러 호출
        try:
            t_press_crawl0 = time.monotonic()

            if press_name == "동아일보":
                result = asyncio.run(donga_crawl(press_results))
            elif press_name == "KBS":
                result = asyncio.run(kbs_crawl(press_results))
            elif press_name == "한겨레":
                result = asyncio.run(hani_crawl(press_results))
            elif press_name == "조선일보":
                result = asyncio.run(chosun_crawl(press_results))
            elif press_name == "국민일보":
                result = asyncio.run(kmib_crawl(press_results))
            elif press_name == "내일신문":
                result = asyncio.run(naeil_crawl(press_results))
            elif press_name == "매일신문":
                result = asyncio.run(everyday_crawl(press_results))
            elif press_name == "한국일보":
                result = asyncio.run(hankookilbo_crawl(press_results))
            else:
                result = []

            result = result or []
            success_list.extend(result)

            es.index(
                index="info_logs",
                document=build_info_docs(
                    run_id=run_id,
                    job_id=job_id,
                    component="crawler",
                    stage=f"{press_name}_crawl_end",
                    status="ok",
                    duration_ms=int((time.monotonic() - t_press_crawl0) * 1000),
                    input_cnt=len(press_results),
                    success_cnt=len(result),
                    failed_cnt=max(0, len(press_results) - len(result)),
                    message=f"{press_name} crawler finished"
                )
            )

        except Exception as e:

            # 기존 error_log 유지
            es.index(
                index="error_log",
                document=build_error_doc(
                    message=f"{press_name} 크롤러 호출 실패",
                    service_name="crawler",
                    service_environment="dev",
                    pipeline_job="crawl_bigkinds_full",
                    pipeline_step=f"{press_name}_crawl",
                    event_severity=3,
                    exception=e,
                    samples=[{
                        "press": press_name,
                        "traceback": traceback.format_exc()
                    }],
                    tags=["crawler", "bigkinds", press_name]
                )
            )

    es.index(
        index="info_logs",
        document=build_info_docs(
            run_id=run_id,
            job_id=job_id,
            component="crawler",
            stage="bigkinds_collect_and_press_crawl_end",
            status="ok",
            duration_ms=int((time.monotonic() - t_job0) * 1000),
            input_cnt=len(all_results),
            success_cnt=len(success_list),
            failed_cnt=max(0, len(all_results) - len(success_list)),
            message=f"all press done. collected={len(all_results)} success={len(success_list)}"
        )
    )

    driver.quit()

    # 워드클라우드
    if all_results:
        print("📊 워드클라우드용 키워드 추출 시작...")
        t0 = time.monotonic()
        asyncio.run(make_wordcloud_data(all_results))
        es.index(
            index="info_logs",
            document=build_info_docs(
                run_id=run_id,
                job_id=job_id,
                component="wordcloud",
                stage="make_wordcloud_data_end",
                status="ok",
                duration_ms=int((time.monotonic() - t0) * 1000),
                input_cnt=len(all_results),
                success_cnt=1,
                failed_cnt=0,
                message="wordcloud data generated"
            )
        )

    id_list = [data["article_id"] for data in all_results]

    logger.info(f"[{now_kst}] 빅카인즈 전체 크롤링 완료. 총 {len(all_results)}개 기사 수집")
    time.sleep(30)

    print(len(success_list))
    logger.info(f"[{len(id_list)}] 개 기사 중 . 총 {len(id_list) - len(success_list)}개 결측치 발생")

    # 전처리
    logger.info("기사 본문 전처리 및 업데이트")
    t0 = time.monotonic()
    clean_articles(success_list)
    es.index(
        index="info_logs",
        document=build_info_docs(
            run_id=run_id,
            job_id=job_id,
            component="preprocess",
            stage="clean_articles_end",
            status="ok",
            duration_ms=int((time.monotonic() - t0) * 1000),
            input_cnt=len(success_list),
            success_cnt=len(success_list),
            failed_cnt=0,
            message="clean_articles updated article_data"
        )
    )

    logger.info("기사별 임베딩 생성")

    if success_list:
        # 임베딩
        t0 = time.monotonic()
        create_embedding(success_list)
        es.index(
            index="info_logs",
            document=build_info_docs(
                run_id=run_id,
                job_id=job_id,
                component="embedding",
                stage="create_embedding_end",
                status="ok",
                duration_ms=int((time.monotonic() - t0) * 1000),
                input_cnt=len(success_list),
                success_cnt=len(success_list),
                failed_cnt=0
            )
        )

        # 카테고라이저
        t0 = time.monotonic()
        categorizer(success_list)
        es.index(
            index="info_logs",
            document=build_info_docs(
                run_id=run_id,
                job_id=job_id,
                component="categorizer",
                stage="categorizer_end",
                status="ok",
                duration_ms=int((time.monotonic() - t0) * 1000),
                input_cnt=len(success_list),
                success_cnt=len(success_list),
                failed_cnt=0
            )
        )

        # 신뢰도
        t0 = time.monotonic()
        run_trust_pipeline(success_list)
        es.index(
            index="info_logs",
            document=build_info_docs(
                run_id=run_id,
                job_id=job_id,
                component="trust",
                stage="trust_pipeline_end",
                status="ok",
                duration_ms=int((time.monotonic() - t0) * 1000),
                input_cnt=len(success_list),
                success_cnt=len(success_list),
                failed_cnt=0
            )
        )

        time.sleep(30)

        # DB upsert
        t0 = time.monotonic()
        upsert_article(success_list)
        es.index(
            index="info_logs",
            document=build_info_docs(
                run_id=run_id,
                job_id=job_id,
                component="db",
                stage="upsert_article_end",
                status="ok",
                duration_ms=int((time.monotonic() - t0) * 1000),
                input_cnt=len(success_list),
                success_cnt=len(success_list),
                failed_cnt=0,
                message="upserted to DB"
            )
        )

    # ✅ 세션 요약 stage summary
    es.index(
        index="info_logs",
        document=build_info_docs(
            run_id=run_id,
            job_id=job_id,
            component="crawler",
            stage="session_summary_end",
            status="warn" if big_error_list else "ok",
            duration_ms=int((time.monotonic() - t_job0) * 1000),
            input_cnt=len(id_list),
            success_cnt=len(success_list),
            failed_cnt=max(0, len(id_list) - len(success_list)),
            message=f"errors={len(big_error_list)} missing={max(0, len(id_list) - len(success_list))}",
            error_message=f"bigkinds_errors={len(big_error_list)}" if big_error_list else None,
            retryable=False
        )
    )

    # 기존 error_log 세션 요약 유지
    if 0 < len(big_error_list) < 20:
        error_doc = build_error_doc(
            message=f"BigKinds 크롤링 중 {len(big_error_list)}개 에러 발생",
            service_name="crawler",
            pipeline_run_id=run_id,
            pipeline_job="crawl_bigkinds_full",
            pipeline_step="individual_press",
            event_severity=4,
            event_outcome="warning",
            metrics={
                "error_count": len(big_error_list),
                "success_count": len(success_list),
            },
            samples=big_error_list,
            tags=["crawler", "bigkinds", "session-summary"],
        )
        es.index(index="error_log", document=error_doc)
    elif len(big_error_list) >= 20:
        error_doc = build_error_doc(
            message=f"BigKinds 크롤링 중 {len(big_error_list)}개 에러 발생, DOM 객체 확인 필요",
            service_name="crawler",
            pipeline_run_id=run_id,
            pipeline_job="crawl_bigkinds_full",
            pipeline_step="individual_press",
            event_severity=3,
            event_outcome="warning",
            metrics={
                "error_count": len(big_error_list),
                "success_count": len(success_list),
            },
            samples=big_error_list,
            tags=["crawler", "bigkinds", "session-summary"],
        )
        es.index(index="error_log", document=error_doc)
    return all_results


if __name__ == '__main__':
    crawl_bigkinds_full()

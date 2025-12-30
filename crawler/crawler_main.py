import time
import asyncio
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from datetime import timedelta, timezone
import traceback


from .kbs_crawler import kbs_crawl
from .donga_crawler import donga_crawl
from .chosun_crawler import chosun_crawl
from .kmib_crawler import kmib_crawl
from .hani_crawler import hani_crawl
from .naeil_crawl import naeil_crawl
from .everyday_crawler import  everyday_crawl
from .hankookilbo_crawler import hankookilbo_crawl

from util.cleaner import clean_articles
from util.elastic import es
from util.logger import Logger
from util.elastic_templates import build_error_doc

from labeler.create_embeddings import create_embedding
from labeler.categorizer import categorizer

logger = Logger().get_logger(__name__)
KST = timezone(timedelta(hours=9))

def crawl_bigkinds_full(): # 이건 그냥 셀레니움하기위한 셋업
    now_kst = datetime.now(KST).isoformat(timespec="seconds")
    print(f"[{now_kst}] 빅카인즈 전체 크롤링 시작")
    options = webdriver.ChromeOptions() 
    options.add_argument("--start-maximized")
    # options.add_argument("--headless")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    press_list = ["동아일보", "KBS", "한겨레", "조선일보", "국민일보","내일신문","매일신문","한국일보"]
    # press_list = ["조선일보"]
    all_results = [] # 빈 리스트 생성해서 이따 JSON 데이터 담을 예정
    big_error_list =[]

    success_list = []
    for press_name in press_list:
        print(f"==== {press_name} 크롤링 시작 ====")
        press_results = [] # 언론사 변경시마다 빈 리스트 생성해서 언론사별 최신 기사 저장 및 전달
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
            continue
        time.sleep(1)

        # 2) 적용하기 클릭 기능
        # try:
        #     apply_btn = driver.find_element(By.CSS_SELECTOR, ".btn-apply")
        #     driver.execute_script("arguments[0].click();", apply_btn)
        # except Exception as e:
        #     big_error_list.append({
        #         "error_type": type(e).__name__,
        #         "error_message": f"{press_name} : {str(e)}"
        #     })
        #     pass
        # time.sleep(1.5)

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
            pass
        time.sleep(3)

        # 4) 기사 20개로 변경 (25.12.23 이지민 주석 변경[30→20])
        # try:
        #     select_tag = Select(driver.find_element(By.ID, "select2"))
        #     select_tag.select_by_value("20")
        #     print("4")
        # except Exception as e:
        #     big_error_list.append({
        #         "error_type": type(e).__name__,
        #         "error_message": f"{press_name} : {str(e)}"
        #     })
        #
        #     pass
        # time.sleep(3)

        # 5) 뉴스분석 클릭 기능
        try:
            analysis_btn = driver.find_element(By.CSS_SELECTOR, "button.step-3-click")
            driver.execute_script("arguments[0].click();", analysis_btn)
        except Exception as e:
            big_error_list.append({
                "error_type": type(e).__name__,
                "error_message": f"{press_name} : {str(e)}"
            })
            continue

        time.sleep(4)

        # 6) 테이블 rows 가져오기 (해당 페이지 넘어갔을 때 엑셀 rows 요소들 불러오는 것)
        rows = driver.find_elements(By.CSS_SELECTOR, "#preview-wrap > table > tbody > tr")

        for row in rows:
            row_id = row.get_attribute("id")
            row_no = row_id.split('-')[1]

            keywords_raw = driver.find_element(By.CSS_SELECTOR, f'td[id="14-{row_no}"]').text
            feature_raw = driver.find_element(By.CSS_SELECTOR, f'td[id="15-{row_no}"]').text  # <-- FIX

            keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()]
            features = [f.strip() for f in feature_raw.split(",") if f.strip()]

            data = {
                "press": driver.find_element(By.CSS_SELECTOR, f'td[id="2-{row_no}"]').text, # 언론사명
                "article_id": driver.find_element(By.CSS_SELECTOR, f'td[id="0-{row_no}"]').text, # 빅카인즈 고유식별번호
                "upload_date": driver.find_element(By.CSS_SELECTOR, f'td[id="1-{row_no}"]').text, # 업로드 날짜
                "reporter": driver.find_element(By.CSS_SELECTOR, f'td[id="3-{row_no}"]').text, # 기자
                "keywords": keywords, # 키워드
                "features": features, # 가중치순 상위 50개
                "url": driver.find_element(By.CSS_SELECTOR, f'td[id="17-{row_no}"]').text, # 기사 원문 링크
                "collected_at": now_kst # 모든 칼럼을 json으로 변환해서 해당 컬럼에 박은 것
            }

            all_results.append(data)
            press_results.append(data)

            # 해당 세션에서 수집된 모든 기사의 article_id를 수집하여 리스트 생성,
            # 추후 각기 다른 작업들의 범위를 일정하게, 안정적으로 맞추기 위해서

            # article_data 인덱스에 우선 bigkinds 내용 저장
            es.index(
                index="article_data",
                document=data,
                id = data['article_id']
            )

        try:
            # 여기 하단에서부터는 언론사별 개별 크롤링 실행
            if press_name == "동아일보":
                result = asyncio.run(donga_crawl(press_results))
                success_list.extend(result)
            elif press_name == "KBS":
                result = asyncio.run(kbs_crawl(press_results))
                success_list.extend(result)
            elif press_name == "한겨레":
                result = asyncio.run(hani_crawl(press_results))
                success_list.extend(result)
            elif press_name == "조선일보":
                result = asyncio.run(chosun_crawl(press_results))
                success_list.extend(result)
            elif press_name == "국민일보":
                result = asyncio.run(kmib_crawl(press_results))
                success_list.extend(result)
            elif press_name == "내일신문":
                result = asyncio.run(naeil_crawl(press_results))
                success_list.extend(result)
            elif press_name == "매일신문":
                result = asyncio.run(everyday_crawl(press_results))
                success_list.extend(result)
            elif press_name == "한국일보":
                result = asyncio.run(hankookilbo_crawl(press_results))
                success_list.extend(result)
        except Exception as e:

            es.index(
                index="error_log",
                document=build_error_doc(
                    message=f"{press_name} 크롤러 호출 실패",
                    samples=[{
                        "press": press_name,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "traceback": traceback.format_exc()
                    }]
                )
            )
    driver.quit()

    id_list = [data["article_id"] for data in all_results]

    logger.info(f"[{now_kst}] 빅카인즈 전체 크롤링 완료. 총 {len(all_results)}개 기사 수집")
    time.sleep(30)
    # null_id = delete_null(id_list)  # ✅ article_raw랑 비교하지 말고, 이번 세션 id_list만 기준
    print(len(success_list))
    logger.info(f"[{id_list}] 개 기사 중 . 총 {len(id_list) - len(success_list)}개 결측치 발생")
    logger.info("기사 본문 전처리 및 업데이트")
    clean_articles(success_list) # 기사 원문(제목,본문)에 대해서 클리닝 작업 실행 및 article_data의 해당 필드 업데이트
    logger.info("기사별 임베딩 생성")
    if success_list:
        create_embedding(success_list)   # 기사별 임베딩 생성 및 article_data의 article_embedding 필드 업데이트
        categorizer(success_list)
    else:
        pass


    if len(big_error_list) > 0 :
        error_doc = build_error_doc(
            message=f"{len(big_error_list)}개 에러 발생",
            samples=big_error_list
        )
        es.index(index="error_log", document=error_doc)
    return all_results

if __name__ == '__main__':
    crawl_bigkinds_full()

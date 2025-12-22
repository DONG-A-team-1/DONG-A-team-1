import time
import json
import asyncio
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy import text
from datetime import timedelta, timezone


from .kbs_crawler import kbs_crawl
from .donga_crawler import donga_crawl
from .chosun_crawler import chosun_crawl
from .kmib_crawler import kmib_crawl
from .hani_crawler import hani_crawl
from .naeil_crawl import naeil_crawl

from .cleaner import clean_articles,  delete_null
from embedding import create_embedding
from util.elastic import es
from util.logger import Logger


logger = Logger().get_logger(__name__)
KST = timezone(timedelta(hours=9))

def crawl_bigkinds_full(): # 이건 그냥 셀레니움하기위한 셋업
    now_kst = datetime.now(KST).isoformat(timespec="seconds")
    print(f"[{now_kst}] 빅카인즈 전체 크롤링 시작")
    options = webdriver.ChromeOptions() 
    options.add_argument("--start-maximized")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    press_list = ["동아일보", "KBS", "한겨레", "조선일보", "국민일보","내일신문"]
    # press_list = ["내일신문"]

    all_results = [] # 빈 리스트 생성해서 이따 JSON 데이터 담을 예정

    for press_name in press_list:
        print(f"==== {press_name} 크롤링 시작 ====")
        press_results = [] # 언론사 변경시마다 빈 리스트 생성해서 언론사별 최신 기사 저장 및 전달
        driver.get("https://www.bigkinds.or.kr/v2/news/index.do")
        time.sleep(2)

        # 1) 언론사 선택 기능
        try:
            checkbox = driver.find_element(By.ID, press_name)
            driver.execute_script("arguments[0].click();", checkbox)
        except:
            continue

        time.sleep(1)

        # 2) 적용하기 클릭 기능
        try:
            apply_btn = driver.find_element(By.CSS_SELECTOR, ".btn-apply")
            driver.execute_script("arguments[0].click();", apply_btn)
        except:
            pass

        time.sleep(1.5)

        # 3) 검색 클릭 기능
        search_btn = driver.find_element(
            By.CSS_SELECTOR,
            "#search-foot-div > div.foot-btn > button.btn.btn-search.news-search-btn.news-report-search-btn"
        )
        driver.execute_script("arguments[0].click();", search_btn)

        time.sleep(3)

        # 4) 기사 30개로 변경
        try:
            select_tag = Select(driver.find_element(By.ID, "select2"))
            select_tag.select_by_value("20")
        except:
            pass

        time.sleep(3)

        # 5) 뉴스분석 클릭 기능
        try:
            analysis_btn = driver.find_element(By.CSS_SELECTOR, "button.step-3-click")
            driver.execute_script("arguments[0].click();", analysis_btn)
        except:
            continue

        time.sleep(4)

        # 6) 테이블 rows 가져오기 (해당 페이지 넘어갔을 때 엑셀 rows 요소들 불러오는 것)
        rows = driver.find_elements(By.CSS_SELECTOR, "#preview-wrap > table > tbody > tr")

        for row in rows:
            row_id = row.get_attribute("id")
            row_no = row_id.split('-')[1]

            try:
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

            except Exception as e:
                print("[오류] 데이터 처리 실패:", e)
                continue
        # 여기 하단에서부터는 언론사별 개별 크롤링 실행
        if press_name == "동아일보":
            asyncio.run(donga_crawl(press_results))
        elif press_name == "KBS":
            asyncio.run(kbs_crawl(press_results))
        elif press_name == "한겨레":
            asyncio.run(hani_crawl(press_results))
        elif press_name == "조선일보":
            asyncio.run(chosun_crawl(press_results))
        elif press_name == "국민일보":
            asyncio.run(kmib_crawl(press_results))
        elif press_name == "내일신문":
            asyncio.run(naeil_crawl(press_results))

    driver.quit()

    id_list = [data["article_id"] for data in all_results]

    logger.info(f"[{now_kst}] 빅카인즈 전체 크롤링 완료. 총 {len(all_results)}개 기사 수집")

    null_id = delete_null() # 기사 원문 수집에 실패한 기사들에 대해서 삭제 진행 및 결측치로 인해 삭제된 article_id 명시해줌
    logger.info(f"[{now_kst}] 개 기사 중 . 총 {len(all_results) - len(null_id)}개 결측치 발생")
    article_list = list(set(id_list) - set(null_id)) # 상단에서 명시된 결측 기사들을 추후 작업에서 제외합니다
    logger.info("기사 본문 전처리 및 업데이트")
    clean_articles(article_list ) # 기사 원문(제목,본문)에 대해서 클리닝 작업 실행 및 article_data의 해당 필드 업데이트
    logger.info("기사별 임베딩 생성")
    create_embedding(article_list)   # 기사별 임베딩 생성 및 article_data의 article_embedding 필드 업데이트
    return all_results

if __name__ == '__main__':
    crawl_bigkinds_full()
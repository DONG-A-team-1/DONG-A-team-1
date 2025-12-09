import time
import json
import asyncio
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
# from database import get_db
from sqlalchemy import text

from crawler.kbs_crawler import kbs_crawl
from donga_crawler import donga_crawl
from  chosun_crawler import chosun_crawl

def crawl_bigkinds_full(): # 이건 그냥 셀레니움하기위한 셋업

    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # press_list = ["동아일보", "KBS", "한겨레", "조선일보", "중앙일보", "국민일보"]
    press_list = ["동아일보", "KBS","조선일보"]

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
            select_tag.select_by_value("30")
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
                data = {
                    "press": driver.find_element(By.CSS_SELECTOR, f'td[id="2-{row_no}"]').text, # 언론사명
                    "news_id": driver.find_element(By.CSS_SELECTOR, f'td[id="0-{row_no}"]').text, # 빅카인즈 고유식별번호
                    "upload_date": driver.find_element(By.CSS_SELECTOR, f'td[id="1-{row_no}"]').text, # 기자
                    "reporter": driver.find_element(By.CSS_SELECTOR, f'td[id="3-{row_no}"]').text, # 업로드한 날짜
                    "keywords": driver.find_element(By.CSS_SELECTOR, f'td[id="14-{row_no}"]').text, # 키워드
                    "features_top50": driver.find_element(By.CSS_SELECTOR, f'td[id="15-{row_no}"]').text, # 가중치순 상위 50개
                    "url": driver.find_element(By.CSS_SELECTOR, f'td[id="17-{row_no}"]').text, # 기사 원문 링크
                    "collected_at": str(datetime.now()) # 모든 칼럼을 json으로 변환해서 해당 컬럼에 박은 것
                }

                all_results.append(data)
                press_results.append(data) # 언론사별 데이터를 묶어서 저장하고 언론사가 변경 될 때마다 삭제됩니다
                # ------------------------
                # DB JSON INSERT
                # ------------------------
                # sql = text("""
                #     INSERT INTO news_analysis
                #     (press, news_id, reporter, upload_date, keywords, features, url, raw_json)
                #     VALUES
                #     (:press, :news_id, :reporter, :upload_date, :keywords, :features, :url, :raw_json)
                # """)
                #
                # params = {
                #     "press": data["press"],
                #     "news_id": data["news_id"],
                #     "reporter": data["reporter"],
                #     "upload_date": data["upload_date"],
                #     "keywords": json.dumps(data["keywords"], ensure_ascii=False),
                #     "features": json.dumps(data["features_top50"], ensure_ascii=False),
                #     "url": data["url"],
                #     "raw_json": json.dumps(data, ensure_ascii=False)
                # }
                #
                # with get_db() as db:
                #     db.execute(sql, params)
                #     db.commit()

            except Exception as e:
                print("[오류] 데이터 처리 실패:", e)
                continue

        if press_name == "동아일보":
            asyncio.run(donga_crawl(press_results))
        elif press_name == "KBS":
            asyncio.run(kbs_crawl(press_results))
        elif press_name == "한겨레":
            pass
        elif press_name == "조선일보":
            asyncio.run(chosun_crawl(press_results))
        elif press_name == "중앙일보":
            pass
        elif press_name == "국민일보":
            pass

    driver.quit()
    return all_results

if __name__ == '__main__':
    crawl_bigkinds_full()
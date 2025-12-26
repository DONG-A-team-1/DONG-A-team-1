from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from datetime import datetime
from elasticsearch import Elasticsearch
import time


# ES 설정
es = Elasticsearch("http://localhost:9200")
INDEX_NAME = "google_trends"

# ===============================
# Chrome 설정
# ===============================
driver_path = "D:/STUDY/git/DONG-A-team-1/driver/chromedriver.exe"

options = Options()
options.add_argument("--remote-allow-origins=*")
options.add_argument("--start-maximized")

# ===============================
# TrendScore (BigKinds 방식)
# ===============================
def trend_score(rank: int, N: int) -> float:
    return round((N + 1 - rank) / N, 3)

# ===============================
# 크롤링
# ===============================
def crawl_trends():
    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=options)

    url = "https://trends.google.com/trending?geo=KR&hl=ko"
    driver.get(url)
    print("[1] Google Trends 접속 완료")

    time.sleep(3)

    elements = driver.find_elements(
        By.CSS_SELECTOR,
        "#trend-table > div.enOdEe-wZVHld-zg7Cn-haAclf > table > tbody:nth-child(3) > tr"
    )

    N = len(elements)
    print(f"[2] 수집된 트렌드 수: {N}")

    trends = []

    for rank, elem in enumerate(elements, start=1):
        title = elem.find_element(
            By.CSS_SELECTOR, "td:nth-child(2) div.mZ3RIc"
        ).text.strip()

        trends.append({
            "rank": rank,
            "title": title,
            "trend_score": trend_score(rank, N)
        })

    driver.quit()

    # ===============================
    # ES 문서 생성
    # ===============================
    doc = {
        "collected_at": datetime.now().strftime("%Y%m%d%H"),
        "trends": trends
    }

    es.index(index=INDEX_NAME, document=doc)
    print("[3] ES 저장 완료")

# ===============================
# 실행
# ===============================
if __name__ == "__main__":
    crawl_trends()

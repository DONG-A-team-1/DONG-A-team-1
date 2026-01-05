from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime, timezone, timedelta
from elasticsearch import Elasticsearch
import time

KST = timezone(timedelta(hours=9))

# ES 설정
es = Elasticsearch("http://localhost:9200")
INDEX_NAME = "google_trends"

# TrendScore (BigKinds와 스케일 통일: 0~1, 고정 Top25 기준)
def trend_score(rank: int, _: int = None) -> float:
    MAX_RANK = 25
    return round((MAX_RANK + 1 - rank) / MAX_RANK, 3)


def crawl_trends():
    # Chrome 옵션
    options = Options()
    options.add_argument("--remote-allow-origins=*")
    options.add_argument("--start-maximized")

    # 스케줄러용 안전 옵션 (강력 추천)
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # 서버 배포할 때는 켜야함! [브라우저 창을 띄우지 않고 백그라운드에서 크롬을 실행]
    # options.add_argument("--headless=new")

    # webdriver-manager 사용 (exe 필요 파일 컴퓨터에 둘 필요없게 함 - 캐시 사용, 구동할 때마다 구동 되는 거 아님)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        url = "https://trends.google.com/trending?geo=KR&hl=ko"
        driver.get(url)
        print("[1] Google Trends 접속 완료")
        time.sleep(3)

        elements = driver.find_elements(
            By.CSS_SELECTOR, "#trend-table > div.enOdEe-wZVHld-zg7Cn-haAclf > table > tbody:nth-child(3) > tr" )

        if not elements:
            raise RuntimeError("Google Trends 테이블 로드 실패")

        N = len(elements)
        print(f"[2] 수집된 트렌드 수: {N}")

        trends = []

        for rank, elem in enumerate(elements, start=1):
            title = elem.find_element(
                By.CSS_SELECTOR, "td:nth-child(2) div.mZ3RIc"
            ).text.strip()

            score = trend_score(rank, N)

            trends.append({
                "rank": rank,
                "title": title,
                "trend_score": score
            })

        # 정상 수집 시에만 ES 저장
        doc = {
            "collected_at": datetime.now(KST).isoformat(timespec="seconds"),
            "trends": trends
        }

        try:
            es.index(index=INDEX_NAME, document=doc)
            print("ES 저장 완료")

        except Exception:

            print("ES 저장 실패!!!!!!!")
        return {t["title"]: t["trend_score"] for t in trends}

    finally:
        driver.quit()

# 단독 실행용
if __name__ == "__main__":
    crawl_trends()
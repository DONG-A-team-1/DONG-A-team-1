from fastapi import FastAPI
from urllib.parse import urlparse, urlunparse
from datetime import datetime, timezone, timedelta
import httpx
from bs4 import BeautifulSoup
from logger import Logger
from typing import List, Dict, Any
app = FastAPI()
logger = Logger().get_logger(__name__)
# base_url = "<https://www.hani.co.kr/arti>"

KST = timezone(timedelta(hours=+9))
now_kst = datetime.now(KST).strftime("%Y%m%d%H%M%S")
HEADERS = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9"}
# 웹에서 브라우저인 척 하고 긁어오기 위해서 넣은 구문
# 브라우저처럼 보이게 해서 정상 HTML 받기 위해서
# User Agent가 비어있거나 기본 https값이면 에러

def force_https(url: str) -> str:
    if not url:
        return url

    parsed = urlparse(url)

    # http → https로 변경
    if parsed.scheme == "http":
        parsed = parsed._replace(scheme="https")
        return urlunparse(parsed)

    # 스킴이 없고 //로 시작하는 경우: 프로토콜 상대 URL
    if parsed.scheme == "" and url.startswith("//"):
        return "https:" + url

    # 이미 https 이거나, 다른 스킴이면 그대로 반환
    return url

async def crawl_hani(bigkinds_data: List[Dict[str,Any]]):  # 뷰티풀 숩으로 가져오기

    domain="hani"
    article_list = []

    async with httpx.AsyncClient(timeout=10.0, headers=HEADERS) as client:  # 기본 최신기사 링크 가져와서 크롤링
        for data in bigkinds_data:
            url = data["url"]
            url = force_https(url) # 아예 원본 기사 하나 가져옴
            res = await client.get(url)
            soup = BeautifulSoup(res.text, "html.parser")
            logger.info(f"crawling {url}")

            # 기사 ID 빅카인즈 식별키로 바꿈
            article_id = url.split("/")[-1].replace(".html", "")
            # if not article_id:
            #     continue
            art_id = f"{domain}_{article_id}"

            # 제목 art_name
            title_tag = soup.select_one('h3.ArticleDetailView_title__9kRU_')
            if not title_tag:
                logger.info("제목 태그 없음")
                continue  # 제목 태그 없으면 건너뛰기
            art_name = title_tag.get_text(strip=True)

            # 본문
            paragraphs = soup.select('div.article-text p.text')
            if not paragraphs:
                logger.info("본문 없음 ")
                continue

            art_content = " ".join(p.get_text(strip=True) for p in paragraphs[:-1])
            if len(art_content) < 50:  # 너무 짧으면 영상 기사 등으로 판단하여 제외
                logger.info("영상 기사일 가능성")
                continue

            # 날짜 art_date 빅카인즈
            article_date = data["upload_date"]

            # 대표 이미지 저장
            image = soup.select_one('picture img')
            art_img = image.get('src')

            # 기자 이름 빅카인즈
            article_write = data.get("reporter", "기자 미상")

            # 리스트에 저장
            article_list.append({
                'article_id': art_id,
                'article_name': art_name,
                'article_content': art_content,
                'article_date': article_date,
                'article_img': art_img,
                'article_url': url,
                'article_write': article_write,
                'collected_at': now_kst,
                'bigkinds_meta': data
            })

        print(f"한겨레 {len(article_list)} 크롤링 완료")
    return article_list
# 맨 앞에 기사 하나 잘 들어오는지 확인하기


from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
import httpx
from bs4 import BeautifulSoup
from logger import Logger
from fastapi import FastAPI

logger = Logger().get_logger(__name__)
app = FastAPI()
KST = timezone(timedelta(hours=+9))
now_kst = datetime.now(KST).strftime("%Y%m%d%H%M%S")
HEADERS = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9"}
# 웹에서 브라우저인 척 하고 긁어오기 위해서 넣은 구문
# 브라우저처럼 보이게 해서 정상 HTML 받기 위해서
# User Agent가 비어있거나 기본 https값이면 에러

async def crawl_kmib(bigkinds_data: List[Dict[str,Any]]):

    domain = "kmib"
    article_list = []

    async with httpx.AsyncClient(timeout=10.0, headers=HEADERS) as client:
        for data in bigkinds_data:
            url = data["url"]
            res = await client.get(url)
            soup = BeautifulSoup(res.content, "html.parser")

            # # 기사 ID 만들기 빅카인즈 고유 식별키로 바꿈
            # article_id = url.split("arcid=")[1].split("&")[0]
            # if not article_id:
            #     continue
            # art_id = f"{domain}_{article_id}"

            # 제목
            art_title =soup.select_one("#article_header h1")

            if not art_title:
                logger.info("제목 태그 없음")
                continue
            art_name = art_title.get_text(strip=True)

            # 본문
            body = soup.select_one(
                "div.article_content #articleBody"
            )
            if not body:
                logger.info("본문 없음 → 제외")
                continue
            for remove in body.select("div.article_recommend"):
                remove.decompose()

            text_raw = body.get_text("\\n", strip=True)
            art_content = [line.strip() for line in text_raw.split("\\n")]
            print(art_content)

            # 날짜
            article_date = data["upload_date"]
            print(article_date)
            # 이미지
            image = soup.select_one('#articleBody > div.article_body_img > figure > img')
            art_img = image.get('src') if image else None
            print(art_img)
            # 기자 이름
            article_writer = data.get('reporter')
            if not article_writer:
                continue
            print(article_writer)
            # 기사 저장
            article_list.append({
                # 'article_id': art_id,
                'article_name': art_name,
                'article_content': art_content,
                'article_date': article_date,
                'article_img': art_img,
                'article_url':url,
                'article_wrtier': article_writer,
                'collected_at': now_kst
            })
        print(f"국민일보 {len(article_list)} 크롤링 완료")
    return article_list


from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
import httpx
from bs4 import BeautifulSoup
from util.logger import Logger
from util.elastic import es

logger = Logger().get_logger(__name__)

KST = timezone(timedelta(hours=+9))
now_kst = datetime.now(KST).strftime("%Y%m%d%H%M%S")
HEADERS = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9"}
# 웹에서 브라우저인 척 하고 긁어오기 위해서 넣은 구문
# 브라우저처럼 보이게 해서 정상 HTML 받기 위해서
# User Agent가 비어있거나 기본 https값이면 에러

async def kmib_crawl(bigkinds_data: List[Dict[str,Any]]):
    id_list = [data["news_id"] for data in bigkinds_data]
    url_list = [data["url"] for data in bigkinds_data]
    domain = "kmib"
    article_list = []

    async with httpx.AsyncClient(timeout=10.0, headers=HEADERS) as client:
        for news_id, url in zip(id_list, url_list):
            res = await client.get(url)
            soup = BeautifulSoup(res.content, "html.parser")

            article_title =soup.select_one("#article_header h1")

            if not article_title:
                logger.info("제목 태그 없음")
                continue
            article_title = article_title.get_text(strip=True)

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
            article_content = [line.strip() for line in text_raw.split("\\n")]


            # 이미지
            image = soup.select_one('#articleBody > div.article_body_img > figure > img')
            article_img = image.get('src') if image else None


            es.update(
                index="article_data",
                id=news_id,
                doc={
                    "article_img": article_img,
                }
            )

            article_raw ={
                "article_id": news_id,
                "article_title": article_title,
                "article_content": article_content,
            }

            es.index(index="article_raw", id=news_id, document=article_raw)

    return article_list

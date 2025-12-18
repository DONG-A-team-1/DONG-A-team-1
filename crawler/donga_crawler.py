import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone

from util.logger import Logger
logger = Logger().get_logger(__name__)

from util.elastic import es

KST = timezone(timedelta(hours=9))
now_kst = datetime.now(KST).strftime("%Y%m%d_%H%M%S")

async def donga_crawl(bigkinds_data):
    print(f"구동시작:{now_kst}")
    id_list = [data["news_id"] for data in bigkinds_data]
    url_list = [data["url"] for data in bigkinds_data] #빅카인즈에서 받아온 데이터의 url 부분만 리스트로 변경하여 준비합니다

    article_list = []

    async with httpx.AsyncClient() as client:
        for news_id, url in zip(id_list, url_list):

            resp = await client.get(url)
            soup = BeautifulSoup(resp.text, "html.parser")


            title = soup.select_one('#contents > header > div > section > h1')
            article_title = title.get_text(strip=True) if title else None

            content = soup.select_one(
                '#contents > div.view_body > div > div.main_view > section.news_view')
            article_content = content.get_text(strip=True) if content else None

            # article_img = soup.select_one("#contents > div.view_body > div > div.main_view > section.news_view > figure > div > img")["src"]

            img = soup.select_one(
                "#contents > div.view_body > div > div.main_view > section.news_view > figure > div > img")
            article_img = img.get("src") if img else None


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
            

    print(f"{len(article_list)}개 수집 완료")
    print(f"동아일보 {now_kst}")





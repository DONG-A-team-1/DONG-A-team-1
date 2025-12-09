import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

KST = timezone(timedelta(hours=9))
now_kst = datetime.now(KST).strftime("%Y%m%d_%H%M%S")

async def donga_crawl(bigkinds_data):
    print(f"구동시작:{now_kst}")
    url_list = [data["url"] for data in bigkinds_data] #빅카인즈에서 받아온 데이터의 url 부분만 리스트로 변경하여 준비합니다

    domian = "donga"
    article_list = []

    async with httpx.AsyncClient() as client:
        for url in url_list:
            resp = await client.get(url)
            soup = BeautifulSoup(resp.text, "html.parser")

            dong_a_id = urlparse(url).path.strip("/").split("/")[-2]

            article_id = f"{domian}_{dong_a_id}"

            name = soup.select_one('#contents > header > div > section > h1')
            article_name = name.get_text(strip=True) if name else None

            content = soup.select_one(
                '#contents > div.view_body > div > div.main_view > section.news_view')
            article_content = content.get_text(strip=True) if content else None

            date = soup.select_one(
                '#contents > header > div > section > ul > li:nth-child(2) > button > span:nth-child(1)')
            article_date =  date.get_text(strip=True) if date else None

            write = soup.select_one('#contents > header > div > section > ul > li:nth-child(1) > strong')
            article_write =  date.get_text(strip=True) if write else None

            article_img = soup.select_one("#contents > div.view_body > div > div.main_view > section.news_view > figure > div > img")["src"]

            article_url = url
            collected_at = now_kst

            article_list.append({
                "article_id": article_id,
                "article_name": article_name,
                "article_content": article_content,
                "article_date": article_date,
                "article_img": article_img,
                "article_url": article_url,
                "article_write": article_write,
                "collected_at": collected_at,
            })

    print(article_list[0])
    print(f"동아일보 {now_kst}")





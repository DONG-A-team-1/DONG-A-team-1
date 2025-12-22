import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from util.elastic import es
from util.logger import Logger, build_error_doc
import inspect
import os

timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)

filename = os.path.basename(__file__)
funcname = inspect.currentframe().f_back.f_code.co_name

logger_name = f"{filename}:{funcname}"
now_kst_iso = datetime.now(timezone(timedelta(hours=9))).isoformat()

logger = Logger().get_logger(__name__)


KST = timezone(timedelta(hours=9))
now_kst = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
domain = "naeil"

async def naeil_crawl(bigkinds_data):
    print(f"구동시작:{now_kst}")
    id_list = [data["article_id"] for data in bigkinds_data]
    url_list = [data["url"] for data in bigkinds_data] #빅카인즈에서 받아온 데이터의 url 부분만 리스트로 변경하여 준비합니다

    article_list = []

    async with httpx.AsyncClient(timeout = timeout) as client:
        for article_id, url in zip(id_list, url_list):
            resp = await client.get(url)
            soup = BeautifulSoup(resp.text, "html.parser")


            title = soup.select_one('#container > section > div > header > h1')
            article_title = title.get_text(strip=True) if title else None

            content = soup.select('div.article-view p')
            if content:
                # 리스트 안의 각 p 태그에서 텍스트를 뽑아와서 줄바꿈(\n)으로 합칩니다.
                article_content = "\n".join([p.get_text(strip=True) for p in content if p.get_text(strip=True)])
            else:
                article_content = None

            # article_img = soup.select_one("#contents > div.view_body > div > div.main_view > section.news_view > figure > div > img")["src"]

            img = soup.select_one(
                "figure.article-img img.fade")
            article_img = img.get("src") if img else None


            es.update(
                index="article_data",
                id=article_id,
                doc={
                    "article_img": article_img,
                }
            )

            article_raw ={
                "article_id": article_id,
                "article_title": article_title,
                "article_content": article_content,
                "collected_at": now_kst_iso
            }

            error_doc = build_error_doc(
                message=f"{article_id} 결측치 존재, url: {url}"
            )

            null_count = 0
            for v in article_raw.values():
                if v in (None, "", []):
                    null_count += 1
            if null_count >= 1:
                    es.create(index="error_log", id=f"{now_kst_iso}_{article_id}", document=error_doc)
            else:
                es.index(index="article_raw", id=article_id, document=article_raw)
            article_list.append(article_raw)
            # print(article_raw)

    print(f"{len(article_list)}개 수집 완료")
    print(f"내일신문 {now_kst}")





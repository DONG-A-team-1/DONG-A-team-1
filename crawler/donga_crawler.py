import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from util.elastic import es
from util.logger import Logger
from util.elastic_templates import build_error_doc
import inspect
import os

filename = os.path.basename(__file__)
funcname = inspect.currentframe().f_back.f_code.co_name

logger_name = f"{filename}:{funcname}"
now_kst_iso = datetime.now(timezone(timedelta(hours=9))).isoformat()

logger = Logger().get_logger(__name__)


KST = timezone(timedelta(hours=9))
now_kst = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
domain = "donga"

async def donga_crawl(bigkinds_data):
    print(f"구동시작:{now_kst}")
    id_list = [data["article_id"] for data in bigkinds_data]
    url_list = [data["url"] for data in bigkinds_data] #빅카인즈에서 받아온 데이터의 url 부분만 리스트로 변경하여 준비합니다

    error_list = []
    empty_articles = []
    async with httpx.AsyncClient() as client:
        for article_id, url in zip(id_list, url_list):
            try:
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

            except Exception as e:
                error_list.append({
                    "error_url": url,
                    "error_type": type(e).__name__,
                    "error_message": f"{str(e)}"
                })
                continue

            null_count = sum(1 for v in article_raw.values() if v in (None, "", []))
            if null_count == 0:
                es.index(index="article_raw", id=article_id, document=article_raw)
            else:
                empty_articles.append({
                    "article_id": article_id
                })
                es.delete(index="article_data", id=article_id)

        # 에러 로그 업로드
        if len(error_list) > 0:
            error_doc = build_error_doc(
                message=f"{len(error_list)}개 에러 발생",
                samples=error_list
            )
            es.index(index="error_log", document=error_doc)

        if len(empty_articles) > 0:
            es.index(
                index="error_log",
                document=build_error_doc(
                    message=f"{len(empty_articles)}개 결측치 발생",
                    samples=empty_articles
                )
            )

    empty_ids = {x["article_id"] for x in empty_articles}
    result = list(set(id_list) - empty_ids)
    print(f"==== 동아일보 상세 크롤링 완료: {len(result)}개 성공====")
    return result 





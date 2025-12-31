import httpx
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
from bs4 import BeautifulSoup

from util.logger import Logger
from util.elastic_templates import build_error_doc
from util.elastic import es

logger = Logger().get_logger(__name__)

now_kst_iso = datetime.now(timezone(timedelta(hours=9))).isoformat()
KST = timezone(timedelta(hours=+9))

now_kst = datetime.now(KST).strftime("%Y%m%d%H%M%S")
HEADERS = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9"}
# 웹에서 브라우저인 척 하고 긁어오기 위해서 넣은 구문
# 브라우저처럼 보이게 해서 정상 HTML 받기 위해서
# User Agent가 비어있거나 기본 https값이면 에러

async def kmib_crawl(bigkinds_data: List[Dict[str,Any]]):
    id_list = [data["article_id"] for data in bigkinds_data]
    url_list = [data["url"] for data in bigkinds_data]

    article_list = []
    error_list = []
    empty_articles = []
    async with httpx.AsyncClient(timeout=10.0, headers=HEADERS) as client:
        for article_id, url in zip(id_list, url_list):
            if not url:
                continue
            try:
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
                    id=article_id,
                    body={"doc": {"article_img": article_img}}
                )

                article_raw = {
                    "article_id": article_id,
                    "article_title": article_title,
                    "article_content": article_content,
                    "collected_at": now_kst_iso
                }

            except Exception as e:
                error_list.append({
                    "error_url":url,
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

    #에러 로그 업로드
    if len(error_list) > 0 :
        error_doc = build_error_doc(
            message=f"{len(error_list)}개 에러 발생",
            samples=error_list
        )
        es.index(index="error_log", document=error_doc)

    if len(empty_articles) >0:
        es.index(
            index="error_log",
            document=build_error_doc(
                message=f"{len(empty_articles)}개 결측치 발생",
                samples=empty_articles
            )
        )
    empty_ids = {x["article_id"] for x in empty_articles}
    result = list(set(id_list) - empty_ids)
    print(f"==== 조선일보 상세 크롤링 완료: {len(result)}개 성공====")
    return result

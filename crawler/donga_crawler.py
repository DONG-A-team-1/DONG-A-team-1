import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from util.elastic import es
from util.logger import Logger
from util.elastic_templates import build_error_doc
import os

logger = Logger().get_logger(__name__)

KST = timezone(timedelta(hours=9))
now_kst_iso = datetime.now(KST).isoformat()
now_run_id = datetime.now(KST).strftime("%Y%m%d_%H")  # yyyymmdd_hh

domain = "donga"

async def donga_crawl(bigkinds_data):
    print(f"구동시작: {now_run_id}")

    id_list = [data["article_id"] for data in bigkinds_data]
    url_list = [data["url"] for data in bigkinds_data]

    error_list = []
    empty_articles = []

    async with httpx.AsyncClient(timeout=10.0) as client:
        for article_id, url in zip(id_list, url_list):
            if not url:
                error_list.append({
                    "article_id": article_id,
                    "error_url": url,
                    "error_type": "EmptyURL",
                    "error_message": "url is empty"
                })
                continue

            try:
                resp = await client.get(url)
                resp.raise_for_status()

                soup = BeautifulSoup(resp.text, "html.parser")

                title = soup.select_one('#contents > header > div > section > h1')
                article_title = title.get_text(strip=True) if title else None

                content = soup.select_one(
                    '#contents > div.view_body > div > div.main_view > section.news_view'
                )
                article_content = content.get_text(strip=True) if content else None

                img = soup.select_one(
                    "#contents > div.view_body > div > div.main_view > section.news_view > figure > div > img"
                )
                article_img = img.get("src") if img else None

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
                    "article_id": article_id,
                    "error_url": url,
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                })
                continue

            null_count = sum(1 for v in article_raw.values() if v in (None, "", []))
            if null_count == 0:
                es.index(index="article_raw", id=article_id, document=article_raw)
            else:
                empty_articles.append({
                    "article_id": article_id,
                    "reason": "null_fields_in_article_raw",
                    "null_count": null_count
                })
                es.delete(index="article_data", id=article_id)

    # ✅ 요약 로그 1건만 저장 (에러 or 결측이 하나라도 있으면)
    if error_list or empty_articles:
        samples = []
        samples.extend(error_list[:10])
        if len(samples) < 10:
            samples.extend(empty_articles[: (10 - len(samples))])

        doc = build_error_doc(
            message=f"동아일보 상세 크롤링 요약: 에러 {len(error_list)}건, 결측 {len(empty_articles)}건",

            service_name="crawler",
            service_environment=os.getenv("APP_ENV", "dev"),

            pipeline_run_id=now_run_id,
            pipeline_job="donga_crawl",
            pipeline_step="article_content",

            event_severity=3,
            event_outcome="failure",

            metrics={
                "error_count": len(error_list),
                "empty_count": len(empty_articles),
                "total_targets": len(id_list),
                "success_count": len(id_list) - len(error_list) - len(empty_articles),
            },

            context={
                "press": "동아일보",
                "domain": domain,
                "total_targets": len(id_list),
            },

            samples=samples,
            tags=["crawler", "donga", "detail", "summary"],
        )
        es.index(index="error_log", document=doc)

    empty_ids = {x["article_id"] for x in empty_articles}
    error_ids = {x["article_id"] for x in error_list}
    result = list(set(id_list) - empty_ids - error_ids)

    print(f"==== 동아일보 상세 크롤링 완료: {len(result)}개 성공====")
    return result

import httpx
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
from bs4 import BeautifulSoup

from util.logger import Logger
from util.elastic_templates import build_error_doc
from util.elastic import es

logger = Logger().get_logger(__name__)

KST = timezone(timedelta(hours=+9))
now_kst_iso = datetime.now(KST).isoformat()
now_run_id = datetime.now(KST).strftime("%Y%m%d_%H")  # yyyymmdd_hh

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9"
}

async def kmib_crawl(bigkinds_data: List[Dict[str, Any]]):
    id_list = [data["article_id"] for data in bigkinds_data]
    url_list = [data["url"] for data in bigkinds_data]

    error_list: List[Dict[str, Any]] = []
    empty_articles: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=10.0, headers=HEADERS) as client:
        for article_id, url in zip(id_list, url_list):
            if not url:
                empty_articles.append(article_id)
                pass
            try:
                res = await client.get(url)
                res.raise_for_status()

                soup = BeautifulSoup(res.content, "html.parser")

                title_el = soup.select_one("#article_header h1")
                article_title = title_el.get_text(strip=True) if title_el else None

                # 본문
                body = soup.select_one("div.article_content #articleBody")
                if body:
                    for remove in body.select("div.article_recommend"):
                        remove.decompose()

                    text_raw = body.get_text("\n", strip=True)
                    article_content = [line.strip() for line in text_raw.split("\n")]
                else:
                    article_content = None

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
        samples: List[Dict[str, Any]] = []
        samples.extend(error_list[:10])
        if len(samples) < 10:
            samples.extend(empty_articles[: (10 - len(samples))])

        doc = build_error_doc(
            message=f"국민일보(KMIB) 상세 크롤링 요약: 에러 {len(error_list)}건, 결측 {len(empty_articles)}건",

            service_name="crawler",
            pipeline_run_id=now_run_id,
            pipeline_job="kmib_crawl",
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
                "press": "국민일보",
                "total_targets": len(id_list),
            },

            samples=samples,
            tags=["crawler", "kmib", "detail", "summary"],
        )
        es.index(index="error_log", document=doc)

    empty_ids = {x["article_id"] for x in empty_articles}
    error_ids = {x["article_id"] for x in error_list}
    result = list(set(id_list) - empty_ids - error_ids)
    print(f"==== 국민일보 상세 크롤링 완료: {len(result)}개 성공====")
    return result

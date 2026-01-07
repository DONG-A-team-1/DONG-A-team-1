import httpx
import os
import inspect

from bs4 import BeautifulSoup
from util.logger import Logger
from util.elastic_templates import build_error_doc
from typing import List, Dict, Any
from util.elastic import es
from urllib.parse import urlparse, urlunparse
from datetime import datetime, timezone, timedelta

logger = Logger().get_logger(__name__)

filename = os.path.basename(__file__)
funcname = inspect.currentframe().f_back.f_code.co_name
logger_name = f"{filename}:{funcname}"

KST = timezone(timedelta(hours=+9))
now_kst_iso = datetime.now(KST).isoformat()
now_run_id = datetime.now(KST).strftime("%Y%m%d_%H")  # yyyymmdd_hh

HEADERS = {
  "User-Agent": "...Chrome...",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
  "Cache-Control": "no-cache",
  "Pragma": "no-cache",
  "Referer": "https://www.hani.co.kr/"
}

def force_https(url: str) -> str:
    if not url:
        return url

    parsed = urlparse(url)

    if parsed.scheme == "http":
        parsed = parsed._replace(scheme="https")
        return urlunparse(parsed)

    if parsed.scheme == "" and url.startswith("//"):
        return "https:" + url

    return url

async def hani_crawl(bigkinds_data: List[Dict[str, Any]]):
    id_list = [data["article_id"] for data in bigkinds_data]
    url_list = [data["url"] for data in bigkinds_data]

    error_list: List[Dict[str, Any]] = []
    empty_articles: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(
        timeout=10.0, headers=HEADERS, http2=True, follow_redirects=True
    ) as client:
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
                url = force_https(url)
                res = await client.get(url)
                res.raise_for_status()

                soup = BeautifulSoup(res.text, "html.parser")
                logger.info(f"crawling {url}")

                # 제목
                title_tag = soup.select_one('h3.ArticleDetailView_title__9kRU_')
                article_title = title_tag.get_text(strip=True) if title_tag else None

                # 본문
                paragraphs = soup.select('div.article-text p.text')
                if paragraphs:
                    article_content = " ".join(p.get_text(strip=True) for p in paragraphs[:-1])
                else:
                    article_content = None

                # 대표 이미지
                image = soup.select_one('picture img')
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
            message=f"한겨레 상세 크롤링 요약: 에러 {len(error_list)}건, 결측 {len(empty_articles)}건",

            service_name="crawler",
            service_environment=os.getenv("APP_ENV", "dev"),

            pipeline_run_id=now_run_id,
            pipeline_job="hani_crawl",
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
                "press": "한겨레",
                "total_targets": len(id_list),
            },

            samples=samples,
            tags=["crawler", "hani", "detail", "summary"],
        )
        es.index(index="error_log", document=doc)

    empty_ids = {x["article_id"] for x in empty_articles}
    error_ids = {x["article_id"] for x in error_list}
    result = list(set(id_list) - empty_ids - error_ids)

    print(f"==== 한겨레 상세 크롤링 완료: {len(result)}개 성공====")
    return result

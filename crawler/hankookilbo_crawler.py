import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import asyncio
from typing import List, Dict, Any
import os

from util.elastic_templates import build_error_doc
from util.elastic import es

KST = timezone(timedelta(hours=9))
BASE_URL = "https://www.hankookilbo.com/"

now_kst_iso = datetime.now(KST).isoformat()
now_run_id = datetime.now(KST).strftime("%Y%m%d_%H")  # yyyymmdd_hh


async def hankookilbo_crawl(bigkinds_data: List[Dict[str, Any]]):
    """
    빅카인즈에서 받은 URL 리스트로 한국일보 상세 기사 크롤링
    - 성공: article_raw 인덱싱
    - 결측: article_data 삭제
    - 에러/결측: 마지막에 요약 로그 1건만 error_log에 적재
    """
    print(f"한국일보 상세 크롤링 구동 시작: {now_run_id}")

    id_list = [data["article_id"] for data in bigkinds_data]
    url_list = [data["url"] for data in bigkinds_data]

    error_list: List[Dict[str, Any]] = []
    empty_articles: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=10.0) as client:
        for article_id, orginal_url in zip(id_list, url_list):
            if not orginal_url:
                error_list.append({
                    "article_id": article_id,
                    "error_url": orginal_url,
                    "error_type": "EmptyURL",
                    "error_message": "url is empty"
                })
                continue

            url = orginal_url  # 한국일보는 별도 URL 변환 로직 없음

            try:
                await asyncio.sleep(0.5)

                resp = await client.get(url)
                resp.raise_for_status()

                soup = BeautifulSoup(resp.text, "html.parser")

                # --- 본문 ---
                content_div = soup.select_one("div.end-body div.col-main")
                if content_div:
                    p_tags = content_div.select("p")
                    article_content = "\n".join(
                        [p.get_text(strip=True) for p in p_tags if p.get_text(strip=True)]
                    )
                else:
                    article_content = None

                # --- 제목 ---
                title_el = soup.select_one("div.end-top div.col-main h1.title")
                article_title = title_el.text.strip() if title_el else None

                # --- 이미지 ---
                news_img = soup.select_one("div.img-box img")
                article_img = news_img["src"] if news_img and news_img.get("src") else None

                # base64 방지 (src가 너무 길면 제거)
                if article_img and len(article_img) > 500:
                    article_img = None

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
            message=f"한국일보 상세 크롤링 요약: 에러 {len(error_list)}건, 결측 {len(empty_articles)}건",

            service_name="crawler",
            service_environment=os.getenv("APP_ENV", "dev"),

            pipeline_run_id=now_run_id,
            pipeline_job="hankookilbo_crawl",
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
                "press": "한국일보",
                "base_url": BASE_URL,
                "total_targets": len(id_list),
            },

            samples=samples,
            tags=["crawler", "hankookilbo", "detail", "summary"],
        )
        es.index(index="error_log", document=doc)

    empty_ids = {x["article_id"] for x in empty_articles}
    error_ids = {x["article_id"] for x in error_list}
    result = list(set(id_list) - empty_ids - error_ids)

    print(f"==== 한국일보 상세 크롤링 완료: {len(result)}개 성공====")
    return result

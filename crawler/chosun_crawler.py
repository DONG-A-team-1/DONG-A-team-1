import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from util.elastic import es
from util.elastic_templates import build_error_doc

KST = timezone(timedelta(hours=9))
now_kst_iso = datetime.now(KST).isoformat()
now_run_id = datetime.now(KST).strftime("%Y%m%d_%H")  # yyyymmdd_hh


async def chosun_crawl(press_results: List[Dict[str, Any]]):
    print(f"조선일보 상세 크롤링 시작 (대상: {len(press_results)}건)")
    error_list: List[Dict[str, Any]] = []
    empty_articles: List[Dict[str, Any]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            locale="ko-KR",
            viewport={"width": 1280, "height": 720},
        )

        for data in press_results:
            article_id = data.get("article_id")
            url = data.get("url")

            if not url:
                error_list.append({
                    "article_id": article_id,
                    "error_url": url,
                    "error_type": "EmptyURL",
                    "error_message": "url is empty"
                })
                continue

            try:
                # 속도 조절 (차단 방지)
                await asyncio.sleep(1.5)

                page = await context.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=25000)

                html = await page.content()
                await page.close()

                soup = BeautifulSoup(html, "lxml")

                # 본문
                p_tags = soup.select("section.article-body p")
                full_content = " ".join(
                    [p.get_text(strip=True) for p in p_tags if p]
                ).strip() or None

                # 제목
                title_tag = soup.select_one("h1.article-header__headline span")
                article_title = title_tag.get_text(strip=True) if title_tag else data.get("title", None)

                # 이미지
                image_tag = soup.select_one("section.article-body div.lazyload-wrapper img")
                article_img = image_tag.get("src") if image_tag and image_tag.get("src") else None

                es.update(
                    index="article_data",
                    id=article_id,
                    body={"doc": {"article_img": article_img}}
                )

                article_raw = {
                    "article_id": article_id,
                    "article_title": article_title,
                    "article_content": full_content,
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

        await context.close()
        await browser.close()

    # ✅ 요약 로그 1건만 저장 (에러 or 결측이 하나라도 있으면)
    if error_list or empty_articles:
        samples: List[Dict[str, Any]] = []
        samples.extend(error_list[:10])
        if len(samples) < 10:
            samples.extend(empty_articles[: (10 - len(samples))])

        doc = build_error_doc(
            message=f"조선일보 상세 크롤링 요약: 에러 {len(error_list)}건, 결측 {len(empty_articles)}건",

            service_name="crawler",
            service_environment=os.getenv("APP_ENV", "dev"),

            pipeline_run_id=now_run_id,
            pipeline_job="chosun_crawl",
            pipeline_step="article_content",

            event_severity=3,
            event_outcome="failure",

            metrics={
                "error_count": len(error_list),
                "empty_count": len(empty_articles),
                "total_targets": len(press_results),
                "success_count": len(press_results) - len(error_list) - len(empty_articles),
            },

            context={
                "press": "조선일보",
                "total_targets": len(press_results),
                "runtime": "playwright",
            },

            samples=samples,
            tags=["crawler", "chosun", "detail", "summary"],
        )
        es.index(index="error_log", document=doc)

    empty_ids = {x["article_id"] for x in empty_articles}
    error_ids = {x["article_id"] for x in error_list}
    id_list = [data.get("article_id") for data in press_results if data.get("article_id")]
    result = list(set(id_list) - empty_ids - error_ids)

    print(f"==== 조선일보 상세 크롤링 완료: {len(result)}개 성공====")
    return result

import asyncio

from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
from playwright.async_api import async_playwright

from util.elastic import es
from util.elastic_templates import build_error_doc

# 로깅을 위한 설정
KST = timezone(timedelta(hours=9))
now_kst_iso = datetime.now(timezone(timedelta(hours=9))).isoformat()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,/ *;q=0.8"
}


async def chosun_crawl(press_results: List[Dict[str, Any]]):
    print(f"조선일보 상세 크롤링 시작 (대상: {len(press_results)}건)")
    error_list = []
    empty_articles =[]

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
                continue
            try:
                # 1. 속도 조절 (조선일보 차단 방지)
                await asyncio.sleep(1.5)
                # 2. Playwright로 페이지 로드
                page = await context.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=25000)

                # 3. 렌더링된 전체 HTML 가져오기
                html = await page.content()
                await page.close()
                soup = BeautifulSoup(html, "lxml")
                # print(soup)
                # 3. 본문 추출 (가장 안정적인 select 방식)
                # section.article-body 내부의 모든 p 태그를 가져와서 합칩니다.

                p_tags = soup.select("section.article-body p")
                full_content = " ".join([p.get_text(strip=True) for p in p_tags if p]).strip()

                # # 4. 제목 및 이미지 추출
                title_tag = soup.select_one("h1.article-header__headline span")
                article_title = title_tag.get_text(strip=True) if title_tag else data.get("title", "제목 없음")

                image_tag = soup.select_one("section.article-body div.lazyload-wrapper img")
                article_img = image_tag.get("src") if image_tag and image_tag.get("src") else None

                es.update(
                    index="article_data",
                    id=article_id,
                    doc={
                        "article_img": article_img,
                    }
                )

                # 6. Elasticsearch 저장
                # article_raw: 전처리 전 원본 데이터 저장
                article_raw = {
                    "article_id": article_id,
                    "article_title": article_title,
                    "article_content": full_content,
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
    id_list = [data["article_id"] for data in press_results]
    result = list(set(id_list) - empty_ids)
    print(f"==== 조선일보 상세 크롤링 완료: {len(result)}개 성공====")
    return result
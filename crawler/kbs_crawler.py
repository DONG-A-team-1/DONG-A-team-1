import httpx
import asyncio
import os

from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

from util.logger import Logger
from util.elastic_templates import build_error_doc
from util.elastic import es

logger = Logger().get_logger(__name__)

KST = timezone(timedelta(hours=9))
BASE_URL = "https://news.kbs.co.kr"

async def kbs_crawl(bigkinds_data: List[Dict[str, Any]]):
    """
    빅카인즈에서 받은 URL 리스트를 사용하여 KBS 상세 기사를 비동기적으로 크롤링합니다.
    - 리다이렉션(302) 회피를 위해 PC 버전 URL로 변경
    - 성공: article_raw 인덱싱
    - 결측: article_data 삭제
    - 에러/결측: 마지막에 요약 로그 1건만 error_log에 적재
    """
    now_kst_iso = datetime.now(KST).isoformat()
    now_run_id = datetime.now(KST).strftime("%Y%m%d_%H")  # yyyymmdd_hh
    print(f"KBS 상세 크롤링 구동 시작: {now_run_id}")

    id_list = [data["article_id"] for data in bigkinds_data]
    url_list = [data["url"] for data in bigkinds_data]

    error_list: List[Dict[str, Any]] = []
    empty_articles: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=10.0) as client:
        for article_id, orginal_url in zip(id_list, url_list):
            # PC 버전 URL로 경로 강제 변경
            if "/news/view.do" in orginal_url:
                url = orginal_url.replace("/news/view.do", "/news/pc/view/view.do")
            else:
                url = orginal_url

            try:
                await asyncio.sleep(0.5)

                resp = await client.get(url)
                resp.raise_for_status()

                soup = BeautifulSoup(resp.text, "html.parser")

                # 본문
                content = soup.select_one("div#cont_newstext")
                article_content = content.get_text(strip=True) if content else None

                # 제목
                title_el = soup.select_one("div.view-headline h4")
                article_title = title_el.text.strip() if title_el else None

                # 이미지
                news_img = soup.select_one("div#element-image img")
                article_img = news_img["src"] if news_img and news_img.get("src") else None

                # article_data 업데이트 (img)
                es.update(
                    index="article_data",
                    id=article_id,
                    doc={"article_img": article_img}
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

            # 결측 체크
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

    if error_list or empty_articles:
        samples: List[Dict[str, Any]] = []
        # 샘플은 최대 10개까지만 들어가도록 build_error_doc 내부에서 slice 처리되지만,
        # 여기서도 “에러 우선”으로 10개 맞춰 구성
        samples.extend(error_list[:10])
        if len(samples) < 10:
            samples.extend(empty_articles[: (10 - len(samples))])

        doc = build_error_doc(
            message=f"KBS 상세 크롤링 요약: 에러 {len(error_list)}건, 결측 {len(empty_articles)}건",

            service_name="crawler",
            service_environment=os.getenv("APP_ENV", "dev"),

            pipeline_run_id=now_run_id,
            pipeline_job="kbs_crawl",
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
                "press": "KBS",
                "base_url": BASE_URL,
                "total_targets": len(id_list),
            },

            samples=samples,
            tags=["crawler", "kbs", "detail", "summary"],
        )
        es.index(index="error_log", document=doc)

    empty_ids = {x["article_id"] for x in empty_articles}
    # 성공 기준: 결측 삭제된 것 제외 (에러는 시도 실패니까 제외)
    result = list(set(id_list) - empty_ids - {x["article_id"] for x in error_list})
    print(f"==== KBS 상세 크롤링 완료: {len(result)}개 성공 ====")
    return result

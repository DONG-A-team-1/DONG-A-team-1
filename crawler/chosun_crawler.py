import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import asyncio
from typing import List, Dict, Any
from util.elastic import es
import os
import inspect

# 로깅을 위한 설정
filename = os.path.basename(__file__)
KST = timezone(timedelta(hours=9))
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,/ *;q=0.8"
}


async def chosun_crawl(press_results: List[Dict[str, Any]]):
    """
    main.py에서 넘겨받은 press_results(기사 목록)를 바탕으로
    조선일보 상세 페이지 본문을 수집합니다.
    """
    funcname = inspect.currentframe().f_code.co_name
    logger_name = f"{filename}:{funcname}"
    now_iso = datetime.now(KST).isoformat()

    print(f"조선일보 상세 크롤링 시작 (대상: {len(press_results)}건)")

    success_list = []

    async with httpx.AsyncClient(timeout=20.0, headers=HEADERS, follow_redirects=True) as client:
        for data in press_results:
            article_id = data.get("article_id")
            url = data.get("url")

            if not url:
                continue

            try:
                # 1. 속도 조절 (조선일보 차단 방지)
                await asyncio.sleep(1.5)

                resp = await client.get(url)
                resp.raise_for_status()

                # 2. 파싱 (lxml 설치되어 있다면 'lxml' 사용 권장, 없으면 'html.parser')
                soup = BeautifulSoup(resp.text, "html.parser")

                # 3. 본문 추출 (가장 안정적인 select 방식)
                # section.article-body 내부의 모든 p 태그를 가져와서 합칩니다.
                p_tags = soup.select("section.article-body p")
                full_content = " ".join([p.get_text(strip=True) for p in p_tags if p]).strip()

                # 본문이 비었을 경우 AMP 페이지 시도
                if not full_content:
                    amp_url = url + "?outputType=amp"
                    amp_resp = await client.get(amp_url)
                    if amp_resp.status_code == 200:
                        amp_soup = BeautifulSoup(amp_resp.text, "html.parser")
                        amp_p = amp_soup.select("section.article-body p") or amp_soup.select("article p")
                        full_content = " ".join([p.get_text(strip=True) for p in amp_p if p]).strip()

                # 4. 제목 및 이미지 추출
                title_tag = soup.select_one("h1.article-header__headline span")
                article_title = title_tag.get_text(strip=True) if title_tag else data.get("title", "제목 없음")

                image_tag = soup.select_one("section.article-body div.lazyload-wrapper img")
                article_img = image_tag.get("src") if image_tag else None

                # 5. 데이터 검증 (본문이 없으면 결측치로 간주)
                if not full_content:
                    error_doc = {
                        "@timestamp": now_iso,
                        "log": {"level": "ERROR", "logger": logger_name},
                        "message": f"본문 수집 실패: {article_id}",
                        "url": url
                    }
                    es.index(index="error_log", id=article_id, document=error_doc)
                    continue

                # 6. Elasticsearch 저장
                # article_raw: 전처리 전 원본 데이터 저장
                article_raw = {
                    "article_id": article_id,
                    "article_title": article_title,
                    "article_content": full_content,
                    "collected_at": now_iso
                }
                es.index(index="article_raw", id=article_id, document=article_raw)

                # article_data: 기존 빅카인즈 데이터에 이미지 경로 업데이트 (upsert)
                es.update(
                    index="article_data",
                    id=article_id,
                    doc={"article_img": article_img},
                    doc_as_upsert=True
                )

                success_list.append(article_id)

            except Exception as e:
                print(f"[조선 오류] {article_id} 처리 실패: {e}")

    print(f"==== 조선일보 상세 크롤링 완료: {len(success_list)}건 성공 ====")
    return success_list
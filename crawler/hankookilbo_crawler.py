import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
import asyncio # 비동기 지연을 위해 추가
from typing import List, Dict, Any
import os
import inspect
from util.elastic_templates import build_error_doc

filename = os.path.basename(__file__)
funcname = inspect.currentframe().f_back.f_code.co_name

logger_name = f"{filename}:{funcname}"
now_kst_iso = datetime.now(timezone(timedelta(hours=9))).isoformat()

KST = timezone(timedelta(hours=9))
now_kst = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
BASE_URL = "https://www.hankookilbo.com/"
from util.elastic import es

async def hankookilbo_crawl(bigkinds_data: List[Dict[str, Any]]):
    """
    빅카인즈에서 받은 URL 리스트를 사용하여 KBS 상세 기사를 비동기적으로 크롤링합니다.
    URL 리다이렉션 오류(302)를 해결하기 위해 URL 경로를 수정합니다.
    """
    print(f"한국일보 상세 크롤링 구동 시작:{now_kst}")

    id_list = [data["article_id"] for data in bigkinds_data]
    url_list = [data["url"] for data in bigkinds_data]

    article_list = []
    error_list = []
    empty_articles = []

    # httpx를 사용하여 비동기 HTTP 요청 처리
    async with httpx.AsyncClient(timeout=10.0) as client:
        for article_id, orginal_url in zip(id_list, url_list):
            # 🚨 리다이렉션 오류(302) 해결 로직: PC 버전 URL로 경로 강제 변경
            # 예: /news/view.do?ncd=...  -> /news/pc/view/view.do?ncd=...
            if "/news/view.do" in orginal_url:
                url = orginal_url.replace("/news/view.do", "/news/pc/view/view.do")
            else:
                url = orginal_url # 이미 올바른 형식일 경우 그대로 사용

            try:
                # 0.5초 비동기 지연 추가 (서버 부하 감소)
                await asyncio.sleep(0.5)

                resp = await client.get(url)
                resp.raise_for_status() # HTTP 오류가 발생하면 예외 발생

                soup = BeautifulSoup(resp.text, "html.parser")

                # --- 기사 본문 추출 ---
                content_div = soup.select_one("div.end-body div.col-main")

                if content_div:
                    # 2. 부모 div 안의 모든 p 태그를 찾음
                    p_tags = content_div.select("p")

                    # 3. 각 p 태그에서 텍스트만 뽑아 줄바꿈(\n)으로 합침
                    # (내용이 없는 빈 p 태그는 제외)
                    article_content = "\n".join([p.get_text(strip=True) for p in p_tags if p.get_text(strip=True)])
                else:
                    article_content = None

                # --- 나머지 정보 추출 ---
                # 'data["newsTitle"]'이 아닌 상세 페이지에서 추출하거나, 안전한 기본값 사용
                article_title = soup.select_one("div.end-top div.col-main h1.title").text.strip() if soup.select_one(
                    "div.end-top div.col-main h1.title") else None

                news_img = soup.select_one("div.img-box img")
                article_img = news_img["src"] if news_img and news_img.get("src") else None
                
                #base64 방지
                if len(article_img) > 500:
                    article_img = None
                    
                es.update(
                    index="article_data",
                    id=article_id,
                    doc={
                        "article_img": article_img
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
    print(f"==== 조선일보 상세 크롤링 완료: {len(result)}개 성공====")
    return result
import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
import asyncio # 비동기 지연을 위해 추가
from typing import List, Dict, Any
import json
import os
import inspect


filename = os.path.basename(__file__)
funcname = inspect.currentframe().f_back.f_code.co_name

logger_name = f"{filename}:{funcname}"
now_kst_iso = datetime.now(timezone(timedelta(hours=9))).isoformat()

KST = timezone(timedelta(hours=9))
now_kst = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
BASE_URL = "https://news.kbs.co.kr"
from util.elastic import es

async def kbs_crawl(bigkinds_data: List[Dict[str, Any]]):
    """
    빅카인즈에서 받은 URL 리스트를 사용하여 KBS 상세 기사를 비동기적으로 크롤링합니다.
    URL 리다이렉션 오류(302)를 해결하기 위해 URL 경로를 수정합니다.
    """
    print(f"KBS 상세 크롤링 구동 시작:{now_kst}")

    id_list = [data["news_id"] for data in bigkinds_data]
    url_list = [data["url"] for data in bigkinds_data]

    domain = "kbs"
    article_list = []

    # httpx를 사용하여 비동기 HTTP 요청 처리
    async with httpx.AsyncClient(timeout=10.0) as client:
        for news_id, orginal_url in zip(id_list, url_list):
            # 🚨 리다이렉션 오류(302) 해결 로직: PC 버전 URL로 경로 강제 변경
            # 예: /news/view.do?ncd=...  -> /news/pc/view/view.do?ncd=...
            if "/news/view.do" in orginal_url:
                url = orginal_url.replace("/news/view.do", "/news/pc/view/view.do")
            else:
                url = url # 이미 올바른 형식일 경우 그대로 사용

            try:
                # 0.5초 비동기 지연 추가 (서버 부하 감소)
                await asyncio.sleep(0.5)

                resp = await client.get(url)
                resp.raise_for_status() # HTTP 오류가 발생하면 예외 발생

                soup = BeautifulSoup(resp.text, "html.parser")


                # --- 기사 본문 추출 ---
                article_content = soup.select_one("div.detail-body")
                content = article_content.get_text(strip=True) if article_content else None

                # --- 나머지 정보 추출 ---
                # 'data["newsTitle"]'이 아닌 상세 페이지에서 추출하거나, 안전한 기본값 사용
                article_title = soup.select_one("div.detail-title h2").text.strip() if soup.select_one("div.detail-title h2") else None

                news_img = soup.select_one("div.detail-visual img")
                article_img = BASE_URL + news_img["src"] if news_img and news_img.get("src") else None

                es.update(
                    index="article_data",
                    id=news_id,
                    doc={
                        "article_img": article_img
                    }
                )

                article_raw ={
                    "article_id": news_id,
                    "article_title": article_title,
                    "article_content": content
                }

                error_doc = {
                "@timestamp": now_kst_iso,
                "log": {
                    "level": "ERROR",
                    "logger": logger_name
                },
                "message": f"{news_id}결측치 존재, url :{url}"
                }

                null_count = 0
                for v in article_raw.values():
                    if v in (None, "", []):
                        null_count += 1
                if null_count >= 1:
                    es.create(index="error_log", id=news_id, document=error_doc)  
                    continue
                else:
                    es.index(index="article_raw", id=news_id, document=article_raw)

            except httpx.RequestError as e:
                print(f"[KBS 오류] URL 접근 실패 ({url}): {e}")
            except Exception as e:
                # 'newsTitle' KeyError 방지를 위해 data.get("newsTitle", "...") 대신 상세 페이지에서 추출하도록 변경했습니다.
                print(f"[KBS 오류] 데이터 파싱 실패 ({url}): {e}")

    print(f"KBS {len(article_list)}건 크롤링 완료.")

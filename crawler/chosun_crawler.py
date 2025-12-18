import httpx
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
import asyncio # 비동기 지연을 위해 추가
from typing import List, Dict, Any
from util.elastic import es

KST = timezone(timedelta(hours=9))
now_kst = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

async def chosun_crawl(bigkinds_data: List[Dict[str, Any]]):
    """
    빅카인즈에서 받은 URL 리스트를 사용하여 조선일보 상세 기사를 비동기적으로 크롤링합니다.
    429 Too Many Requests 오류를 해결하기 위해 비동기 지연을 추가합니다.
    """
    print(f"조선일보 상세 크롤링 구동 시작:{now_kst}")

    id_list = [data["news_id"] for data in bigkinds_data]
    url_list = [data["url"] for data in bigkinds_data]

    domain = "chosun"
    article_list = []

    # httpx를 사용하여 비동기 HTTP 요청 처리
    async with httpx.AsyncClient(timeout=15.0, headers=HEADERS) as client:

        for news_id, url in zip(id_list, url_list):

            try:
                # 🚨 429 Too Many Requests 오류 해결: 비동기 지연 시간 추가 (0.5초)
                await asyncio.sleep(2)

                # 기사 상세 페이지 접속 및 본문 추출
                resp = await client.get(url)
                resp.raise_for_status() # 4xx, 5xx 에러 시 예외 발생

                soup = BeautifulSoup(resp.text, "html.parser")

                # --- 본문 추출 ---
                # 불필요한 태그 제거
                for tag in soup.select("div.ad, div.promotion, div.related, div.article-body > :last-child"):
                    tag.decompose()

                paragraphs = soup.select("section.article-body p, div.article-body p")
                full_content = " ".join([p.get_text(strip=True) for p in paragraphs]).strip()
                if not full_content: # 본문이 추출되지 않으면 AMP 버전 시도 (선택 사항)
                     amp_url = url + "?outputType=amp"
                     amp_resp = await client.get(amp_url)
                     amp_soup = BeautifulSoup(amp_resp.text, "lxml")
                     amp_paragraphs = amp_soup.select("section.article-body p") or amp_soup.select("article p")
                     full_content = " ".join([p.get_text(strip=True) for p in amp_paragraphs]).strip()
                # --- 본문 추출 끝 ---

                # --- 기타 정보 추출 ---
                article_name_tag = soup.select_one("h1.article-header__title")
                # 🚨 'newsTitle' KeyError 방지: 상세 페이지에서 추출하거나, 기본값 사용
                article_title = article_name_tag.text.strip() if article_name_tag else "제목 추출 실패"


                image_tag = soup.select_one("div.article-body figure img")
                article_img = image_tag.get("src") if image_tag and image_tag.get("src") else None

                es.update(
                    index="article_data",
                    id=news_id,
                    doc={
                        "article_img": article_img,
                    }
                )

                article_raw ={
                    "article_id": news_id,
                    "article_title": article_title,
                    "article_content": full_content 
                }

                es.index(index="article_raw", id=news_id, document=article_raw)

            except httpx.RequestError as e:
                print(f"[조선 오류] URL 접근 실패 ({url}): {e}")
            except Exception as e:
                print(f"[조선 오류] 데이터 파싱 실패 ({url}): {e}")

    print(f"조선일보 {len(article_list)}건 크롤링 완료.")


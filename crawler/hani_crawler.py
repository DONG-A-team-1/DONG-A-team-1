from urllib.parse import urlparse, urlunparse
from datetime import datetime, timezone, timedelta
import httpx
from bs4 import BeautifulSoup
from util.logger import Logger
from util.elastic_templates import build_error_doc
from typing import List, Dict, Any
from util.elastic import es
import os
import inspect


logger = Logger().get_logger(__name__)
# base_url = "<https://www.hani.co.kr/arti>"

filename = os.path.basename(__file__)
funcname = inspect.currentframe().f_back.f_code.co_name

logger_name = f"{filename}:{funcname}"
now_kst_iso = datetime.now(timezone(timedelta(hours=9))).isoformat()
KST = timezone(timedelta(hours=+9))
now_kst = datetime.now(KST).strftime("%Y%m%d%H%M%S")
HEADERS = {
  "User-Agent": "...Chrome...",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
  "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
  "Cache-Control": "no-cache",
  "Pragma": "no-cache",
  "Referer": "https://www.hani.co.kr/"
}

# 웹에서 브라우저인 척 하고 긁어오기 위해서 넣은 구문
# 브라우저처럼 보이게 해서 정상 HTML 받기 위해서
# User Agent가 비어있거나 기본 https값이면 에러

def force_https(url: str) -> str:
    if not url:
        return url

    parsed = urlparse(url)

    # http → https로 변경
    if parsed.scheme == "http":
        parsed = parsed._replace(scheme="https")
        return urlunparse(parsed)

    # 스킴이 없고 //로 시작하는 경우: 프로토콜 상대 URL
    if parsed.scheme == "" and url.startswith("//"):
        return "https:" + url

    # 이미 https 이거나, 다른 스킴이면 그대로 반환
    return url

async def hani_crawl(bigkinds_data: List[Dict[str,Any]]):  # 뷰티풀 숩으로 가져오기
    id_list = [data["article_id"] for data in bigkinds_data]
    url_list = [data["url"] for data in bigkinds_data]

    article_list = []
    error_list =[]
    empty_articles = []

    async with httpx.AsyncClient(timeout=10.0, headers=HEADERS, http2=True,  follow_redirects=True) as client:
        # 기본 최신기사 링크 가져와서 크롤링
        for article_id, url in zip(id_list, url_list):
            try:
                url = force_https(url) # 아예 원본 기사 하나 가져옴
                res = await client.get(url)
                soup = BeautifulSoup(res.text, "html.parser")
                logger.info(f"crawling {url}")
    
                # 제목 art_name
                title_tag = soup.select_one('h3.ArticleDetailView_title__9kRU_')
                if not title_tag:
                    logger.info("제목 태그 없음")
                    continue  # 제목 태그 없으면 건너뛰기
                article_title = title_tag.get_text(strip=True)
    
                # 본문
                paragraphs = soup.select('div.article-text p.text')
                if not paragraphs:
                    logger.info("본문 없음 ")
                    continue
    
                article_content = " ".join(p.get_text(strip=True) for p in paragraphs[:-1])
                if len(article_content) < 50:  # 너무 짧으면 영상 기사 등으로 판단하여 제외
                    logger.info("영상 기사일 가능성")
                    continue
    
                # 대표 이미지 저장
                image = soup.select_one('picture img')
                article_img = image.get('src')
    
    
                es.update(
                    index="article_data",
                    id=article_id,
                    doc={
                        "article_img": article_img,
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
    print(f"==== 한겨례 상세 크롤링 완료: {len(result)}개 성공====")
    return result


from util.elastic import es
from util.text_cleaner import clean_article_text


# 수집된 기사에 대한 전처리 작업을 시행하기 위한 함수입니다
def clean_articles(article_ids: list[str]):
    
    body = {
        "_source": ["article_id", "article_title", "article_content"],
        "size": len(article_ids),
        "query": {"terms": {"article_id": article_ids}}
    }

    # ES의 article_raw 인덱스에서 전달받은 article_id들에 대해서 데이터를 호출합니다
    resp = es.search(index="article_raw", body=body)
    
    # 필요한 데이터만 추출
    article_list = [h["_source"] for h in resp["hits"]["hits"]]

    for article in article_list:
        article_id = article.get("article_id")
        # 기사의 제목을 전처리합니다, 에러 방지위해 혹시 None인 겯우 방지 위해 공백 문자열을 넣고 갑니다
        title_raw = article.get("article_title")
        if title_raw is None:
            title_raw = ""
        elif isinstance(title_raw, list):
            title_raw = " ".join(str(x) for x in title_raw if x) #전달받은 데이터가 문자열이 아닌 배열인 경우에 이를 문장으로 다시 변환합니다

        # 상단 title_raw 전처리와 동일한 진행입니다
        content_raw = article.get("article_content")
        if content_raw is None:
            content_raw = ""
        elif isinstance(content_raw, list):
            content_raw = " ".join(str(x) for x in content_raw if x)

        # util.text_cleaner에 담긴 문자열 전처리 함수를 호출해서 사용합니다
        title = clean_article_text(str(title_raw).strip())
        content = clean_article_text(str(content_raw).strip())

        cleaned_article = {
            "article_id": article_id,
            "article_title": title,
            "article_content": content,
            "status": "1"
        }
        # 성공적으로 전처리 된 기사들을 아직 비어있는 article_data 인덱스에 삽입시킵니다
        es.update(index="article_data_validate", id=article_id, body={"doc": cleaned_article})
    return None


# bigkinds 수집 성공했으나 기사 원문 수집에 실패한 경우를 필터링 하기 위한 함수입니다
def delete_null(article_ids: list[str]) -> list[str]:
    """
    이번 세션에서 수집한 article_ids 중,
    article_data에서 본문이 없거나 비어있는 문서를 삭제.
    """
    if not article_ids:
        return []

    # 본문/제목 필드명은 너희 스키마에 맞게 조정
    # 예: article_content / article_title
    query = {
        "_source": ["article_id", "article_title", "article_content"],
        "size": len(article_ids),
        "query": {
            "terms": {"article_id": article_ids}
        }
    }

    resp = es.search(index="article_raw", body=query)
    hits = resp.get("hits", {}).get("hits", [])

    null_ids = []
    for h in hits:
        src = h.get("_source", {})
        aid = src.get("article_id")
        title = (src.get("article_title") or "").strip()
        content = src.get("article_content")

        # content가 None이거나, 문자열인데 공백이거나, 리스트인데 비어있으면 실패로 간주
        empty_content = (
            content is None or
            (isinstance(content, str) and not content.strip()) or
            (isinstance(content, list) and len(content) == 0)
        )

        if empty_content:
            es.delete(index="article_data", id=aid)
            es.delete(index="article_raw", id=aid)
            null_ids.append(aid)

    return null_ids


# 이미 들어간 내용 전처리용
def re_clean_articles():
    body = {
        "_source": ["article_id", "article_title", "article_content"],
        "size": 500

    }

    # ES의 article_raw 인덱스에서 전달받은 article_id들에 대해서 데이터를 호출합니다
    resp = es.search(index="article_raw", body=body)

    # 필요한 데이터만 추출
    article_list = [h["_source"] for h in resp["hits"]["hits"]]
    print(len(article_list))
    for article in article_list:
        article_id = article.get("article_id")

        # 기사의 제목을 전처리합니다, 에러 방지위해 혹시 None인 겯우 방지 위해 공백 문자열을 넣고 갑니다
        title_raw = article.get("article_title")
        if title_raw is None:
            title_raw = ""
        elif isinstance(title_raw, list):
            title_raw = " ".join(str(x) for x in title_raw if x)  # 전달받은 데이터가 문자열이 아닌 배열인 경우에 이를 문장으로 다시 변환합니다

        # 상단 title_raw 전처리와 동일한 진행입니다
        content_raw = article.get("article_content")
        if content_raw is None:
            content_raw = ""
        elif isinstance(content_raw, list):
            content_raw = " ".join(str(x) for x in content_raw if x)

        # util.text_cleaner에 담긴 문자열 전처리 함수를 호출해서 사용합니다
        title = clean_article_text(str(title_raw).strip())
        content = clean_article_text(str(content_raw).strip())

        cleaned_article = {
            "article_id": article_id,
            "article_title": title,
            "article_content": content
        }
        # 성공적으로 전처리 된 기사들을 아직 비어있는 article_data 인덱스에 삽입시킵니다
        es.update(index="article_data", id=article_id, body={"doc": cleaned_article})
    return None

if __name__ == "__main__":
    delete_null()


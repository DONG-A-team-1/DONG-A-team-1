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
            "article_content": content
        }
        # 성공적으로 전처리 된 기사들을 아직 비어있는 article_data 인덱스에 삽입시킵니다
        es.update(index="article_data", id=article_id, body={"doc": cleaned_article})
    return None


# bigkinds 수집 성공했으나 기사 원문 수집에 실패한 경우를 필터링 하기 위한 함수입니다
def delete_null():
    null_id = [ ]
    query = {
        "_source": ["article_id"],
        "size":500,
        "query": {
            "range": {
                "collected_at": {
                    "gte": f"now-2h",
                    "lte": "now"
                }
            }
        }
    }

    ids = set()
    # article_raw 인덱스에서 최근 2시간간의 기사 500개의 식별키를 호출합니다
    resp = es.search(index="article_raw", body=query)

    # 호출된 모든 식별키를 ids에 담습니다(혹시 몰라 set이긴합니다)
    for h in resp["hits"]["hits"]:
        ids.add(h["_source"]["article_id"])

    # 이번에는 article_data 인덱스에 대해서 동일한 조건(2시간,500개)로 수집합니다
    # 수집 결과를 비교 대조하고 결측 데이터를 삭제합니다
    resp2 = es.search(index="article_data", body=query)
    for h in resp2["hits"]["hits"]:
        data_id = h["_source"]["article_id"]
        if data_id not in ids:
            es.delete(index="article_data", id=data_id)
            null_id.append(data_id)
    # crawler.main 함수에서 사용할 원문이 없는, 추후 작업을 진행할 이유가 없는 데이터를 삭제합니다
    return null_id



if __name__ == "__main__":
    delete_null()


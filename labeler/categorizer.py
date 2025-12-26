import joblib
from util.elastic import  es
from elasticsearch import helpers
from util.logger import build_error_doc

model = r"C:\Users\M\Desktop\Project\DONG-A-team-1\model\news_category_classifier_add.pkl"
model = joblib.load(model)

# article_id를 리스트 형태로 넣어주면 해당 기사들을 찾고 카테고리를 추가합니다
def categorizer(article_list):

    query = {
        "_source": ["article_id", "article_title", "article_content"],
        "size": 1000,
        "query": {
            "terms": {"article_id": article_list}
        }
    }

    resp = es.search(index="article_data", body=query)
    hits = resp.get("hits", {}).get("hits", [])

    article_ids = []
    texts = []
    for h in hits:
        src = h.get("_source", {})
        article_id = src.get("article_id")
        if not article_id:
            continue

        title = src.get("article_title") or ""
        content = src.get("article_content") or ""
        texts.append(f"{title} {content}")
        article_ids.append(article_id)

    predict_labels = model.predict(texts)

    label_mapping = {
        0: "스포츠",
        1: "지역",
        2: "문화",
        3: "사회/경제/산업",
        4: "정치",
        5: "국제"
    }
    
    results = []
    null_cat = []

    for a_id, predict_label in zip(article_ids, predict_labels):
        lbl = int(predict_label)
        if lbl in label_mapping:
            results.append({
                "article_id": a_id,
                "category": label_mapping[lbl]
            })
        else:
            null_cat.append(a_id)

    actions = (
        {
            "_op_type": "update",
            "_index": "article_data",
            "_id": r["article_id"],  # ES 문서 _id가 article_id인 경우
            "doc": {
                "article_label": {
                    "category": r["category"]
                }
            }
        }
        for r in results
    )

    for null_id in null_cat:
        es.delete(index="article_data",id=null_id)
        es.delete(index="article_raw",id=null_id)

    if len(null_cat):
        error_doc = build_error_doc(
            message=f"{len(null_cat)}개 기사 카테고리 확인되지 않아 삭제함"
        )
        es.create(index="error_log", document=error_doc)

    helpers.bulk(
        es,
        actions,
        request_timeout=120,
        raise_on_error=False,
    )

    print(f"{len(results)}개의 기사 카테고리 라벨링 성공")

if __name__ == "__main__":
    pass


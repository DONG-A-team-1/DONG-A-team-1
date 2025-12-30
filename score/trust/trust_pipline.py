# trust_pipeline.py 신뢰도 점수 파이프라인

from elasticsearch import Elasticsearch, helpers
from score.trust.total_trust_score import compute_trust_score

ES_HOST = "http://localhost:9200"
INDEX_NAME = "article_data"

es = Elasticsearch(ES_HOST)


def run_trust_pipeline(article_ids: list, batch_size: int = 100):
    """
    이번 사이클 article_id만 대상으로
    신뢰도 점수 계산 → status=4 업데이트
    """

    if not article_ids:
        print("[trust_pipeline] article_ids 비어 있음")
        return

    # dict / str 혼용 대응
    ids = [
        a["article_id"] if isinstance(a, dict) else a
        for a in article_ids
    ]

    # 반드시 mget 필요
    resp = es.mget(
        index=INDEX_NAME,
        body={"ids": ids},
        _source=["article_title", "article_content"]
    )

    docs = [d for d in resp["docs"] if d.get("found")]

    actions = []
    total_docs = len(docs)
    updated_docs = 0

    for d in docs:
        doc_id = d["_id"]
        src = d["_source"]

        trust_result = compute_trust_score(
            src.get("article_title", ""),
            src.get("article_content", "")
        )

        actions.append({
            "_op_type": "update",
            "_index": INDEX_NAME,
            "_id": doc_id,
            "doc": trust_result   # status=4 포함
        })

        if len(actions) >= batch_size:
            helpers.bulk(es, actions)
            updated_docs += len(actions)
            actions.clear()

    if actions:
        helpers.bulk(es, actions)
        updated_docs += len(actions)

    print(
        f"[trust_pipeline_by_ids] "
        f"대상 기사 수: {total_docs}, "
        f"신뢰도 점수 부여 완료: {updated_docs}"
    )

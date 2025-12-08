from elasticsearch import Elasticsearch

es = Elasticsearch(
    "http://localhost:9200",
    basic_auth=("elastic", "elastic"),
    verify_certs=False,
    ssl_show_warn=False,
)

ES_INDEX = "products"

def create_index():
    if es.indices.exists(index=ES_INDEX):
        print("기존 인덱스를 삭제합니다.")
        es.indices.delete(index=ES_INDEX)

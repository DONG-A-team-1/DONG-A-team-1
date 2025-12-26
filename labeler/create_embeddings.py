from util.elastic import es
import numpy as np
import re
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import TfidfVectorizer

model = SentenceTransformer("snunlp/KR-SBERT-V40K-klueNLI-augSTS")

def sent_weights_tfidf_in_doc(sents: list[str], mode="sum") -> np.ndarray:
    if not sents:
        return np.array([], dtype=np.float32)

    vec = TfidfVectorizer(
        analyzer="char",     # 한국어에서 토크나이저 없이도 안정적
        ngram_range=(3, 5),
        min_df=1
    )
    X = vec.fit_transform(sents)  # (n_sent, vocab)

    if mode == "sum":
        scores = np.asarray(X.sum(axis=1)).ravel()
    elif mode == "mean":
        nnz = np.maximum(X.getnnz(axis=1), 1)
        scores = np.asarray(X.sum(axis=1)).ravel() / nnz
    else:  # "max"
        scores = np.asarray(X.max(axis=1).toarray()).ravel()

    # softmax-like (너무 쏠리면 temp 올려도 됨)
    scores = scores + 1e-6
    scores = scores - scores.max()
    w = np.exp(scores)
    w = w / (w.sum() + 1e-12)
    return w.astype(np.float32)

# L2 정규화를 통해 1에 가깝게 눌러줍니다
def l2_normalize(v: np.ndarray, eps: float = 1e-12) -> np.ndarray:
    return v / (np.linalg.norm(v) + eps)

# 문장별 벡터 생성 위해서 문장 단위 분리를 진행합니다
def split_sentences_ko(text: str) -> list[str]:
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return []
    return re.split(r"(?<=[.!?])\s+", text)

# 본격적으로 임베딩을 생성합니다
def build_doc_embeddings(
    model,
    articles: list[dict],
    alpha: float = 0.3,
    sent_weight_mode: str = "tfidf",
    max_sents: int = 40,
) -> dict[str, list[float]]:
    """
    input:
      articles = [
        {"article_id": "...", "title": "...", "content": "..."},
        ...
      ]

    output:
      {
        article_id: [float, float, ...]  # len = 768
      }
    """

    per_doc_sents = []
    per_doc_weights = []
    titles = []
    ids = []

    # 벡터화 수행을 위한 전처리 작업을 합니다
    # id, 제목 , [본문(문장단위분리)] , [문장별 가중치] 를 각각 다른 list에 담아둡니다 (임베딩을 한번에 진행하기 위한 batch 처리)
    for a in articles:
        a_id = a["article_id"]
        title = (a.get("article_title") or "").strip()
        content = (a.get("article_content") or "").strip()
        
        #문장 단위 분리입니다
        sents = split_sentences_ko(content)
        sents = [s for s in sents if s]

        # 기사 내용이 너무 길면 40번 문장 이후로는 날리는 기능입니다, 후반 문장 중요도도 낮고 처리 시간이 길어지는걸 방지합니다
        if len(sents) > max_sents:
            sents = sents[:max_sents]
        
        # 지정한 옵션에 따라 문장 가중치를 결정합니다. 저희는 디폴트로 sqrt_len 사용합니다
        if not sents:
            w = np.array([], dtype=np.float32)
        elif sent_weight_mode == "tfidf":  # ✅ 추가
            w = sent_weights_tfidf_in_doc(sents, mode="sum")

            if len(w) > 0:
                w[0] *= 1.3
            if len(w) > 1:
                w[1] *= 1.2

        elif sent_weight_mode == "uniform":
            w = np.ones(len(sents), dtype=np.float32)
        elif sent_weight_mode == "len":
            w = np.array([len(s) for s in sents], dtype=np.float32)
        else:  # sqrt_len
            w = np.sqrt(np.array([len(s) for s in sents], dtype=np.float32))

        per_doc_sents.append(sents)
        per_doc_weights.append(w)
        titles.append(title if title else " ")
        ids.append(a_id)

    # 우선 제목 임베딩 리스트를 생성합니다
    title_vecs = model.encode(titles, normalize_embeddings=False)

    # 모든 문장의 임베딩 리스트를 생성합니다
    flat_sents = []
    spans = []
    idx = 0

    #전체 기사의 문장 리스트 (per_doc_sents)에서 각 기사의 문장 리스트를 꺼내 위치를 확인(spans) 하고 하나의 리스트(flat_sents)에 담아둡니다
    for sents in per_doc_sents:
        flat_sents.extend(sents)
        spans.append((idx, idx + len(sents)))
        idx += len(sents)
    
    # 모든 문장을 batch로 벡터화합니다
    sent_vecs = (
        model.encode(flat_sents, normalize_embeddings=False)
        if flat_sents else None
    )

    # 이제 분리된 제목과 본문을 합치고 기사별 벡터를 생성합니다
    doc_embeddings = {}

    # 앞서 언급된 전체 배열에서 각기 다른 기사의 문장과 가중치들을 다시 분리해내는 과정입니다
    for i, aid in enumerate(ids):
        Et = title_vecs[i]
        start, end = spans[i]
        if start == end:
            Ebody = np.zeros_like(Et)
        else:
            Es = sent_vecs[start:end]
            w = per_doc_weights[i]
            w = w / (w.sum() + 1e-12)
            Ebody = (Es * w[:, None]).sum(axis=0)
        # 완성된 임베딩은 article_id : 벡터LIST 형태로 dict가 됩니다
        Edoc = alpha * Et + (1 - alpha) * Ebody
        Edoc = l2_normalize(Edoc.astype(np.float32))
        doc_embeddings[aid] = Edoc.tolist()  # ES 저장용 list
    return doc_embeddings


# 원하는 기사 식별키의 목록을 넣고 임베딩 필드를 업데이트 시키는 함수입니다
from elasticsearch import helpers

def create_embedding(article_list):
    # 1) 원하는 _id(=article_id)들을 mget으로 정확히 조회
    resp = es.mget(
        index="article_data",
        body={"ids": article_list},
        _source=["article_id", "article_title", "article_content"]
    )

    # 2) 존재하는 문서만 추출 (없는 id는 누락됨)
    articles = [d["_source"] for d in resp["docs"] if d.get("found")]

    # (옵션) 못 찾은 id 로그
    not_found = [d["_id"] for d in resp["docs"] if not d.get("found")]
    if not_found:
        print(f"[warn] not found ids: {len(not_found)} (ex: {not_found[:5]})")

    doc_embeddings = build_doc_embeddings(
        articles=articles,
        model=model,
        sent_weight_mode="tfidf",
    )

    actions = (
        {
            "_op_type": "update",
            "_index": "article_data",
            "_id": article_id,
            "doc": {"article_embedding": vec}
        }
        for article_id, vec in doc_embeddings.items()
    )

    helpers.bulk(es, actions, chunk_size=500, request_timeout=120)
    print("임베딩 생성 성공")


def re_embedding():
    body = {
        "_source": ["article_id", "article_title", "article_content"],
        "size": 500,
    }
    resp = es.search(index="article_data", body=body)
    articles = [h["_source"] for h in resp["hits"]["hits"]]

    doc_embeddings = build_doc_embeddings(articles= articles, model=model)

    actions = (
        {
            "_op_type": "update",
            "_index": "article_data",
            "_id": article_id,
            "doc": {
                "article_embedding": vec  # ← 필드 지정
            }
        }
        for article_id, vec in doc_embeddings.items()
    )

    # es에 모든 내용을 한번에 올립니다
    helpers.bulk(es, actions, chunk_size=500, request_timeout=120)
    print("작업 성공")

if __name__ == "__main__":
    re_embedding()



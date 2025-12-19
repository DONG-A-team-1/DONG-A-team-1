from util.elastic import es
import numpy as np
import re
from sentence_transformers import SentenceTransformer
from elasticsearch import helpers

def l2_normalize(v: np.ndarray, eps: float = 1e-12) -> np.ndarray:
    return v / (np.linalg.norm(v) + eps)

def split_sentences_ko(text: str) -> list[str]:
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return []
    return re.split(r"(?<=[.!?])\s+", text)

def build_doc_embeddings(
    model,
    articles: list[dict],
    alpha: float = 0.3,
    sent_weight_mode: str = "sqrt_len",
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

    # 1️⃣ 기사별 문장 분리 + 가중치 준비
    for a in articles:
        a_id = a["article_id"]
        title = (a.get("article_title") or "").strip()
        content = (a.get("article_content") or "").strip()

        sents = split_sentences_ko(content)
        sents = [s for s in sents if s]

        if len(sents) > max_sents:
            sents = sents[:max_sents]

        if not sents:
            w = np.array([], dtype=np.float32)
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

    # 2️⃣ 제목 임베딩 (batch)
    title_vecs = model.encode(titles, normalize_embeddings=False)

    # 3️⃣ 모든 문장을 한 번에 임베딩
    flat_sents = []
    spans = []
    idx = 0
    for sents in per_doc_sents:
        flat_sents.extend(sents)
        spans.append((idx, idx + len(sents)))
        idx += len(sents)

    sent_vecs = (
        model.encode(flat_sents, normalize_embeddings=False)
        if flat_sents else None
    )

    # 4️⃣ 문서 임베딩 생성
    doc_embeddings = {}

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

        Edoc = alpha * Et + (1 - alpha) * Ebody
        Edoc = l2_normalize(Edoc.astype(np.float32))
        doc_embeddings[aid] = Edoc.tolist()  # ES 저장용 list
    return doc_embeddings


def create_embedding(article_list):

    model = SentenceTransformer("snunlp/KR-SBERT-V40K-klueNLI-augSTS")
    body = {
        "_source": ["article_id", "article_title", "article_content"],
        "size": len(article_list),
        "query": {"terms": {"article_id": article_list}}
    }

    resp = es.search(index="article_data", body=body)

    articles = [h["_source"] for h in resp["hits"]["hits"]]
    doc_embeddings = build_doc_embeddings(articles=articles, model=model)

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

    keys = list(doc_embeddings.keys())


    helpers.bulk(es, actions, chunk_size=500, request_timeout=120)
    print("임베딩 생성 성공")

if __name__ == "__main__":
    create_embedding(article_list=["01100401.20251218171902001","01100401.20251218171902001"])



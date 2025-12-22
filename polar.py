from util.elastic import es

import re
import numpy as np
from collections import Counter, defaultdict

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score


def find_best_k(X, k_min=2, k_max=30, random_state=42):
    """
    TF-IDF(sparse) + KMeans에서 silhouette(cosine) 최대인 k 선택
    """
    n = X.shape[0]
    k_max = min(k_max, n - 1)
    if k_max < k_min:
        raise ValueError(f"Not enough samples: n={n}, need at least {k_min+1}")

    results = []
    best_sil, best_k, best_km = -1.0, None, None

    for k in range(k_min, k_max + 1):
        km = KMeans(n_clusters=k, random_state=random_state, n_init="auto")
        labels = km.fit_predict(X)

        sil = silhouette_score(X, labels, metric="cosine")
        inertia = float(km.inertia_)
        results.append((k, sil, inertia))

        if sil > best_sil:
            best_sil, best_k, best_km = sil, k, km

    print("k\tSilhouette(cos)\tInertia")
    for k, sil, inertia in results:
        print(f"{k}\t{sil:.4f}\t\t{inertia:.2f}")
    print(f"\nBEST k = {best_k} (silhouette={best_sil:.4f})")

    return best_k, best_km, results


def create_topic():
    query = {
        "_source": ["article_id", "article_title", "article_content", "features"],
        "size": 500,
        "query": {
            "range": {"collected_at": {"gte": "now-1d", "lte": "now"}}
        }
    }

    resp = es.search(index="article_data", body=query)
    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        print("No documents found.")
        return

    article_ids, titles, texts, features_list = [], [], [], []

    for h in hits:
        src = h.get("_source", {})
        article_ids.append(src.get("article_id"))

        title = src.get("article_title", "") or ""
        content = src.get("article_content", "") or ""
        titles.append(title)
        texts.append(f"{title} {content}")

        feats = src.get("features", []) or []
        if not isinstance(feats, list):
            feats = [str(feats)]
        features_list.append([str(x).strip() for x in feats if str(x).strip()])

    # ===== 1) 전체 feature vocab 만들기 (노이즈 컷: 1회 이하 제거) =====
    global_counter = Counter()
    for feats in features_list:
        global_counter.update(feats)

    vocab = [w for w, c in global_counter.items() if c > 1]  # 등장 1회 이하는 제거
    vocab = sorted(vocab, key=lambda w: global_counter[w], reverse=True)

    if not vocab:
        print("Vocab is empty after filtering. (min count > 1)")
        return

    vocab_set = set(vocab)

    # ===== 2) 기사 원문에서 vocab에 해당하는 feature만 남겨 '토큰 문서' 생성 =====
    token_docs = []
    for text in texts:
        tokens = re.findall(r"[0-9A-Za-z가-힣_]+", text)
        kept = [t for t in tokens if t in vocab_set]
        token_docs.append(" ".join(kept))

    # (선택) 매칭 0개 문서가 너무 많으면 품질 떨어짐 → 확인용
    empty_cnt = sum(1 for d in token_docs if not d.strip())
    if empty_cnt > 0:
        print(f"[WARN] empty token_docs: {empty_cnt}/{len(token_docs)} (no vocab match)")

    # ===== 3) 고정 vocab으로 TF-IDF 벡터화 =====
    vec = TfidfVectorizer(
        vocabulary=vocab,          # ✅ 전역 feature vocab을 고정 축으로 사용
        token_pattern=r"[^ ]+",
        sublinear_tf=True,
        norm="l2"
    )
    X = vec.fit_transform(token_docs)

    # ===== 4) 자동 k 선택 + 클러스터링 =====
    best_k, best_km, _ = find_best_k(X, k_min=2, k_max=30)
    labels = best_km.labels_

    # ===== 검증 출력: 클러스터 top features + 제목 =====
    terms = np.array(vec.get_feature_names_out())
    centers = best_km.cluster_centers_

    cluster_titles = defaultdict(list)
    for lbl, title in zip(labels, titles):
        cluster_titles[int(lbl)].append(title)

    for c in range(best_k):
        top_terms = terms[np.argsort(centers[c])[::-1][:8]]
        print(f"\n[Cluster {c}] (n={len(cluster_titles[c])}) top_features: {', '.join(top_terms)}")
        for t in cluster_titles[c][:10]:  # 너무 길면 10개만
            print(" -", t)

    return {
        "article_ids": article_ids,
        "titles": titles,
        "labels": labels.tolist(),
        "best_k": best_k,
        "vocab_size": len(vocab)
    }


if __name__ == "__main__":
    create_topic()

from __future__ import annotations

import numpy as np
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple, Optional, Mapping

from elasticsearch import helpers
from kiwipiepy import Kiwi
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import silhouette_score

from util.elastic import es  # Elasticsearch client

# =========================
# CONFIG
# =========================
DEBUG = False
KST = timezone(timedelta(hours=9))

# ES _id prefix (yyyymmdd_hh)
now = datetime.now(KST)
fmt = now.strftime("%Y%m%d_%H")

# 클러스터링 대상 기사 범위 설정
TOPIC_FETCH_SIZE = 1000
TITLE_BOOST = 2
COLLECTED_RANGE = "now-1d"  # now-1d ~ now

# stance params
MAX_CHAR_DISTANCE = 250
MIN_ENTITY_HITS = 1
MAX_EXAMPLE_SENTS = 2

# output
DEFAULT_OUTPUT_PATH = "data/topic_entity_stance.json"
DEBUG_OUTPUT_PATH = "data/topic_entity_stance_debug.json"

# post-filter params
MIN_CLUSTER_SIZE = 6          # "5개 이하 제거" => 최소 6
NEUTRAL_RATIO_MAX = 0.6       # 중립 비중이 이 이상이면 제거
REQUIRE_BOTH_SIDES = True     # pos만/neg만이면 제거

# topic_polarity index name
TOPIC_INDEX_NAME = "topic_polarity"

# =========================
# DEBUG
# =========================
def dprint(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)

# =========================
# 0) 정규화 관련 함수들입니다
# =========================
TOKEN_RE = re.compile(r"[0-9A-Za-z가-힣_]+")
_WS_RE = re.compile(r"\s+")
_CORP_SUFFIX_RE = re.compile(r"(주\)|\(주\)|㈜|유한회사|주식회사|\(유\)|유\)|유한)")

def tokenize(s: str) -> List[str]:
    return TOKEN_RE.findall(s or "")

def norm_nospace(s: str) -> str:
    return _WS_RE.sub("", (s or "")).strip()

def normalize_entities(xs: Any) -> List[str]:
    if not xs:
        return []
    out = []
    for x in xs:
        t = str(x).strip()
        if not t:
            continue
        if len(t) < 2:
            continue
        t = _CORP_SUFFIX_RE.sub("", t).strip()
        if len(t) < 2:
            continue
        out.append(t)
    return list(dict.fromkeys(out))

# =========================
# 1) K 선택 (silhouette 안전 -> 최적의 클러스터 수 산출)
# =========================
def find_best_k_safe(
    X,
    k_min: int = 2,
    k_max: int = 30,
    random_state: int = 42,
) -> Tuple[int, Optional[KMeans]]:
    n = X.shape[0]
    k_max = min(k_max, n - 1)
    if k_max < k_min:
        return 1, None

    best_sil, best_k, best_km = -1.0, None, None
    for k in range(k_min, k_max + 1):
        km = KMeans(n_clusters=k, random_state=random_state, n_init="auto")
        labels = km.fit_predict(X)
        if len(set(labels)) < 2:
            continue
        sil = silhouette_score(X, labels, metric="cosine")
        if sil > best_sil:
            best_sil, best_k, best_km = sil, k, km

    if best_k is None:
        return 1, None
    return int(best_k), best_km

# =========================
# 2) 특성 추출 -> 전체 특성 리스트 생성
# =========================
def build_vocab_from_features(features_list: List[List[str]], min_df: int = 2) -> List[str]:
    cnt = Counter()
    for feats in features_list:
        for f in feats:
            s = str(f).strip()
            if not s:
                continue
            phrase = norm_nospace(s)
            if len(phrase) >= 4:
                cnt[phrase] += 1
            for t in tokenize(s):
                if len(t) >= 2:
                    cnt[t] += 1

    vocab = [t for t, c in cnt.items() if c >= min_df]
    vocab.sort(key=lambda t: cnt[t], reverse=True)
    return vocab

# =========================
# 3) 토픽 클러스터 생성
# =========================
def create_topic(
    index_name: str = "article_data",
    size: int = TOPIC_FETCH_SIZE,
    *,
    title_boost: int = TITLE_BOOST,
) -> Dict[str, Any]:
    query = {
        "_source": ["article_id", "article_title", "article_content", "features", "article_label", "collected_at"],
        "size": size,
        "query": {
            "bool": {
                "filter": [
                    {"range": {"collected_at": {"gte": COLLECTED_RANGE, "lte": "now"}}},
                    {"term": {"article_label.category": "정치"}},
                ]
            }
        },
    }

    resp = es.search(index=index_name, body=query)
    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        print("[create_topic] No documents found.")
        return {"article_ids": [], "labels": [], "cluster_keywords": {}}

    article_ids: List[str] = []
    texts: List[str] = []
    texts_ns: List[str] = []
    features_list: List[List[str]] = []

    for h in hits:
        src = h.get("_source", {})
        aid = src.get("article_id")
        if not aid:
            continue
        article_ids.append(aid)

        title = (src.get("article_title") or "").strip()
        content = src.get("article_content") or ""
        if isinstance(content, list):
            content = " ".join(content)

        title_part = (" ".join([title] * max(1, int(title_boost)))).strip()
        full_text = f"{title_part} {content}".strip()
        texts.append(full_text)
        texts_ns.append(norm_nospace(full_text))

        feats = src.get("features") or []
        if not isinstance(feats, list):
            feats = [str(feats)]
        feats = [str(x).strip() for x in feats if str(x).strip()]
        features_list.append(feats)

    vocab = build_vocab_from_features(features_list, min_df=2)
    if not vocab:
        print("[create_topic] vocab empty -> return single cluster(0).")
        return {"article_ids": article_ids, "labels": [0] * len(article_ids), "cluster_keywords": {0: []}}

    vocab_set = set(vocab)
    phrase_vocab = [v for v in vocab if len(v) >= 4]

    token_docs: List[str] = []
    valid_idx: List[int] = []
    kept_counts: List[int] = []

    for i, (text, text_ns) in enumerate(zip(texts, texts_ns)):
        doc_tokens: List[str] = []
        doc_tokens.extend([t for t in tokenize(text) if t in vocab_set])
        for p in phrase_vocab:
            if p in text_ns:
                doc_tokens.append(p)

        kept_counts.append(len(doc_tokens))
        if doc_tokens:
            token_docs.append(" ".join(doc_tokens))
            valid_idx.append(i)

    empty_cnt = sum(1 for c in kept_counts if c == 0)
    print(f"[create_topic] total={len(article_ids)}, valid={len(valid_idx)}, empty={empty_cnt}, vocab={len(vocab)}")

    if len(valid_idx) < 3:
        print("[create_topic] Not enough valid docs -> return single cluster(0).")
        return {"article_ids": article_ids, "labels": [0] * len(article_ids), "cluster_keywords": {0: vocab[:8]}}

    vec = TfidfVectorizer(vocabulary=vocab, token_pattern=r"[^ ]+", sublinear_tf=True, norm="l2")
    X = vec.fit_transform(token_docs)

    best_k, best_km = find_best_k_safe(X, k_min=2, k_max=30)

    labels_all = [-1] * len(article_ids)
    cluster_keywords: Dict[int, List[str]] = {}

    if best_k == 1 or best_km is None:
        for i in valid_idx:
            labels_all[i] = 0
        cluster_keywords[0] = vocab[:8]
        print("[create_topic] best_k=1 (no separable clusters)")
    else:
        valid_labels = best_km.labels_.tolist()
        for pos, i in enumerate(valid_idx):
            labels_all[i] = int(valid_labels[pos])
        print(f"[create_topic] best_k={best_k}")

        terms = np.array(vec.get_feature_names_out())
        centers = best_km.cluster_centers_
        for c in range(best_k):
            top_terms = terms[np.argsort(centers[c])[::-1][:8]].tolist()
            cluster_keywords[int(c)] = top_terms
            print(f"[Cluster {c}] top_terms: {', '.join(top_terms)}")

    return {"article_ids": article_ids, "labels": labels_all, "cluster_keywords": cluster_keywords}

# =========================
# 4) 미리 만들어둔 긍/부정 말뭉치를 기반으로 리스트 생성합니다
# =========================
def load_predicate_lexicon(path: str = r"data/predicate_lexicon.json") -> Dict[str, Counter]:
    with open(path, "r", encoding="utf-8") as f:
        items = json.load(f)

    dist: Dict[str, Counter] = defaultdict(Counter)
    for it in items:
        pred = (it.get("pred") or "").strip()
        pol = (it.get("polarity") or "미정").strip()
        if pred:
            dist[pred][pol] += 1
    return dist

# =========================
# 5) 문장 단위 분리함수
# =========================
_SENT_SPLIT = re.compile(r"(?:[\.?!]\s+|다\.\s+|[\n\r]+)")

def split_sentences(text: str) -> List[str]:
    text = (text or "").strip()
    if not text:
        return []
    return [s.strip() for s in _SENT_SPLIT.split(text) if s and s.strip()]

# =========================
# 6) 키워 토크나이져 통해서 형태소 추출
# =========================
KIWI = Kiwi()
_PRED_POS = {"VV", "VA"}

def _is_negated(tokens, pred_idx: int, window: int = 3) -> bool:
    start = max(0, pred_idx - window)
    end = min(len(tokens), pred_idx + window + 1)

    for j in range(start, end):
        tk = tokens[j]
        if tk.tag == "VX" and tk.form in {"않", "않다"}:
            return True

    for j in range(max(0, pred_idx - window), pred_idx + 1):
        tk = tokens[j]
        if tk.tag == "MAG" and tk.form in {"안", "못"}:
            return True

    return False

def extract_predicates_kiwi(sent: str) -> List[Dict[str, Any]]:
    s = sent or ""
    if not s.strip():
        return []

    analyzed = KIWI.analyze(s, top_n=1)
    if not analyzed:
        return []

    tokens = analyzed[0][0]
    out: List[Dict[str, Any]] = []

    for i, tk in enumerate(tokens):
        if tk.tag in _PRED_POS:
            base = tk.form
            if base.endswith("하"):
                pred = base[:-1] + "하다"
            elif base.endswith("되"):
                pred = base[:-1] + "되다"
            else:
                pred = base + "다"
            neg = _is_negated(tokens, i, window=3)
            out.append({"pred": pred, "start": int(tk.start), "end": int(tk.start + tk.len), "neg": bool(neg)})

    for i in range(len(tokens) - 1):
        a = tokens[i]
        b = tokens[i + 1]
        if a.tag in {"NNG", "NNP", "XR"} and b.tag in {"XSV", "XSA"}:
            if b.form == "하":
                pred = a.form + "하다"
            elif b.form == "되":
                pred = a.form + "되다"
            else:
                continue
            neg = _is_negated(tokens, i + 1, window=3)
            out.append({"pred": pred, "start": int(a.start), "end": int(b.start + b.len), "neg": bool(neg)})

    uniq, seen = [], set()
    for p in out:
        key = (p["pred"], p["start"], p["end"], p["neg"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)
    return uniq

# =========================
# 7) entity span find
# =========================
def find_entity_spans(sent: str, entity: str) -> List[Tuple[int, int]]:
    spans: List[Tuple[int, int]] = []
    start = 0
    while True:
        idx = sent.find(entity, start)
        if idx == -1:
            break
        spans.append((idx, idx + len(entity)))
        start = idx + max(1, len(entity))
    if spans:
        return spans

    sent_ns = norm_nospace(sent)
    ent_ns = norm_nospace(entity)
    if not ent_ns:
        return []

    nonspace_positions = [i for i, ch in enumerate(sent) if ch.strip() != ""]
    start = 0
    while True:
        idx = sent_ns.find(ent_ns, start)
        if idx == -1:
            break
        if idx < len(nonspace_positions):
            s0 = nonspace_positions[idx]
            s1 = nonspace_positions[min(idx + len(ent_ns) - 1, len(nonspace_positions) - 1)] + 1
            spans.append((s0, s1))
        start = idx + max(1, len(ent_ns))
    return spans

# =========================
# 8) predicate scoring
# =========================
def score_predicate(pred: str, pred_dist: Dict[str, Counter], negated: bool) -> Tuple[float, float]:
    cnts = pred_dist.get(pred)
    if not cnts:
        return 0.0, 0.0

    total = sum(cnts.values()) or 0
    if total == 0:
        return 0.0, 0.0

    p_pos = cnts.get("긍정", 0) / total
    p_neg = cnts.get("부정", 0) / total
    if (p_pos + p_neg) == 0:
        return 0.0, 0.0

    if negated:
        p_pos, p_neg = p_neg, p_pos

    denom = (p_pos + p_neg) or 1.0
    score = (p_pos - p_neg) / denom
    conf = max(p_pos, p_neg) / denom
    return float(score), float(conf)

# =========================
# 9) entity-predicate stance
# =========================
def label_text_by_entities_kiwi(
    text: str,
    persons: List[str],
    orgs: List[str],
    pred_dist: Dict[str, Counter],
    *,
    max_char_distance: int = MAX_CHAR_DISTANCE,
    min_entity_hits: int = MIN_ENTITY_HITS,
    max_example_sents: int = MAX_EXAMPLE_SENTS,
) -> Dict[str, Any]:
    sents = split_sentences(text)
    persons = normalize_entities(persons)
    orgs = normalize_entities(orgs)

    if not sents or (not persons and not orgs):
        return {
            "main_entity": None,
            "final": {"label": "미정", "score": 0.0, "confidence": 0.0, "hits": 0},
            "entity_scores": {},
            "main_evidence": [],
        }

    entities = [(e, "PERSON") for e in persons] + [(e, "ORG") for e in orgs]
    entities.sort(key=lambda x: len(x[0]), reverse=True)

    ent_score = defaultdict(float)
    ent_weight = defaultdict(float)
    ent_hits = defaultdict(int)
    ent_conf_sum = defaultdict(float)
    ent_sent_cnt = defaultdict(int)
    ent_examples = defaultdict(list)
    ent_type: Dict[str, str] = {}

    for sent in sents:
        mentioned: List[Tuple[str, str, List[Tuple[int, int]]]] = []
        for name, typ in entities:
            spans = find_entity_spans(sent, name)
            if spans:
                mentioned.append((name, typ, spans))
        if not mentioned:
            continue

        preds = extract_predicates_kiwi(sent)
        if not preds:
            continue

        preds_in_lex = [p for p in preds if p["pred"] in pred_dist]
        if not preds_in_lex:
            continue

        for name, typ, spans in mentioned:
            ent_type[name] = typ

            local_hits = 0
            local_score_sum = 0.0
            local_weight_sum = 0.0
            local_conf_sum = 0.0
            matched_preds: List[str] = []

            for pr in preds_in_lex:
                pred = pr["pred"]
                ps, pe = pr["start"], pr["end"]
                neg = pr["neg"]

                dist = min(
                    min(abs(ps - es_), abs(ps - ee), abs(pe - es_), abs(pe - ee))
                    for (es_, ee) in spans
                )
                if dist > max_char_distance:
                    continue

                sc, cf = score_predicate(pred, pred_dist, negated=neg)
                if cf <= 0:
                    continue

                dist_factor = 1.0 / (1.0 + (dist / 40.0))
                w = (1.0 + cf) * dist_factor

                local_score_sum += sc * w
                local_weight_sum += w
                local_conf_sum += cf
                local_hits += 1
                matched_preds.append(pred + ("(NEG)" if neg else ""))

            if local_hits <= 0 or local_weight_sum <= 0:
                continue

            ent_score[name] += local_score_sum
            ent_weight[name] += local_weight_sum
            ent_hits[name] += local_hits
            ent_conf_sum[name] += (local_conf_sum / local_hits)
            ent_sent_cnt[name] += 1

            if len(ent_examples[name]) < max_example_sents:
                ent_examples[name].append(f"{sent}  [pred={', '.join(matched_preds[:6])}]")

    entity_scores: Dict[str, Any] = {}
    for name in ent_hits:
        if ent_hits[name] < min_entity_hits:
            continue

        avg_conf = ent_conf_sum[name] / max(ent_sent_cnt[name], 1)
        avg_score = ent_score[name] / max(ent_weight[name], 1e-9)

        entity_scores[name] = {
            "type": ent_type.get(name, "UNK"),
            "score": float(round(avg_score, 4)),
            "hits": int(ent_hits[name]),
            "confidence": float(round(avg_conf, 4)),
            "sentences": ent_examples[name],
        }

    if not entity_scores:
        return {
            "main_entity": None,
            "final": {"label": "미정", "score": 0.0, "confidence": 0.0, "hits": 0},
            "entity_scores": {},
            "main_evidence": [],
        }

    def _key(item):
        v = item[1]
        return (v["hits"] * abs(v["score"]), v["hits"])

    main_entity, m = max(entity_scores.items(), key=_key)

    if m["score"] > 0.2:
        final_label = "긍정"
    elif m["score"] < -0.2:
        final_label = "부정"
    else:
        final_label = "미정"

    return {
        "main_entity": main_entity,
        "final": {
            "label": final_label,
            "score": float(m["score"]),
            "confidence": float(m["confidence"]),
            "hits": int(m["hits"]),
        },
        "entity_scores": entity_scores,
        "main_evidence": m.get("sentences", []),
    }

# =========================
# 9.5) topic_name 생성 유틸 (동사/명사/키워드 템플릿)
# =========================
_PRED_IN_EVID_RE = re.compile(r"\[pred=([^\]]+)\]")

def extract_preds_from_evidence(evidence_sents: List[str]) -> List[str]:
    """
    evidence 문장에 붙은 [pred=...]에서 predicate 리스트를 뽑아 정규화해서 반환
    예: "… [pred=경고하다, 도입하다]" -> ["경고하다","도입하다"]
        "… [pred=비판하다(NEG)]" -> ["비판하다"]
    """
    out: List[str] = []
    for s in evidence_sents or []:
        m = _PRED_IN_EVID_RE.search(s)
        if not m:
            continue
        preds = m.group(1)
        for p in preds.split(","):
            p = p.strip()
            if not p:
                continue
            p = p.replace("(NEG)", "").strip()
            out.append(p)
    return out

def build_topic_name(
    *,
    entity: Optional[str],
    verb: Optional[str],
    keywords: List[str],
) -> str:
    """
    동사/명사/키워드 기반 템플릿 생성
    """
    kws = [k for k in (keywords or []) if str(k).strip()]
    k1 = kws[0] if len(kws) > 0 else ""
    k2 = kws[1] if len(kws) > 1 else ""

    if entity and verb and k1:
        return f"{entity}, {k1} 관련 ‘{verb}’ 공방"
    if entity and k1:
        return f"{entity} 관련 {k1} 이슈"
    if k1 and k2:
        return f"{k1}·{k2} 이슈"
    if k1:
        return f"{k1} 이슈"
    return "주요 이슈"

# =========================
# 10) topic doc builders
# =========================
def stance_intensity(score: float, conf: float, hits: int) -> float:
    return float(abs(score) * conf * math.log1p(max(0, hits)))

def reorder_cluster_features_by_hits(
    article_rows: List[Dict[str, Any]],
    cluster_keywords: Dict[int, List[str]],
) -> Dict[int, List[str]]:
    """
    cluster_keywords(=top_terms)를, 클러스터 내 기사 features에서의 hit 수 기준으로 재정렬
    """
    cluster_feat_hits: Dict[int, Counter] = defaultdict(Counter)
    for r in article_rows:
        cid = int(r["cluster_id"])
        feats = r.get("features") or []
        for f in feats:
            f = str(f).strip()
            if f:
                cluster_feat_hits[cid][f] += 1

    reordered: Dict[int, List[str]] = {}
    for cid, feats in cluster_keywords.items():
        cnt = cluster_feat_hits.get(int(cid), Counter())
        base_order = {t: i for i, t in enumerate(feats)}
        feats_sorted = sorted(feats, key=lambda t: (-cnt.get(t, 0), base_order.get(t, 10**9)))
        reordered[int(cid)] = feats_sorted
    return reordered

def build_topic_docs(
    article_rows: List[Dict[str, Any]],
    cluster_keywords: Dict[int, List[str]],
    cluster_sizes: Mapping[int, int],
    *,
    per_side_limit: int = 5,
) -> List[Dict[str, Any]]:
    by_topic: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for r in article_rows:
        by_topic[int(r["cluster_id"])].append(r)

    now_iso = datetime.now(KST).isoformat()
    out: List[Dict[str, Any]] = []

    for cid, rows in sorted(by_topic.items(), key=lambda x: x[0]):
        pos_list: List[Dict[str, Any]] = []
        neg_list: List[Dict[str, Any]] = []
        neu_list: List[Dict[str, Any]] = []
        pos = neg = neu = 0

        for r in rows:
            sc = float(r.get("stance_score") or 0.0)
            cf = float(r.get("confidence") or 0.0)
            hits = int(r.get("hits") or 0)

            item = {
                "article_id": r.get("article_id"),
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "stance_score": sc,
                "intensity": stance_intensity(sc, cf, hits),
            }

            if sc > 0:
                pos_list.append(item); pos += 1
            elif sc < 0:
                neg_list.append(item); neg += 1
            else:
                neu_list.append(item); neu += 1

        pos_list.sort(key=lambda x: x["intensity"], reverse=True)
        neg_list.sort(key=lambda x: x["intensity"], reverse=True)
        neu_list.sort(key=lambda x: x["intensity"], reverse=True)

        # ✅ NEW: cluster 대표 entity/verb 추출
        ent_cnt = Counter()
        verb_cnt = Counter()

        for r in rows:
            me = (r.get("main_entity") or "").strip()
            if me:
                ent_cnt[me] += 1

            preds = extract_preds_from_evidence(r.get("main_evidence") or [])
            for p in preds:
                if p:
                    verb_cnt[p] += 1

        top_entity = ent_cnt.most_common(1)[0][0] if ent_cnt else None
        top_verb = verb_cnt.most_common(1)[0][0] if verb_cnt else None

        topic_features = cluster_keywords.get(cid, [])
        topic_name = build_topic_name(
            entity=top_entity,
            verb=top_verb,
            keywords=topic_features[:3],
        )

        topic_doc = {
            "topic_id": str(cid),
            "topic_name": topic_name,  # ✅ NEW
            "topic_features": topic_features,
            "topic_article_count": int(cluster_sizes.get(cid, 0)),
            "topic_analyzed_count": int(len(rows)),
            "positive_articles": pos_list[:per_side_limit],
            "negative_articles": neg_list[:per_side_limit],
            "neutral_articles": neu_list[:per_side_limit],
            "stats": {"pos": pos, "neg": neg, "neutral": neu},
            "calculated_at": now_iso,
        }
        out.append(topic_doc)

    return out

def filter_topic_docs(
    topic_docs: List[Dict[str, Any]],
    *,
    min_cluster_size: int = MIN_CLUSTER_SIZE,
    require_both_sides: bool = REQUIRE_BOTH_SIDES,
    neutral_ratio_max: float = NEUTRAL_RATIO_MAX,
) -> List[Dict[str, Any]]:
    out = []
    for t in topic_docs:
        total = int(t.get("topic_article_count") or 0)
        st = t.get("stats") or {}
        pos = int(st.get("pos") or 0)
        neg = int(st.get("neg") or 0)
        neu = int(st.get("neutral") or 0)

        if total < min_cluster_size:
            continue

        if require_both_sides and (pos == 0 or neg == 0):
            continue

        denom = max(1, (pos + neg + neu))
        if (neu / denom) >= neutral_ratio_max:
            continue

        out.append(t)
    return out

# =========================
# 11) debug legacy json (예전 형식 + title/url 포함)
# =========================
def to_legacy_debug_json(topic_docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _expand(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for a in items or []:
            out.append({
                "article_id": a.get("article_id"),
                "title": a.get("title", ""),
                "url": a.get("url", ""),
                "score": float(a.get("stance_score") or 0.0),
                "intensity": float(a.get("intensity") or 0.0),
            })
        return out

    out_topics = []
    for t in topic_docs:
        out_topics.append({
            "topic_id": int(t["topic_id"]),
            "topic_name": t.get("topic_name", ""),  # ✅ NEW (debug에도 넣어줌)
            "topic_feature_keywords": t.get("topic_features", []),
            "positive_articles": _expand(t.get("positive_articles", [])),
            "negative_articles": _expand(t.get("negative_articles", [])),
            "neutral_articles": _expand(t.get("neutral_articles", [])),
            "calculated_at": t.get("calculated_at"),
        })
    return out_topics

# =========================
# 12) ES upsert (최소 nested만 저장)
# =========================
def _strip_article_fields_for_es(topic_doc: Dict[str, Any]) -> Dict[str, Any]:
    def _min_items(items):
        return [
            {
                "article_id": x.get("article_id"),
                "stance_score": float(x.get("stance_score") or 0.0),
                "intensity": float(x.get("intensity") or 0.0),
            }
            for x in (items or [])
        ]

    return {
        "topic_id": topic_doc.get("topic_id"),
        "topic_name": topic_doc.get("topic_name", ""),  # ✅ NEW
        "topic_features": topic_doc.get("topic_features", []),
        "topic_article_count": int(topic_doc.get("topic_article_count") or 0),
        "topic_analyzed_count": int(topic_doc.get("topic_analyzed_count") or 0),
        "positive_articles": _min_items(topic_doc.get("positive_articles")),
        "negative_articles": _min_items(topic_doc.get("negative_articles")),
        "neutral_articles": _min_items(topic_doc.get("neutral_articles")),
        "stats": topic_doc.get("stats", {}),
        "calculated_at": topic_doc.get("calculated_at"),
    }

def upsert_topic_docs_to_es(
    topic_docs: List[Dict[str, Any]],
    *,
    index_name: str = TOPIC_INDEX_NAME,
    id_field: str = "topic_id",
):
    actions = []
    for d in topic_docs:
        topic_part = str(d.get(id_field) or "").strip()
        if not topic_part:
            continue

        doc_id = f"{fmt}_{topic_part}"  # yyyymmdd_hh_topic_id
        doc_min = _strip_article_fields_for_es(d)

        actions.append({
            "_op_type": "update",
            "_index": index_name,
            "_id": doc_id,
            "doc": doc_min,
            "doc_as_upsert": True,
        })

    if not actions:
        print("[upsert_topic_docs_to_es] no actions")
        return

    helpers.bulk(es, actions, chunk_size=200, request_timeout=120)
    print(f"[upsert_topic_docs_to_es] upserted={len(actions)} into {index_name}")

# =========================
# 13) main pipeline
# =========================
def _extract_entities_from_source(src: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    ent = src.get("entities") or {}
    persons = ent.get("person") or src.get("persons") or []
    orgs = ent.get("org") or src.get("organizations") or []
    if not isinstance(persons, list):
        persons = [persons]
    if not isinstance(orgs, list):
        orgs = [orgs]
    return persons, orgs

def label_polar_entity_centered_to_topics_json(
    *,
    index_name: str = "article_data",
    output_path_topics: str = DEFAULT_OUTPUT_PATH,
    debug_output_path: str = DEBUG_OUTPUT_PATH,
    fetch_size: int = 500,
    predicate_lexicon_path: str = r"data/predicate_lexicon.json",
    per_side_limit: int = 5,
    # filters
    min_cluster_size: int = MIN_CLUSTER_SIZE,
    require_both_sides: bool = REQUIRE_BOTH_SIDES,
    neutral_ratio_max: float = NEUTRAL_RATIO_MAX,
    # es save
    save_to_es: bool = True,
    topic_index_name: str = TOPIC_INDEX_NAME,
) -> List[Dict[str, Any]]:
    # 1) topic clustering
    raw = create_topic(index_name=index_name, size=TOPIC_FETCH_SIZE, title_boost=TITLE_BOOST)
    article_ids = raw.get("article_ids", [])
    labels = raw.get("labels", [])
    cluster_keywords = raw.get("cluster_keywords", {})

    cluster_sizes = Counter(labels)

    labeled = [
        {"article_id": aid, "cluster_id": cid}
        for aid, cid in zip(article_ids, labels)
        if aid is not None and cid is not None and cid >= 0
    ]
    cluster_ids = sorted({d["cluster_id"] for d in labeled})
    if not cluster_ids:
        print("[label_polar_entity_centered_to_topics_json] No clusters to process.")
        return []

    # 2) predicate lexicon
    pred_dist = load_predicate_lexicon(predicate_lexicon_path)
    print("[predicate_lexicon] size:", len(pred_dist))

    # 3) article stance
    all_rows: List[Dict[str, Any]] = []
    total_es_hits = 0

    for cid in cluster_ids:
        a_ids = [d["article_id"] for d in labeled if d["cluster_id"] == cid]
        if not a_ids:
            continue

        query = {
            "_source": [
                "article_id", "article_title", "article_content", "url",
                "press", "upload_date",
                "features",
                "entities", "persons", "organizations",
            ],
            "size": min(fetch_size, len(a_ids)),
            "query": {"terms": {"article_id": a_ids}},
        }

        resp = es.search(index=index_name, body=query)
        hits = resp.get("hits", {}).get("hits", [])
        total_es_hits += len(hits)

        for h in hits:
            src = h.get("_source", {})

            title = (src.get("article_title") or "").strip()
            url = src.get("url") or ""

            content = src.get("article_content") or ""
            if isinstance(content, list):
                content = " ".join(content)
            text = f"{title}\n\n{content}".strip()

            persons, orgs = _extract_entities_from_source(src)

            out = label_text_by_entities_kiwi(
                text=text,
                persons=persons,
                orgs=orgs,
                pred_dist=pred_dist,
                max_char_distance=MAX_CHAR_DISTANCE,
                min_entity_hits=MIN_ENTITY_HITS,
                max_example_sents=MAX_EXAMPLE_SENTS,
            )

            feats = src.get("features") or []
            if not isinstance(feats, list):
                feats = [str(feats)]
            feats = [str(x).strip() for x in feats if str(x).strip()]

            all_rows.append({
                "cluster_id": int(cid),
                "article_id": src.get("article_id"),
                "title": title,
                "url": url,
                "stance_score": out["final"]["score"],
                "confidence": out["final"]["confidence"],
                "hits": out["final"]["hits"],
                "features": feats,

                # ✅ topic_name 재료
                "main_entity": out.get("main_entity"),
                "main_evidence": out.get("main_evidence", []),
            })

    # 4) cluster feature: hit 많은 순으로 재정렬
    cluster_keywords = reorder_cluster_features_by_hits(all_rows, cluster_keywords)

    # 5) topic docs 생성 (ES 필드명 + topic_name 포함)
    topic_docs = build_topic_docs(
        all_rows,
        cluster_keywords,
        cluster_sizes,
        per_side_limit=per_side_limit,
    )

    # 6) 필터 적용
    topic_docs = filter_topic_docs(
        topic_docs,
        min_cluster_size=min_cluster_size,
        require_both_sides=require_both_sides,
        neutral_ratio_max=neutral_ratio_max,
    )

    # 7) JSON 저장 (ES 저장용 구조)
    with open(output_path_topics, "w", encoding="utf-8") as f:
        json.dump(topic_docs, f, ensure_ascii=False, indent=2)

    # 8) 확인용 JSON 저장 (예전 형식 + title/url + topic_name)
    legacy_debug = to_legacy_debug_json(topic_docs)
    with open(debug_output_path, "w", encoding="utf-8") as f:
        json.dump(legacy_debug, f, ensure_ascii=False, indent=2)

    # 9) 콘솔 샘플 출력
    print("\n[DEBUG] legacy topic json sample (first 2 topics)")
    print(json.dumps(legacy_debug[:2], ensure_ascii=False, indent=2))

    print("[OK] saved:")
    print(f" - topics(es_schema): {output_path_topics}")
    print(f" - topics(debug_legacy): {debug_output_path}")
    print(f" - total_es_hits: {total_es_hits}")
    print(f" - topic_docs(after filter): {len(topic_docs)}")

    # 10) ES 저장(upsert) (topic_name 포함)
    if save_to_es:
        upsert_topic_docs_to_es(topic_docs, index_name=topic_index_name)

    return topic_docs

if __name__ == "__main__":
    label_polar_entity_centered_to_topics_json()

from datetime import datetime, timezone, timedelta
from util.elastic import es
from util.database import SessionLocal
from sqlalchemy import text
from util.logger import Logger

import numpy as np
import json
import random

"""
[안전 버전 패치 포인트 요약]

1. 추천 입력 기사 = status=5 만 사용
2. article_label 항상 .get() 접근
3. 트렌드 / 신뢰도 점수 None / 누락 방어
4. 새 환경에서도 KeyError 발생 불가
"""

logger = Logger().get_logger(__name__)
KST = timezone(timedelta(hours=9))


def _soft_shuffle_topk(ranked, top_k=12, strength=1.0):
    """
    top_k: 섞을 상위 구간
    strength: 0.0이면 거의 점수순, 1.0~2.0이면 변동성 증가
              (점수 차이가 클수록 상위가 더 자주 유지됨)
    """
    if len(ranked) <= 1:
        return ranked

    k = min(top_k, len(ranked))
    head = ranked[:k]
    tail = ranked[k:]

    # 점수 높은 애가 앞에 더 자주 오도록:
    # 1) head에서 하나 뽑고
    # 2) 뽑은 애 제거
    # 3) 반복 (without replacement)
    # 가중치는 score^strength 사용
    out = []
    pool = head[:]
    while pool:
        weights = [(max(1, x["final_score"]) ** strength) for x in pool]
        pick = random.choices(pool, weights=weights, k=1)[0]
        out.append(pick)
        pool.remove(pick)

    return out + tail

# -------------------------------------------------
# 유저 임베딩 업데이트 (기존 유지)
# -------------------------------------------------
def update_user_embedding(user_id):
    db = SessionLocal()

    article_row = db.execute(
        text("""
            SELECT sd.article_id, ps.preference_score
            FROM session_data sd
            JOIN preference_score ps ON ps.session_id = sd.session_id
            WHERE sd.user_id = :uid
            ORDER BY ps.occurred_at DESC
            LIMIT 1
        """),
        {"uid": user_id}
    ).fetchone()

    if not article_row:
        return

    article_id, preference_score = article_row
    preference_score = float(preference_score)

    resp = es.search(
        index="article_data",
        body={
            "_source": ["article_embedding"],
            "query": {"term": {"article_id": article_id}}
        }
    )

    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        return

    article_embedding = hits[0]["_source"].get("article_embedding")
    if not article_embedding or len(article_embedding) != 768:
        raise RuntimeError(f"Invalid article_embedding article_id={article_id}")

    old_emb = np.asarray(article_embedding, dtype=np.float32)

    resp = es.search(
        index="user_embeddings",
        body={
            "_source": ["embedding"],
            "query": {"term": {"user_id": user_id}}
        }
    )

    hits = resp.get("hits", {}).get("hits", [])

    if not hits:
        es.index(
            index="user_embeddings",
            id=user_id,
            document={
                "user_id": user_id,
                "embedding": old_emb.tolist(),
                "updated_at": datetime.now(KST)
            }
        )
        # [LOG-4A] 최초 개인화 시점
        logger.info(f"[EMB UPDATE] user_id={user_id} CREATE")
        return

    user_embedding = hits[0]["_source"].get("embedding")
    if not user_embedding or len(user_embedding) != 768:
        raise RuntimeError(f"Invalid user_embedding user_id={user_id}")

    new_emb = np.asarray(user_embedding, dtype=np.float32)
    updated_embedding = 0.9 * old_emb + 0.1 * preference_score * new_emb

    es.update(
        index="user_embeddings",
        id=user_id,
        body={
            "doc": {
                "embedding": updated_embedding.tolist(),
                "updated_at": datetime.now(KST)
            }
        }
    )
    # [LOG-4B] 개인화 누적 반영
    logger.info(f"[EMB UPDATE] user_id={user_id} UPDATE")


# -------------------------------------------------
# 유저 기사 조회 (기존 유지)
# -------------------------------------------------
def user_articles(user_id):
    resp = es.search(
        index="user_embeddings",
        body={
            "_source": ["embedding"],
            "query": {"term": {"user_id": user_id}}
        }
    )

    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        return []

    query_vec = hits[0]["_source"]["embedding"]

    res = es.search(
        index="article_data",
        size=20,
        knn={
            "field": "article_embedding",
            "query_vector": query_vec,
            "k": 1000,
            "num_candidates": 2000,
            "filter": [
                {"term": {"status": 5}},
                {"range": {"collected_at": {"gte": "now-3d"}}}
            ]
        },
        _source=["article_id", "article_title", "collected_at"]
    )

    return [
        {
            "article_id": h["_source"].get("article_id"),
            "title": h["_source"].get("article_title"),
            "score": h["_score"],
            "collected_at": h["_source"].get("collected_at"),
        }
        for h in res.get("hits", {}).get("hits", [])
    ]

def get_similar_users_mean_embedding(
    vec: list,
    top_k: int = 5,
):
    if not vec or len(vec) != 768:
        return None

    # 2. 유사 유저 kNN 검색 (본인 제외는 embedding 동일성으로 간접 처리)
    res = es.search(
        index="user_embeddings",
        size=top_k,
        knn={
            "field": "embedding",
            "query_vector": vec,
            "k": top_k,
            "num_candidates": 100,
        },
        _source=["embedding"],
    )

    hits = res.get("hits", {}).get("hits", [])
    if not hits:
        return None

    emb_list = []

    for h in hits:
        emb = (h.get("_source") or {}).get("embedding")
        if not emb or len(emb) != 768:
            continue

        # 3. 🔥 본인 embedding 제외 (완전 동일 벡터 방어)
        if np.allclose(emb, vec, atol=1e-6):
            continue
        emb_list.append(np.asarray(emb, dtype=np.float32))

    if not emb_list:
        return None

    # 4. 평균 임베딩 계산 + 정규화
    mean_emb = np.mean(emb_list, axis=0)
    norm = np.linalg.norm(mean_emb)

    if norm == 0:
        return None
    return mean_emb / norm

def dedupe_hits(base_hits: list, item_hits: list) -> list:
    """
    base_hits + item_hits 를 article_id 기준으로 병합
    - base_hits 우선
    - item_hits는 base에 없는 기사만 추가
    - 입력 hit 구조 그대로 유지 (_score 포함)

    return: deduped hits list
    """
    seen = set()
    merged = []

    # 1) base 후보 먼저
    for h in base_hits or []:
        src = h.get("_source", {})
        aid = src.get("article_id")
        if not aid:
            continue
        if aid in seen:
            continue

        seen.add(aid)
        merged.append(h)

    # 2) item-based 후보 추가
    for h in item_hits or []:
        src = h.get("_source", {})
        aid = src.get("article_id")
        if not aid:
            continue
        if aid in seen:
            continue

        seen.add(aid)
        merged.append(h)

    return merged


def recommend_articles(user_id: str, limit: int = 20,random: bool = False):
    """
    유저별 추천 기사 생성 (안전 버전)

    - user_embeddings 인덱스 없을 때도 절대 에러 안 남
    - status=5 기사만 추천
    - article_label 누락 완전 방어
    """
    # -------------------------------------------------
    # 1. 유저 임베딩 존재 여부 확인 (🔥 핵심)
    # -------------------------------------------------
    if not es.indices.exists(index="user_embeddings"):
        has_user_embedding = False
        user_hits = []
    else:
        resp = es.search(
            index="user_embeddings",
            body={
                "_source": ["embedding"],
                "query": {"term": {"user_id": user_id}}
            }
        )
        user_hits = resp.get("hits", {}).get("hits", [])
        has_user_embedding = len(user_hits) > 0
        # [LOG-1] 콜드스타트 / 개인화 분기 확인
        logger.info(
            f"[RECOMMEND] user_id={user_id} has_user_embedding={has_user_embedding}"
        )
    # -------------------------------------------------
    # 2. 후보 기사 조회
    # -------------------------------------------------
    if has_user_embedding:
        query_vec = user_hits[0]["_source"]["embedding"]
        res_base = es.search(
            index="article_data",
            size=100,
            knn={
                "field": "article_embedding",
                "query_vector": query_vec,
                "k": 100,
                "num_candidates": 500,
                "filter": [
                    {"term": {"status": 5}},
                    {"range": {"collected_at": {"gte": "now-3d"}}}
                ]
            },
            _source=[
                "article_id",
                "article_title",
                "article_label",
                "collected_at",
                "article_img",
                "article_content",    #  본문  해정 개인화 페이지 추가
                "reporter",           #  기자명
                "press",              #  언론사
                "upload_date"         #  업로드 날짜
            ]
        )

        similar_user_vec = get_similar_users_mean_embedding(query_vec)
        res_item = es.search(
            index="article_data",
            size=200,
            knn={
                "field": "article_embedding",
                "query_vector": similar_user_vec,
                "k": 200,
                "num_candidates": 1000,
                "filter": [
                    {"term": {"status": 5}},
                    {"range": {"collected_at": {"gte": "now-3d"}}}
                ]
            },
            _source=[
                "article_id",
                "article_title",
                "article_label",
                "collected_at",
                "article_img",
                "article_content", #해정 추가
                "reporter",
                "press",
                "upload_date"
            ]
        )

        base_hits = res_base.get("hits", {}).get("hits", [])
        item_hits = res_item.get("hits", {}).get("hits", [])

        # hits = dedupe_hits(base_hits, item_hits)
        hits = base_hits
        if not hits:
            return []
    else:
        res = es.search(
            index="article_data",
            size=100,
            query={
                "bool": {
                    "must": [
                        {"term": {"status": 5}},
                        {"range": {"collected_at": {"gte": "now-3d"}}}
                    ]
                }
            },
            sort=[{"article_label.trend_score": {"order": "desc"}}],
            _source=[
                "article_id",
                "article_title",
                "article_label",
                "collected_at",
                "article_img",
                "article_content", # 해정 추가
                "reporter",
                "press",
                "upload_date"
            ]
        )

        hits = res.get("hits", {}).get("hits", [])
        if not hits:
            return []
    filtered_hits = []

    for h in hits:
        src = h.get("_source", {})
        title = src.get("article_title", "").strip()

        # 1. 제목 너무 짧은 경우 제거
        if len(title) < 12:
            continue

        filtered_hits.append(h)

    hits = filtered_hits
    # [LOG-2] 추천 후보 수 확인 (kNN / 필터 정상 여부)
    logger.info(
        f"[RECOMMEND] user_id={user_id} candidate_hits={len(hits)}"
    )

    if not hits:
        return []
    # -------------------------------------------------
    # 3. 점수 범위 계산
    # -------------------------------------------------
    trend_scores = []
    trust_scores = []

    for h in hits:
        label = h["_source"].get("article_label", {})
        trend_scores.append(label.get("trend_score", 0.0))
        trust_scores.append(label.get("article_trust_score", 0.0))

    trend_min, trend_max = min(trend_scores), max(trend_scores)
    trust_min, trust_max = min(trust_scores), max(trust_scores)

    if has_user_embedding:
        emb_scores = [h["_score"] for h in hits]
        emb_min, emb_max = min(emb_scores), max(emb_scores)
    else:
        emb_min = emb_max = None

    def normalize(v, mn, mx):
        if mx == mn:
            return 0.0
        return (v - mn) / (mx - mn)

    # -------------------------------------------------
    # 4. 최종 점수 계산
    # -------------------------------------------------
    ranked = []

    for h in hits:
        src = h["_source"]
        label = src.get("article_label", {})

        # 🔥 트렌드 점수: 항상 0~1 범위 → 100배
        raw_trend = label.get("trend_score")
        if raw_trend is None:
            trend_score = 0
        else:
            trend_score = round(float(raw_trend) * 100)  # 0.74 → 74

        # 🔥 신뢰도 점수: 이미 1~100 범위 → 반올림만
        raw_trust = label.get("article_trust_score")
        if raw_trust is None:
            trust_score = 0
        else:
            trust_score = round(float(raw_trust))  # 68.57 → 69
        #----------------------------------------------------해정
        trend = normalize(label.get("trend_score", 0.0), trend_min, trend_max)
        trust = normalize(label.get("article_trust_score", 0.0), trust_min, trust_max)

        if has_user_embedding:
            emb = normalize(h["_score"], emb_min, emb_max)
            final_raw = 0.6 * emb + 0.2 * trend + 0.2 * trust
        else:
            final_raw = 0.7 * trend + 0.3 * trust

        ranked.append({
            "article_id": src.get("article_id"),
            "title": src.get("article_title", ""),
            "article_img": src.get("article_img"),
            "final_score": int(round(final_raw * 100)),
            "collected_at": src.get("collected_at"),
            # 추가 필드 해정 추가
            "content": src.get("article_content", ""),
            "reporter": src.get("reporter", ""),
            "press": src.get("press", ""),
            "category": label.get("category", "기타"),
            "upload_date": src.get("upload_date"),
            # ✅ 점수 추가
            "trend_score": label.get("trend_score", 0.0),
            "trust_score": label.get("article_trust_score", 0.0),
        })

    ranked.sort(key=lambda x: x["final_score"], reverse=True)
    # [LOG-3] 최종 추천 결과 (체감 확인용 핵심 로그)
    logger.info(
        f"[RECOMMEND RESULT] user_id={user_id} "
        f"top_ids={[r['article_id'] for r in ranked[:limit]]}"
    )
    if not random:
        return ranked[:limit]
    else:
        return _soft_shuffle_topk(ranked, top_k=12, strength=1.2)[:limit]


if __name__ == "__main__":
    logger.info(
        json.dumps(
            recommend_articles("test_user"),
            ensure_ascii=False,
            indent=2
        )
    )
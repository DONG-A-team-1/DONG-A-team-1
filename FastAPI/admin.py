import base64
import json
from datetime import datetime, timezone, timedelta
from typing import Any, Optional, Tuple, List, Dict, Set

from sqlalchemy import text
from util.database import SessionLocal
from util.elastic import es
from util.logger import Logger

from .article import get_article_from_es

logger = Logger().get_logger(__name__)
KST = timezone(timedelta(hours=9))


# 페이지네이션 관련 함수
def encode_cursor(search_after: Optional[List[Any]]) -> Optional[str]:
    if not search_after:
        return None
    raw = json.dumps(search_after, ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")
def decode_cursor(cursor: Optional[str]) -> Optional[List[Any]]:
    if not cursor:
        return None
    raw = base64.urlsafe_b64decode(cursor.encode("ascii"))
    return json.loads(raw.decode("utf-8"))


# 전용 유틸 함수
def safe_dt(iso_ts: Optional[str]) -> Optional[datetime]:
    if not iso_ts:
        return None
    try:
        s = str(iso_ts).strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None
def format_timestamp(iso_ts: Optional[str]) -> str:
    dt = safe_dt(iso_ts)
    if not dt:
        return ""
    return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")
def to_epoch_ms(iso_ts: Optional[str]) -> int:
    dt = safe_dt(iso_ts)
    if not dt:
        return 0
    return int(dt.timestamp() * 1000)
def _ensure_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]

# 전체 로그 목록 및 로그 데이터 포맷팅  및 정규화 함수
def parse_admin_log_doc(doc: dict) -> dict:
    _src = doc.get("_source", {}) or {}
    idx = doc.get("_index", "") or ""
    ts = _src.get("@timestamp")

    if "error_log" in idx:
        samples = _src.get("samples") or []
        stack = samples[0] if samples else ""
        return {
            "id": doc.get("_id"),  # ✅ 식별은 _id로 (정렬 아님)
            "ts": to_epoch_ms(ts),
            "time": format_timestamp(ts),
            "type": "error",
            "source": (_src.get("log") or {}).get("logger", ""),
            "message": _src.get("message", ""),
            "stack": stack or "스택 트레이스 없음",
        }

    # info_logs
    return {
        "id": doc.get("_id"),
        "ts": to_epoch_ms(ts),
        "time": format_timestamp(ts),
        "type": normalize_log_type((_src.get("pipeline") or {}).get("status")),
        "source": (_src.get("trace") or {}).get("job_id", ""),
        "message": (_src.get("pipeline") or {}).get("message", ""),
        "stack": "",
    }
def normalize_log_type(raw_type: Optional[str]) -> str:
    if not raw_type:
        return "info"
    t = str(raw_type).lower()
    mapping = {
        "error": "error", "err": "error", "exception": "error", "fatal": "error", "critical": "error",
        "warning": "warning", "warn": "warning",
        "info": "info", "information": "info", "debug": "info", "trace": "info",
        "success": "success", "ok": "success", "passed": "success", "complete": "success", "completed": "success",
    }
    return mapping.get(t, "info")
def get_admin_logs_page(size: int = 200, cursor: Optional[str] = None) -> Tuple[list, Optional[str], bool]:
    """
    info_logs + error_log 합쳐서 @timestamp desc 기준으로 search_after paging
    tie-break: _shard_doc (✅ _id 정렬 금지 이슈 회피)
    """
    search_after = decode_cursor(cursor)

    body = {
        "size": size,
        "sort": [
            {"@timestamp": {"order": "desc"}},
            {"_shard_doc": {"order": "desc"}},  # ✅ _id 대신
        ],
        "_source": True,
    }
    if search_after:
        body["search_after"] = search_after

    src = es.search(index="info_logs,error_log", body=body)
    hits = src.get("hits", {}).get("hits", []) or []

    items = [parse_admin_log_doc(h) for h in hits]

    next_cursor = None
    if hits:
        next_cursor = encode_cursor(hits[-1].get("sort"))

    has_more = len(hits) == size
    return items, next_cursor, has_more
def denormalize_category(category: str) -> str:
    if not category:
        return ""

    category = category.lower().strip()
    return {
        "society": "사회/경제/산업",
        "politics": "정치",
        "world": "국제",
        "culture": "문화",
        "sports": "스포츠",
        "local": "지역",
    }.get(category, category)

# 전체 기사 목록 및 기사 데이터 포맷팅 함수
def parse_admin_article_doc(doc: dict) -> dict:
    _src = doc.get("_source", {}) or {}
    al = _src.get("article_label") or {}
    ts = _src.get("collected_at")

    labels = al.get("labels", None)
    if labels is None:
        labels = al.get("label", None)
    if labels is None:
        labels = []
    if not isinstance(labels, list):
        labels = [labels]

    return {
        "id": _src.get("article_id"),
        "title": _src.get("article_title"),
        "category": al.get("category"),
        "labels": labels,
        "trust_score": al.get("article_trust_score"),
        "date": format_timestamp(ts),
        "ts": to_epoch_ms(ts),
        "status": _src.get("status"),
    }
def get_admin_articles_page(size: int = 200, cursor: Optional[str] = None) -> Tuple[list, Optional[str], bool]:
    """
    article_data collected_at desc 기준 search_after paging
    tie-break: _shard_doc
    """
    search_after = decode_cursor(cursor)

    body = {
        "_source": ["article_id", "article_title", "collected_at", "article_label","status"],
        "size": size,
        "sort": [
          { "collected_at": { "order": "desc" } },
          { "article_id": { "order": "desc" } }
        ]
    }
    if search_after:
        body["search_after"] = search_after

    src = es.search(index="article_data", body=body)
    hits = src.get("hits", {}).get("hits", []) or []

    items = [parse_admin_article_doc(h) for h in hits]
    # logger.info(items)
    next_cursor = None
    if hits:
        next_cursor = encode_cursor(hits[-1].get("sort"))

    has_more = len(hits) == size
    return items, next_cursor, has_more

# 기사 편집
def hide_articles(article_id:str, status):
    new_status = int(0) if status == 0 else 5
    es.update(
        index="article_data",
        id=article_id,
        body={"doc": {"status": int(new_status)}},
        refresh=True,  # 관리자 즉시 반영 원하면
    )
    # logger.info("article status change: %s → %s", status, new_status)
    return {"success": True, "article_id": article_id, "status": new_status}
def article_detail(article_id:str):
    source = ["article_content"]
    res = get_article_from_es(article_id,source)
    if not res:
        return {"success": False, "article_id": article_id}
    else:
        return res[0]
def edit_articles(article_id: str, body):
    try:
        params = {}
        script_lines = []

        if body.title is not None:
            params["title"] = body.title
            script_lines.append("ctx._source.article_title = params.title;")

        if body.content is not None:
            params["content"] = body.content
            script_lines.append("ctx._source.article_content = params.content;")

        if body.category is not None:
            params["category"] = body.category
            # ✅ article_label 없으면 생성하고 category만 갱신
            script_lines.append("""
              if (ctx._source.article_label == null) { ctx._source.article_label = new HashMap(); }
              ctx._source.article_label.category = params.category;
            """)

        if not script_lines:
            return {"success": False, "message": "No fields to update", "article_id": article_id}

        es.update(
            index="article_data",
            id=article_id,
            body={
                "script": {
                    "lang": "painless",
                    "source": "\n".join(script_lines),
                    "params": params
                }
            },
            refresh=True
        )

        return {
            "success": True,
            "article_id": article_id,
            "updated": list(params.keys())
        }
    except Exception:
        logger.exception("edit_articles failed")
        return {"success": False, "article_id": article_id}

# 전체 유저 목록
def get_admin_users():
    db = SessionLocal()
    try:
        rows = db.execute(
            text("""
                SELECT
                    ui.user_id,
                    ui.user_name,
                    ua.user_email,
                    ua.joined_at,
                    ua.is_active,
                    sd.last_active
                FROM user_auth ua
                JOIN user_info ui
                    ON ua.user_id = ui.user_id
                LEFT JOIN (
                    SELECT
                        user_id,
                        MAX(started_at) AS last_active
                    FROM session_data
                    GROUP BY user_id
                ) sd
                    ON ua.user_id = sd.user_id
            """)
        ).fetchall()

        users = [
            {
                "id": r.user_id,
                "name": r.user_name,
                "email": r.user_email,
                "joinDate": r.joined_at.strftime("%Y-%m-%d") if r.joined_at else None,
                "lastActive": r.last_active.strftime("%Y-%m-%d %H:%M") if r.last_active else None,
                "is_active": int(r.is_active) if r.is_active is not None else 0,
                "status": "active" if r.is_active == 1 else "inactive",
            }
            for r in rows
        ]
        return users
    finally:
        db.close()

# 유저 비활성/활성화
def toggle_users(user_id: str):
    db = SessionLocal()
    try:
        cur = db.execute(
            text("SELECT is_active FROM user_auth WHERE user_id=:user_id"),
            {"user_id": user_id}
        ).scalar()
        cur = int(cur or 0)
        new_val = 0 if cur == 1 else 1

        db.execute(
            text("UPDATE user_auth SET is_active=:v WHERE user_id=:user_id"),
            {"v": new_val, "user_id": user_id}
        )
        db.commit()
        return {"success": True, "user_id": user_id, "is_active": int(new_val)}
    finally:
        db.close()


def _parse_topic_hits(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    ES hit의 _id를 _source에 합쳐서 반환.
    -> 이후 로직에서 t["_id"]로 접근 가능
    """
    hits = resp.get("hits", {}).get("hits", [])
    out = []
    for h in hits:
        s = h.get("_source", {}) or {}
        s["_id"] = h.get("_id")          # ✅ 핵심: ES 문서 _id 보존
        out.append(s)
    return out


def _collect_article_ids(topics: List[Dict[str, Any]]) -> List[str]:
    ids: Set[str] = set()
    for t in topics:
        for a in t.get("positive_articles", []) or []:
            if a.get("article_id"):
                ids.add(a["article_id"])
        for a in t.get("negative_articles", []) or []:
            if a.get("article_id"):
                ids.add(a["article_id"])
        for a in t.get("neutral_articles", []) or []:
            if a.get("article_id"):
                ids.add(a["article_id"])
    return list(ids)


def _fetch_articles_map(
    article_ids: List[str],
    article_index: str = "article_data",
    source_fields: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    if not article_ids:
        return {}

    if source_fields is None:
        source_fields = ["article_id", "article_title", "url", "press"]

    body = {
        "_source": source_fields,
        "size": min(len(article_ids), 10000),
        "query": {"terms": {"article_id": article_ids}},
    }

    resp = es.search(index=article_index, body=body)
    hits = resp.get("hits", {}).get("hits", [])

    m: Dict[str, Dict[str, Any]] = {}
    for h in hits:
        s = h.get("_source", {}) or {}
        aid = s.get("article_id")
        if not aid:
            continue
        title = s.get("article_title") or s.get("title") or ""
        m[aid] = {
            "id": aid,
            "title": title,
            "url": s.get("url"),
            "press": s.get("press"),
        }
    return m


def get_admin_topics(
    topic_index: str = "topic_polarity",
    article_index: str = "article_data",
    limit: int = 200,
    calculated_after: Optional[str] = None,
) -> List[Dict[str, Any]]:

    query: Dict[str, Any] = {"match_all": {}}
    if calculated_after:
        query = {"range": {"calculated_at": {"gte": calculated_after}}}

    body = {
        "size": limit,
        "sort": [{"rank": {"order": "asc"}}, {"calculated_at": {"order": "desc"}}],
        "query": query,
        "_source": [
            "topic_id",
            "topic_name",
            "rank",
            "positive_articles.article_id",
            "positive_articles.intensity",
            "negative_articles.article_id",
            "negative_articles.intensity",
            "neutral_articles.article_id",
            "neutral_articles.intensity",
            "calculated_at",
        ],
    }

    resp = es.search(index=topic_index, body=body)
    topics = _parse_topic_hits(resp)   # ✅ 이제 topic["_id"] 포함됨

    article_ids = _collect_article_ids(topics)
    articles_map = _fetch_articles_map(article_ids, article_index=article_index)

    out: List[Dict[str, Any]] = []

    for t in topics:
        es_id = t.get("_id")  # ✅ ES 문서 _id
        topic_id = t.get("topic_id")
        topic_name = t.get("topic_name") or ""
        calculated_at = t.get("calculated_at")

        # date: 'YYYY-MM-DD'
        date_str = None
        if calculated_at:
            if isinstance(calculated_at, str):
                date_str = calculated_at[:10]
            else:
                try:
                    date_str = datetime.fromtimestamp(calculated_at / 1000).strftime("%Y-%m-%d")
                except Exception:
                    date_str = None

        merged_articles: List[Dict[str, Any]] = []

        for a in t.get("positive_articles", []) or []:
            aid = a.get("article_id")
            if not aid:
                continue
            meta = articles_map.get(aid, {"id": aid, "title": ""})
            merged_articles.append({
                "id": aid,
                "title": meta.get("title", ""),
                "sentiment": "positive",
            })

        for a in t.get("negative_articles", []) or []:
            aid = a.get("article_id")
            if not aid:
                continue
            meta = articles_map.get(aid, {"id": aid, "title": ""})
            merged_articles.append({
                "id": aid,
                "title": meta.get("title", ""),
                "sentiment": "negative",
            })

        for a in t.get("neutral_articles", []) or []:
            aid = a.get("article_id")
            if not aid:
                continue
            meta = articles_map.get(aid, {"id": aid, "title": ""})
            merged_articles.append({
                "id": aid,
                "title": meta.get("title", ""),
                "sentiment": "neutral",
            })

        # ✅ 핵심 변경:
        # - 프론트 ID 컬럼을 ES _id로 쓰고 싶으면 "id"를 es_id로 내려준다.
        # - topic_id(숫자)는 별도 필드로 보존해두면 운영/디버깅에 좋음.
        out.append({
            "id": es_id,            # ✅ 화면 ID = ES _id
            "topic_id": topic_id,   # ✅ 내부 비즈니스 ID도 보존(원하면 프론트 숨김)
            "name": topic_name,
            "articles": merged_articles,
            "date": date_str,
        })
    return out

def edit_topics(topic_id, body):
    q = {
        "size": 1,
        "query": {"term": {"topic_id.keyword": topic_id}}  # keyword 없으면 "topic_id"로
    }
    resp = es.search(index="topic_polarity", body=q)
    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        return {"ok": False, "topic_id": topic_id, "updated": False, "reason": "topic not found in topic_data"}

    doc_id = hits[0]["_id"]

    update_doc: Dict[str, Any] = {}

    # ✅ 2) name 변경
    if getattr(body, "name", None) is not None:
        update_doc["topic_name"] = body.name

    # ✅ 3) articles 최종 상태 반영 → 3그룹 재구성
    if getattr(body, "articles", None) is not None:
        pos, neg, neu = [], [], []
        for a in body.articles:
            # a가 pydantic이면 a.article_id / a.sentiment
            # a가 dict이면 a["article_id"] / a["sentiment"]
            article_id = getattr(a, "article_id", None) or (a.get("article_id") if isinstance(a, dict) else None)
            sentiment = getattr(a, "sentiment", None) or (a.get("sentiment") if isinstance(a, dict) else None)

            if not article_id:
                continue

            item = {"article_id": str(article_id)}

            if sentiment == "positive":
                pos.append(item)
            elif sentiment == "negative":
                neg.append(item)
            else:
                neu.append(item)

        update_doc["positive_articles"] = pos
        update_doc["negative_articles"] = neg
        update_doc["neutral_articles"] = neu

    if not update_doc:
        return {"ok": True, "topic_id": topic_id, "updated": False, "reason": "no changes"}

    # ✅ 4) ES update: index도 topic_data로 통일
    es.update(
        index="topic_polarity",
        id=doc_id,
        body={"doc": update_doc},
        refresh=True
    )
    return {"ok": True, "topic_id": topic_id, "updated": True, "doc_id": doc_id}
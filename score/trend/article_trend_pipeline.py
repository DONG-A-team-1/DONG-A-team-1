# 기사별 최종 Trend Score 파이프라인

from datetime import datetime, timedelta, timezone
from elasticsearch import helpers
from util.elastic import es
from util.logger import Logger

# 외부 트렌드 파이프라인
from score.trend.google_trends_ES import crawl_trends
from score.trend.bigkinds_trends_ES import run_bigkinds_trend_pip

logger = Logger().get_logger(__name__)
KST = timezone(timedelta(hours=9))

ARTICLE_INDEX = "article_data"
TRENDING_INDEX = "trending_articles"
BIGKINDS_INDEX = "bigkinds_trends"
GOOGLE_INDEX = "google_trends"


# -------------------------------------------------
# 최신 BigKinds 트렌드 dict 로드
# 최신 문서를 확실히 가져오기 위해 sort 추가
# -------------------------------------------------
def load_latest_bigkinds_trend_dict():
    res = es.search(
        index=BIGKINDS_INDEX,
        body={
            "size": 1,
            "sort": [{"collected_at": "desc"}],
            "_source": ["trends"]
        }
    )

    hits = res.get("hits", {}).get("hits", [])
    if not hits:
        logger.warning("[TREND] BigKinds trend empty")
        return {}

    trends = hits[0].get("_source", {}).get("trends", [])

    result = {}
    for t in trends:
        title = t.get("title")
        score = t.get("trend_score")
        if title is not None and score is not None:
            result[title] = float(score)

    return result


# -------------------------------------------------
# 최신 Google 트렌드 dict 로드
# -------------------------------------------------
def load_latest_google_trend_dict():
    res = es.search(
        index=GOOGLE_INDEX,
        body={
            "size": 1,
            "sort": [{"collected_at": "desc"}],
            "_source": ["trends"]
        }
    )

    hits = res.get("hits", {}).get("hits", [])
    if not hits:
        logger.warning("[TREND] Google trend empty")
        return {}

    trend_list = hits[0]["_source"]["trends"]
    return {t["title"]: t["trend_score"] for t in trend_list}


# -------------------------------------------------
# 기사별 트렌드 점수 계산
# -------------------------------------------------
def calc_article_trend_score(keywords, google_dict, bigkinds_dict):
    """
    기사 features 기준 트렌드 점수 계산

    Google 0.5 + BigKinds 0.5 고정 가중치
    - 하나만 있으면 최대 0.5
    - 둘 다 있으면 최대 1.0
    - 둘 다 없으면 0
    """

    google_scores = []
    bigkinds_scores = []

    for k in keywords:
        for g_key, g_score in google_dict.items():
            if k in g_key or g_key in k:
                google_scores.append(g_score)
                break

        for b_key, b_score in bigkinds_dict.items():
            if k in b_key or b_key in k:
                bigkinds_scores.append(b_score)
                break

    google_part = max(google_scores) if google_scores else 0.0
    bigkinds_part = max(bigkinds_scores) if bigkinds_scores else 0.0

    final_score = 0.5 * google_part + 0.5 * bigkinds_part

    if final_score <= 0:
        return 0.0, []

    top_keywords = list(
        dict.fromkeys(
            [
                k for k in keywords
                if any(k in x or x in k for x in google_dict.keys())
                or any(k in x or x in k for x in bigkinds_dict.keys())
            ]
        )
    )[:5]

    return round(final_score, 3), top_keywords


# -------------------------------------------------
# 메인 파이프라인 (1시간 주기 실행)
# -------------------------------------------------
def run_article_trend_pipeline():
    """
    처리 흐름

    1. Google Trends 수집
    2. BigKinds Trends 계산
    3. 최신 트렌드 로드
    4. 최근 24시간 & status=4 기사 조회
    5. 기사별 Trend Score 계산
    6. trending_articles 스냅샷 저장
    7. article_data 업데이트
       - 트렌드 점수 있는 기사만 status=5
       - article_label.trend_score만 부분 업데이트
    """

    now = datetime.now(KST)
    since = now - timedelta(hours=24)

    logger.info("[TREND] 파이프라인 시작")

    # 1. 트렌드 수집
    try:
        crawl_trends()
        logger.info("[TREND] Google Trends 수집 완료")
    except Exception:
        logger.exception("[TREND] Google Trends 실패")

    try:
        run_bigkinds_trend_pip()
        logger.info("[TREND] BigKinds Trends 계산 완료")
    except Exception:
        logger.exception("[TREND] BigKinds Trends 실패")

    # 2. 최신 트렌드 로드
    google_dict = load_latest_google_trend_dict()
    bigkinds_dict = load_latest_bigkinds_trend_dict()

    if not google_dict and not bigkinds_dict:
        logger.warning("[TREND] 트렌드 데이터 없음 → 종료")
        return

    logger.info(
        "[TREND] google=%d, bigkinds=%d",
        len(google_dict),
        len(bigkinds_dict)
    )

    # 3. 기사 조회 (신뢰도 계산 완료 + 최근 24시간)
    query = {
        "size": 10000,
        "_source": ["features"],
        "query": {
            "bool": {
                "must": [
                    {"term": {"status": 4}},
                    {
                        "range": {
                            "collected_at": {
                                "gte": since.isoformat(),
                                "lte": now.isoformat()
                            }
                        }
                    }
                ]
            }
        }
    }

    hits = es.search(index=ARTICLE_INDEX, body=query)["hits"]["hits"]
    logger.info("[TREND] 대상 기사 수: %d", len(hits))

    trend_articles = []
    bulk_updates = []

    # 4. 기사별 Trend Score 계산
    for h in hits:
        article_id = h["_id"]
        features = h["_source"].get("features", [])

        score, top_keywords = calc_article_trend_score(
            features,
            google_dict,
            bigkinds_dict
        )

        # 트렌드 점수가 있는 경우만 처리
        if score > 0:
            trend_articles.append({
                "article_id": article_id,
                "final_trend_score": score,
                "trend_keywords": top_keywords
            })

            # status 변경 + trend_score만 부분 업데이트
            bulk_updates.append({
                "_op_type": "update",
                "_index": ARTICLE_INDEX,
                "_id": article_id,
                "doc": {
                    "status": 5,
                    "article_label": {
                        "trend_score": score
                    }
                }
            })

    # 5. trending_articles 저장
    if trend_articles:
        es.index(
            index=TRENDING_INDEX,
            id=now.strftime("%Y%m%d%H"),
            document={
                "trend_at": now.strftime("%Y%m%d%H"),
                "trend_articles": trend_articles
            }
        )
        logger.info(
            "[TREND] trending_articles 저장: %d",
            len(trend_articles)
        )

    # 6. article_data bulk 업데이트
    if bulk_updates:
        helpers.bulk(es, bulk_updates)
        logger.info(
            "[TREND] article_data 업데이트 완료: %d건",
            len(bulk_updates)
        )

    logger.info("[TREND] 파이프라인 종료")


if __name__ == "__main__":
    run_article_trend_pipeline()

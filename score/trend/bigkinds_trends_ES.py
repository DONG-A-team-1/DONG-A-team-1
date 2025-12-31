from collections import Counter
from datetime import datetime, timedelta, timezone
from util.elastic import es
from util.logger import Logger

logger = Logger().get_logger(__name__)
KST = timezone(timedelta(hours=9))



# article_data에서 features 읽기

def article_data_features(hours: int = 1): # 최근 1시간 이내 수집된 기사들의 'features' 필드만 조회

    # ES article_data를 '입력 데이터'로 사용
    now = datetime.now(KST) # 현재 시각 한국 표준시(KST)
    since = now - timedelta(hours=hours) # 시간의 차이(간격) 객체 hours는 이미 위에서 1 선언

    query = {
        "size": 10000, # 최대 10,000개까지 가져와
        "_source": ["features"], # features 필드만
        "query": {
            "range": {
                "collected_at": { # since ≤ collected_at ≤ now 가져와
                    "gte": since.isoformat(), # 1시간 전부터 (막대그래프에서 1시간전 기준 오른쪽)
                    "lte": now.isoformat() # 현재시간 (막대그래프에서 현재 기준 왼쪽)
                }
            }
        }
    }

    res = es.search(index="article_data", body=query)
    return [hit["_source"] for hit in res["hits"]["hits"]] # hits.hits 실제 문서 리스트


# features 기준 wordcount
def bigkinds_wordcount(articles):
    """
    - 여러 기사에 등장한 features를 전부 합쳐서
      '키워드 → 등장 횟수' 형태로 집계 [기사 단위 집계 목적]
    - articles: [{ "features": [...] }, ...]
      예)
      Counter({
            "이노스페이스": 17,
            "우주발사체": 14,
            ...
          })
    """
    counter = Counter()
    for article in articles: # ES에서 가져온 기사 리스트
        for f in article.get("features", []): # 그 기사에 들어있는 각 feature 키워드 순회
            counter[f] += 1 # 키워드가 등장할 때마다 +1
    return counter



# TrendScore 계산
def bigkinds_trend_score(rank: int, N: int = 25) -> float:
    # 공식: (N + 1 - rank) / N * 100
    # Google Trend랑 스케일 맞추기 (0~100)
    return round((N + 1 - rank) / N * 100, 4)



# Top25랭킹 + score 딕셔너리 생성
def bigkinds_trend_dict(wordcount: Counter, top_n: int = 25):
    trends = []
    score_dict = {}

    if not wordcount:
        logger.warning("기사 기반 키워드 없음")
        return trends, score_dict

    '''
    wordcounter타입 = 위에서 생성한 Counter타입과 같음
    top_n 타입 = 정수이고 따로 숫자를 지정하지않으면 기본값이 25
    '''

    for rank, (keyword, _) in enumerate(wordcount.most_common(top_n), start=1):
        score = bigkinds_trend_score(rank, top_n)

        trends.append({
            "rank": rank,
            "title": keyword,
            "trend_score": score
        })

        score_dict[keyword] = score
    '''
        trends → ES : 저장용(랭킹, 키워드, 점수)
        score_dict → 기사 : 점수 계산용(메모리 병합용)
        score_dict는 ES에 저장하지 않고 반환
        return score_dict
    '''
    return trends, score_dict



# ES 저장
def save_bigkinds_trends(trends):
    """
    - 저장 인덱스: bigkinds_trends
    - 서비스에서 '뉴스 트렌드' 점수로 바로 집계
    """
    doc = {
        "collected_at": datetime.now(KST).isoformat(timespec="seconds"),
        "trends": trends
    }

    es.index(index="bigkinds_trends", document=doc)
    logger.info(f"BigKinds 트렌드 {len(trends)}개 저장 완료")


# 전체 실행 함수

def run_bigkinds_trend_pip():
    """
    [역할]
    - BigKinds 트렌드 계산의 진입점(entry point)

    [처리 흐름]
    1. article_data에서 features 조회
    2. wordcount 집계
    3. Top25 + TrendScore 산출
    4. bigkinds_trends ES 저장
    5. score_dict 반환 (기사 점수 계산 대비)
    """
    articles_features = article_data_features()
    logger.info(f"기사 {len(articles_features)}개 기반 트렌드 계산")

    features_wordcount = bigkinds_wordcount(articles_features)
    trends, score_dict = bigkinds_trend_dict(features_wordcount)

    if not trends:
        logger.warning("BigKinds 트렌드 결과 없음 → ES 저장 스킵")
        return

    save_bigkinds_trends(trends)

if __name__ == "__main__":
    run_bigkinds_trend_pip()



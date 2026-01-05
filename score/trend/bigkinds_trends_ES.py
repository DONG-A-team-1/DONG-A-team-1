from collections import Counter
from datetime import datetime, timedelta, timezone
from util.elastic import es
from util.logger import Logger

logger = Logger().get_logger(__name__)
KST = timezone(timedelta(hours=9))



# article_data에서 features 읽기

from datetime import datetime, timedelta, timezone
from util.elastic import es

KST = timezone(timedelta(hours=9))


def article_data_features(hours: int = 1):
    """
    BigKinds 트렌드용
    - 이번 사이클에 ES에 적재된 기사들의 features 전부 사용
    - 시간 / status 조건 ❌
    """
    query = {
        "size": 10000,                      # 최대 10,000개까지 조회
        "_source": ["features"],            # features 필드만 가져옴
        "query": {
            "exists": {
                "field": "features"
            }
        }
    }

    res = es.search(index="article_data", body=query)
    return [hit["_source"] for hit in res["hits"]["hits"]]

# features 기준 wordcount
def bigkinds_wordcount(articles):
    """
    - 여러 기사에 등장한 features를 전부 합쳐서
      '키워드 → 등장 횟수(df)' 형태로 집계
    - 기사 단위 중복 제거
    - df ≥ 2 조건 적용
    """
    counter = Counter()

    for article in articles:
        # ✅ 기사 단위 중복 제거
        unique_features = set(article.get("features", []))
        for f in unique_features:
            counter[f] += 1

    # ✅ df ≥ 2 필터
    counter = Counter({k: v for k, v in counter.items() if v >= 2})

    return counter




# TrendScore 계산
def bigkinds_trend_score(rank: int, N: int = 25) -> float:
    # 공식: (N + 1 - rank) / N -> Google Trend랑 스케일 맞추기
    return round((N + 1 - rank) / N, 4)



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
        # ES 매핑 yyyyMMddHH 일치 시켜줘어ㅔㅇ너래;ㄹ;ㅜㄴㅇㄻㄴㅇ;ㅜ
        "collected_at": datetime.now(KST).strftime("%Y%m%d%H"),
        "trends": trends
    }

    es.index(
        index="bigkinds_trends",
        document=doc,
        refresh="wait_for"  # 새로고침 안되서 자꾸 빈거를 가져와서 0뜸 이거 없으면 진짜안돌아간리ㅓㅜㅏㅁㄴㅇ;라ㅣㅜㅁㄴ어라ㅠㅜ
    )

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
    return score_dict

if __name__ == "__main__":
    run_bigkinds_trend_pip()



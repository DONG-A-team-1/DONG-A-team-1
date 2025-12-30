# HAND (FINAL)
"""
[공식 출처]
- 한국기자협회
- 감염병보도준칙
- 아동·인권 보도준칙
- 혐오표현 반대 미디어 실천 선언
- 주관적·미화 표현 자제 원칙
https://www.journalist.or.kr/news/section4.html?p_num=20

[역할 정의]
- 기사 제목/본문에서
- 언론 윤리·보도준칙 위반 "위험 신호"를 수치화
- KLUE-BERT(제목–본문 불일치 모델)의 판단을 보조

⚠️ 주의
- 이 점수는 사실 여부를 판정하지 않음
- 기사 품질·윤리 리스크 신호임
- 단독 판단에 사용 ❌
"""

# 감염병·재난 보도 과장 표현 금지
PANIC_WORDS = [
    "패닉", "대혼란", "대란", "공포", "창궐", "확산 공포"
]

# 아동학대·가정범죄 미화·완화 표현 금지
CHILD_ABUSE_EUPHEMISMS = [
    "일가족 동반 자살",
    "일가족 극단 선택",
    "훈육",
    "체벌"
]

# 주관적·선정적 표현 자제
SUBJECTIVE_WORDS = [
    "의외의", "예상을 넘는", "기대에 못 미치는"
]

# 범죄·성폭력 미화·축소 표현 금지
CRIME_EUPHEMISMS = [
    "몹쓸짓", "나쁜 손", "몰카", "성추문"
]

# 혐오·차별·이념 낙인 표현 금지
HATE_EXPRESSIONS = [
    "빨갱이", "종북", "홍어", "전라디언"
]

# 과도한 감정·선정성
SENSATIONAL_WORDS = [
    "충격", "경악", "소름", "발칵", "충격적"
]

# 제목용 HAND
def hand_title_score(title: str) -> float:
    """
    기사 제목 윤리·보도준칙 위반 위험 점수 [0,1]
    - 제목만 사용
    - '존재 여부' 중심 판단
    - KLUE-BERT 보조 신호
    """
    if not isinstance(title, str):
        return 0.0

    # 한글에는 영향 거의 없지만, 영어 혼합 기사 대비
    title = title.lower()
    score = 0.0

    # 감염병·재난 과장
    if any(w in title for w in PANIC_WORDS):
        score = max(score, 0.4)

    # 아동학대·가정범죄 미화
    if any(w in title for w in CHILD_ABUSE_EUPHEMISMS):
        score = max(score, 0.6)

    # 주관적·선정적 평가
    if any(w in title for w in SUBJECTIVE_WORDS):
        score = max(score, 0.2)

    # 범죄 미화·완화
    if any(w in title for w in CRIME_EUPHEMISMS):
        score = max(score, 0.4)

    # 혐오·차별 표현
    if any(w in title for w in HATE_EXPRESSIONS):
        score = max(score, 0.7)

    # 감정 과잉·선정성
    if any(w in title for w in SENSATIONAL_WORDS):
        score = max(score, 0.3)

    # 암시적 질문형 제목
    if "?" in title:
        score = max(score, 0.2)

    return min(score, 1.0)


# 본문용 HAND
def hand_body_score(content: str) -> float:
    """
    기사 본문 윤리·보도준칙 위반 위험 점수 [0,1]
    - 본문 사용
    - '반복·누적·정착' 중심 판단
    - 혐오·과장 표현의 확산 위험 감지
    """

    if not isinstance(content, str):
        return 0.0

    content = content.lower()
    score = 0.0

    # 혐오·차별 표현 반복
    hate_hits = sum(content.count(w) for w in HATE_EXPRESSIONS)
    if hate_hits >= 3:
        score = max(score, 0.6)

    # 감염병·재난 과장 반복
    panic_hits = sum(content.count(w) for w in PANIC_WORDS)
    if panic_hits >= 3:
        score = max(score, 0.4)

    # 아동학대 미화 표현 반복
    abuse_hits = sum(content.count(w) for w in CHILD_ABUSE_EUPHEMISMS)
    if abuse_hits >= 2:
        score = max(score, 0.6)

    # 주관적·선정적 표현 누적
    subjective_hits = sum(content.count(w) for w in SUBJECTIVE_WORDS)
    if subjective_hits >= 3:
        score = max(score, 0.3)

    return min(score, 1.0)

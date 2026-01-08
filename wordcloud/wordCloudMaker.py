

# 학원가서 수정해야됨
from collections import Counter
from pyecharts import options as opts
from pyecharts.charts import WordCloud


async def make_wordcloud_data(bigkinds_data):
    # 1. 데이터 가공
    all_keywords = []
    for data in bigkinds_data:
        all_keywords.extend(data.get("keywords", []))

    # 2. 빈도수 계산 및 상위 키워드 추출 (너무 많으면 느려지므로 TOP 50 권장)
    counts = Counter(all_keywords).most_common(100)
    word_data = [(k, v) for k, v in counts]

    # 3. 워드클라우드 설정
    wordcloud = (
        WordCloud()
        .add(
            "",
            word_data,
            word_size_range=[20, 100],  # 폰트 크기 조절
            shape="circle"  # 중앙 밀집을 위해 circle 추천
        )
        .set_global_opts(
            tooltip_opts=opts.TooltipOpts(is_show=True),
        )
    )
    # print(word_data)
    # 핵심: 차트의 설정값(Option)만 JSON으로 반환
    return wordcloud.dump_options_with_quotes()

# async def make_wordcloud_data(bigkinds_data):
#     # 1. 키워드 가공
#     all_keywords = []
#     for data in bigkinds_data:
#         all_keywords.extend(data.get("keywords", []))
#
#     # 2. 빈도수 계산
#     counts = Counter(all_keywords)
#
#     # 3. 프론트엔드 가중치(1~5)에 맞게 정규화 (선택 사항)
#     # 단순히 상위 30개 정도만 뽑아서 보냅니다.
#     common_tags = counts.most_common(30)
#
#     # 프론트엔드 형식: [{"text": "단어", "weight": 5}, ...]
#     # 빈도수가 가장 높으면 5, 낮으면 1이 되도록 매핑
#     max_count = common_tags[0][1] if common_tags else 1
#
#     result = []
#     for text, count in common_tags:
#         weight = max(1, min(5, int((count / max_count) * 5)))
#         result.append({"text": text, "weight": weight})
#
#     # 이 데이터를 파일로 저장하거나 API 응답으로 보냅니다.
#     with open("wordcloud/keywords.json", "w", encoding="utf-8") as f:
#         json.dump(result, f, ensure_ascii=False)
#
#     print("키워드 데이터 저장 완료: keywords.json")


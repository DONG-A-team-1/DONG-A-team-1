import re
from typing import Optional, Tuple

_VIEW_TOKENS = ("크게보기", "작게보기", "기사보기", "더보기")

def clean_article_text(text: Optional[str]) -> str:
    if not text:
        return ""

    s = text

    # 1️⃣ UI 토큰 제거
    for tok in _VIEW_TOKENS:
        s = s.replace(tok, " ")

    # 2️⃣ HTML 태그 제거
    s = re.sub(r"<[^>]+>", " ", s)

    # 3️⃣ 줄바꿈으로 잘린 단어만 복구 (핵심 수정)
    # 예: "박\n물관" → "박물관"
    s = re.sub(r"([가-힣])\n+([가-힣])", r"\1\2", s)

    # 4️⃣ 나머지 줄바꿈/탭 → 공백
    s = re.sub(r"[\r\n\t]+", " ", s)

    # 5️⃣ 공백 정규화
    s = re.sub(r"\s+", " ", s).strip()

    return s
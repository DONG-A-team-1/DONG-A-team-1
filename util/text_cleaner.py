import re
from typing import Optional

_VIEW_TOKENS = ("크게보기", "작게보기", "기사보기", "더보기")

# 사진/캡션 관련 패턴 (짧은 구간만 제거 → 오탐 방지)
_INLINE_PHOTO_PATTERNS = [
    r'\((?:[^)]{0,80})?(?:사진|제공|출처|캡처)(?:[^)]{0,80})?\)',
    r'\[(?:[^\]]{0,80})?(?:사진|제공|출처|캡처)(?:[^\]]{0,80})?\]',
    r'【(?:[^】]{0,80})?(?:사진|제공|출처|캡처)(?:[^】]{0,80})?】',
]

# 문장 단위로 남아있는 캡션 제거용
_PHOTO_LINE_RE = re.compile(
    r'(사진\s*(?:출처|제공)\s*[=:]\s*[^.。\n]+'
    r'|사진\s*[=:]\s*[^.。\n]+'
    r'|자료\s*사진'
    r'|동아DB|동아일보DB'
    r'|연합뉴스|뉴시스|로이터|AFP|AP|EPA'
    r'|ⓒ\s*[^.。\n]+'
    r'|출처\s*:\s*[^.。\n]+'
    r'|제공\s*:\s*[^.。\n]+'
    r'|캡처|캡처화면|화면캡처|유튜브\s*캡처)'
)

def clean_article_text(text: Optional[str]) -> str:
    if not text:
        return ""

    s = text

    # 1️⃣ UI 토큰 제거
    for tok in _VIEW_TOKENS:
        s = s.replace(tok, " ")

    # 2️⃣ HTML 태그 제거
    s = re.sub(r"<[^>]+>", " ", s)

    # 3️⃣ 괄호형 사진 출처/캡션 제거 (본문 중간)
    for p in _INLINE_PHOTO_PATTERNS:
        s = re.sub(p, " ", s)

    # 4️⃣ 줄바꿈으로 잘린 단어 복구
    # 예: "박\n물관" → "박물관"
    s = re.sub(r"([가-힣])\n+([가-힣])", r"\1\2", s)

    # 5️⃣ 나머지 줄바꿈/탭 → 공백
    s = re.sub(r"[\r\n\t]+", " ", s)

    # 6️⃣ 사진 출처 문장 제거 (짧은 캡션성 문장만)
    s = re.sub(_PHOTO_LINE_RE, " ", s)

    # 7️⃣ 공백 정규화
    s = re.sub(r"\s+", " ", s).strip()

    return s

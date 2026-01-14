# total_trust_score.py
# - KLUE-BERT + HAND 신뢰도 점수 산출 (ES 연동용)

import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from pathlib import Path

from score.trust.HAND import (
    hand_title_score,
    hand_body_score
)

# ===============================
# 모델 경로 (프로젝트 루트 기준)
# ===============================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = PROJECT_ROOT / "model" / "klue_bert_clickbait_test (2)" / "epoch_3"

print("USING FILE :", __file__)
print("MODEL_DIR  :", MODEL_DIR)
print("EXISTS     :", MODEL_DIR.exists())

MODEL_VERSION = "klue_v1_hand_v6"
MAX_LEN = 256

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# 반드시 local_files_only=True
tokenizer = AutoTokenizer.from_pretrained(
    MODEL_DIR,
    local_files_only=True
)

model = AutoModelForSequenceClassification.from_pretrained(
    MODEL_DIR,
    num_labels=2,
    local_files_only=True
)

model.to(device)
model.eval()

print("학습한 모델 로드 완료:", device)


# ===============================
# KLUE-BERT 추론
# ===============================
def klue_clickbait_prob(title: str, content: str) -> float:
    text = f"[TITLE] {title} [CONTENT] {content}"

    encoding = tokenizer(
        text,
        max_length=MAX_LEN,
        truncation=True,
        padding="max_length",
        return_tensors="pt"
    )

    with torch.no_grad():
        outputs = model(
            input_ids=encoding["input_ids"].to(device),
            attention_mask=encoding["attention_mask"].to(device)
        )

    probs = F.softmax(outputs.logits, dim=1)
    return probs[0, 1].item()


# ===============================
# 최종 신뢰도 계산
# ===============================
def compute_trust_score(title: str, content: str) -> dict:
    clickbait_prob = klue_clickbait_prob(title, content)
    hand_title = hand_title_score(title)
    hand_body  = hand_body_score(content)

    final_risk = (
        0.70 * clickbait_prob +
        0.20 * hand_title +
        0.10 * hand_body
    )

    trust_score = (1 - final_risk) * 100

    return {
        "article_label": {
            "article_trust_score": round(trust_score, 2)
        },
        "status": 4
    }

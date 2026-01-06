from util.database import SessionLocal
from sqlalchemy import text
from datetime import datetime
from util.elastic import es  # util 폴더의 elastic.py
from util.text_cleaner import yyyymmdd_to_iso

import random
import string

# 아이디 중복 체크
def check_id(user_id: str):
    with SessionLocal() as connection:
        query = text("SELECT user_id FROM user_auth WHERE user_id = :u_id")
        result = connection.execute(query, {"u_id": user_id}).fetchone()
        return result is None  # 존재하지 않으면 True(사용 가능)


# 회원가입
def register(data: dict):
    with SessionLocal() as connection:
        transaction = connection.begin()
        try:
            # 1. user_auth 삽입
            auth_query = text("""
                INSERT INTO user_auth (user_id, user_pw, user_email, joined_at, is_active) 
                VALUES (:u_id, :pw, :email, :joined, :active)
            """)
            connection.execute(auth_query, {
                "u_id": data['user_id'], "pw": data['user_pw'], "email": data['user_email'],
                "joined": datetime.now(), "active": True
            })

            # 2. user_info 삽입
            info_query = text("""
                INSERT INTO user_info (user_id, date_of_birth, user_name, user_gender) 
                VALUES (:u_id, :dob, :name, :gender)
            """)
            connection.execute(info_query, {
                "u_id": data['user_id'], "dob": data['birthdate'],
                "name": data['user_name'], "gender": data['user_gender']
            })

            # 3. security_questions 삽입
            question_query = text("""
                INSERT INTO security_questions (user_id, question_text, answer_hash) 
                VALUES (:u_id, :q_text, :a_hash)
            """)
            connection.execute(question_query, {
                "u_id": data['user_id'], "q_text": data['security_question'], "a_hash": data['security_answer']
            })

            transaction.commit()
            return True
        except Exception as e:
            transaction.rollback()
            print(f"DB Error: {e}")
            return False


# 로그인
def login(user_id, password, session):
    with SessionLocal() as connection:
        query = text("""
            SELECT user_id, is_active 
            FROM user_auth 
            WHERE user_id = :u_id AND user_pw = :pw
        """)
        result = connection.execute(query, {"u_id": user_id, "pw": password}).fetchone()

        if result:
            if result.is_active == 1:
                session['loginId'] = result.user_id

                user_info = get_user_data(result.user_id)
                user_name = user_info.user_name if user_info else "회원"

                return "SUCCESS", user_name

            return "INACTIVE", None

        return "FAIL", None

# 아이디 찾기
def find_id(email: str, security_answer: str):
    with SessionLocal() as connection:
        query = text("""
            SELECT a.user_id 
            FROM user_auth a
            JOIN security_questions q ON a.user_id = q.user_id
            WHERE a.user_email = :email AND q.answer_hash = :ans
        """)
        result = connection.execute(query, {"email": email, "ans": security_answer}).fetchone()

        if result:
            return result.user_id  # 아이디 반환
        return None

# 비밀번호 찾기
def reset_to_temp_pw(user_id, email):
    with SessionLocal() as connection:
        # 1. 먼저 유저가 존재하는지 확인
        query_check = text("SELECT 1 FROM user_auth WHERE user_id = :u_id AND user_email = :email")
        exists = connection.execute(query_check, {"u_id": user_id, "email": email}).fetchone()

        if not exists:
            return None

        # 2. 임시 비밀번호 생성 (10자리)
        chars = string.ascii_letters + string.digits + "!@#$"
        temp_pw = "".join(random.sample(chars, 10))

        # 3. DB 업데이트
        query_update = text("UPDATE user_auth SET user_pw = :pw WHERE user_id = :u_id")
        connection.execute(query_update, {"pw": temp_pw, "u_id": user_id})
        connection.commit()  # 변경사항 저장

        return temp_pw

# 비밀번호 변경
def change_pw(user_id, current_pw, new_pw):
    with SessionLocal() as connection:
        check_query = text("SELECT user_id FROM user_auth WHERE user_id = :u_id AND user_pw = :pw")
        user = connection.execute(check_query, {"u_id": user_id, "pw": current_pw}).fetchone()

        if not user: return False

        update_query = text("UPDATE user_auth SET user_pw = :new_pw WHERE user_id = :u_id")
        connection.execute(update_query, {"new_pw": new_pw, "u_id": user_id})
        connection.commit()
        return True


# 회원 탈퇴
def withdraw(user_id):
    with SessionLocal() as connection:
        query = text("UPDATE user_auth SET is_active = 0 WHERE user_id = :u_id")
        connection.execute(query, {"u_id": user_id})
        connection.commit()
        return True


# 정보 수정
def update_user_info(user_id, name, gender, birth, email):
    with SessionLocal() as connection:
        # 만약 정보가 두 테이블에 나눠져 있다면 각각 UPDATE 해야 합니다.
        # 예시: 이메일은 auth 테이블, 나머지는 info 테이블인 경우

        # 1. user_auth 테이블 (이메일 수정)
        query_auth = text("UPDATE user_auth SET user_email = :email WHERE user_id = :u_id")
        connection.execute(query_auth, {"email": email, "u_id": user_id})

        # 2. user_info 테이블 (이름, 성별, 생년월일 수정)
        query_info = text("""
            UPDATE user_info 
            SET user_name = :name, user_gender = :gender, date_of_birth = :birth 
            WHERE user_id = :u_id
        """)
        connection.execute(query_info, {"name": name, "gender": gender, "birth": birth, "u_id": user_id})

        connection.commit()


# 비밀번호 변경
def update_pw(user_id, current_pw, new_pw):
    with SessionLocal() as connection:
        # 현재 비밀번호 확인
        check_query = text("SELECT 1 FROM user_auth WHERE user_id = :u_id AND user_pw = :pw")
        if not connection.execute(check_query, {"u_id": user_id, "pw": current_pw}).fetchone():
            return False

        # 새 비밀번호 업데이트
        update_query = text("UPDATE user_auth SET user_pw = :new_pw WHERE user_id = :u_id")
        connection.execute(update_query, {"new_pw": new_pw, "u_id": user_id})
        connection.commit()
        return True


def get_user_data(user_id):
    with SessionLocal() as connection:
        # user_auth(a)와 user_info(i)를 JOIN 합니다.
        query = text("""
            SELECT 
                i.user_name, 
                a.user_email, 
                i.date_of_birth, 
                i.user_gender
            FROM user_auth a
            JOIN user_info i ON a.user_id = i.user_id
            WHERE a.user_id = :u_id
        """)
        user = connection.execute(query, {"u_id": user_id}).fetchone()
        return user

# 검색 ------해정--------------------------------------

# FastAPI/search.py (또는 해당 파일)

def search_articles(search_type: str, query: str, size: int = 20):
    """기사 검색"""

    # 검색 타입별 쿼리 생성
    if search_type == "all" or search_type == "title_body":
        # 제목 + 본문 검색
        es_query = {
            "bool": {
                "should": [
                    {"match": {"article_title": query}},
                    {"match": {"article_content": query}}
                ]
            }
        }

    elif search_type == "title":
        # 제목만 검색
        es_query = {"match": {"article_title": query}}

    elif search_type == "content" or search_type == "body":
        # 본문만 검색
        es_query = {"match": {"article_content": query}}

    elif search_type == "keywords" or search_type == "keyword":
        # 키워드 검색
        es_query = {"match": {"keywords.raw": query}} # keywords=메인필드 , 정확한 일치만 검색
        # 서브필드 kewords.raw=타입 text 분석기 nori로 유연한 검색 가능함.

    else:
        # 기본: 제목 + 본문
        es_query = {
            "bool": {
                "should": [
                    {"match": {"article_title": query}},
                    {"match": {"article_content": query}}
                ]
            }
        }

    # ES 검색 실행
    body = {
        "_source": [
            "article_id",
            "press",
            "upload_date",
            "article_title",
            "article_content",
            "article_img",
            "url",
            "article_label",
            "keywords"
        ],
        "size": size,
        "query": es_query,
        "sort": [
            {"upload_date": {"order": "desc"}}
        ]
    }

    resp = es.search(index="article_data", body=body)
    hits = resp.get("hits", {}).get("hits", [])

    # 결과 포맷팅
    articles = []
    for hit in hits:
        src = hit.get("_source", {})
        label = src.get("article_label") or {}

        # trustScore 안전하게 처리
        raw_score = label.get("article_trust_score")

        if raw_score is None:
            trust_score = 0
        else:
            raw_score = float(raw_score)

            if raw_score <= 1:
                # 0~1 확률형
                trust_score = round(raw_score * 100)
            elif raw_score <= 100:
                # 이미 퍼센트
                trust_score = round(raw_score)
            else:
                # 0~4095 같은 raw 모델 점수
                trust_score = round((raw_score / 4095) * 100)

        articles.append({
            "article_id": src.get("article_id"),
            "title": src.get("article_title", ""),
            "content": src.get("article_content", ""),
            "image": src.get("article_img"),
            "category": label.get("category"),
            "source": src.get("press"),
            "upload_date": yyyymmdd_to_iso(src.get("upload_date")),
            "trustScore": trust_score,
            "keywords": src.get("keywords", [])
        })

    return {
        "success": True,
        "query": query,
        "search_type": search_type,
        "total": resp['hits']['total']['value'],
        "articles": articles
    }
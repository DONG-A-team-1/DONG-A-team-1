from util.database import SessionLocal
from sqlalchemy import text
from datetime import datetime


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
def login(user_id, password, session): # 배운대로 세션 넣긴 했는데 수정가능
    with SessionLocal() as connection:
        query = text("SELECT user_id, is_active FROM user_auth WHERE user_id = :u_id AND user_pw = :pw")
        result = connection.execute(query, {"u_id": user_id, "pw": password}).fetchone()

        if result:
            if result.is_active == 1:
                session['loginId'] = result.user_id  # 세션 저장
                return "SUCCESS"
            return "INACTIVE"
        return "FAIL"


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


# 탈퇴
def withdraw(user_id):
    with SessionLocal() as connection:
        query = text("UPDATE user_auth SET is_active = 0 WHERE user_id = :u_id")
        connection.execute(query, {"u_id": user_id})
        connection.commit()
        return True
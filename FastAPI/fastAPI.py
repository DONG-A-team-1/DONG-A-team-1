from fastapi import FastAPI, Form, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse,RedirectResponse
from ..util.database import get_engine
from sqlalchemy import text
from datetime import datetime
import os

app = FastAPI()
engine = get_engine()
app.mount("/view", StaticFiles(directory="DONG-A-team-1/view"), name="view")

@app.get("/")
async def read_root():
    return RedirectResponse(url="/view/logregist.html") # 홈페이지로 바꿔야됨 확인용으로 logregist 로 해둠

@app.post("/register")
async def register_user(
    user_id: str = Form(...),      # ERD 에 int 라고 돼있어서 int 로 작성했는데 수정하면 됩니다 ( 수정 완 )
    user_pw: str = Form(...),      # 비밀번호
    user_email: str = Form(...),   # 이메일
    security_question: str = Form(...),
    security_answer: str = Form(...),
    user_name: str = Form(...),    # 유저 이름
    birthdate: str = Form(...),    # 생년월일
    user_gender: str = Form(...)   # 성별 (male, female, none)
):
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            # 1. [부모] user_auth 테이블에 데이터 삽입
            auth_query = text("""
                INSERT INTO user_auth (user_id, user_pw, user_email, joined_at, is_active) 
                VALUES (:u_id, :pw, :email, :joined, :active)
            """)
            connection.execute(auth_query, {
                "u_id": user_id,
                "pw": user_pw,
                "email": user_email,
                "joined": datetime.now(),
                "active": True
            })

            # 2. [자식] user_info 테이블에 데이터 삽입 (동일한 user_id 사용)
            info_query = text("""
                INSERT INTO user_info (user_id, date_of_birth, user_name, user_gender) 
                VALUES (:u_id, :dob, :name, :gender)
            """)
            connection.execute(info_query, {
                "u_id": user_id,
                "dob": birthdate,
                "name": user_name,
                "gender": user_gender
            })

            # 3. [자식] security_questions 테이블에 데이터 삽입 (동일한 user_id 사용)
            question_query = text("""
                INSERT INTO security_questions (user_id, question_text, answer_hash) 
                VALUES (:u_id, :q_text, :a_hash)
            """)
            connection.execute(question_query, {
                "u_id": user_id,
                "q_text": security_question,
                "a_hash": security_answer
            })

            transaction.commit()
            return {"status": "success", "message": "회원가입이 완료되었습니다."}

        except Exception as e:
            transaction.rollback()
            print(f"회원가입 에러 발생: {e}")
            return JSONResponse(status_code=500, content={"message": "가입 실패: 아이디 중복 또는 데이터 오류"})


@app.get("/check-id")
async def check_id(user_id: int):  # ERD에 따라 int로 설정
    with engine.connect() as connection:
        # user_auth 테이블에서 해당 user_id가 있는지 조회
        query = text("SELECT user_id FROM user_auth WHERE user_id = :u_id")
        result = connection.execute(query, {"u_id": user_id}).fetchone()

        if result:
            # 아이디가 이미 존재함
            return {"available": False}
        else:
            # 아이디 사용 가능
            return {"available": True}
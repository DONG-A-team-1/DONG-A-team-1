from fastapi import FastAPI, Form, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse,RedirectResponse
from util.database import SessionLocal
from sqlalchemy import text
from datetime import datetime
import os

app = FastAPI()
app.mount("/view", StaticFiles(directory="view"), name="view")

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
    with SessionLocal() as connection:
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
async def check_id(user_id: str):  # ERD에 따라 int로 설정
    with SessionLocal() as connection:
        # user_auth 테이블에서 해당 user_id가 있는지 조회
        query = text("SELECT user_id FROM user_auth WHERE user_id = :u_id")
        result = connection.execute(query, {"u_id": user_id}).fetchone()

        if result:
            # 아이디가 이미 존재함
            return {"available": False}
        else:
            # 아이디 사용 가능
            return {"available": True}


@app.get("/login")
async def login(user_id: str, password: str):
    with SessionLocal() as connection:
        # 아이디, 비밀번호가 일치하는 데이터를 찾으면서 동시에 is_active 상태도 가져옵니다.
        query = text("""
            SELECT user_id, user_pw, is_active 
            FROM user_auth 
            WHERE user_id = :u_id AND user_pw = :pw
        """)

        result = connection.execute(query, {"u_id": user_id, "pw": password}).fetchone()

        # 1. 일치하는 계정이 없는 경우 (아이디 혹은 비밀번호 틀림)
        if not result:
            return JSONResponse(
                status_code=401,
                content={"status": "fail", "message": "아이디 또는 비밀번호가 일치하지 않습니다."}
            )

        # 2. 계정은 존재하지만 비활성화(is_active=0) 상태인 경우
        # tinyint(1)은 Python에서 0 또는 1(혹은 True/False)로 읽힙니다.
        if result.is_active == 0:
            return JSONResponse(
                status_code=403,
                content={"status": "fail", "message": "접근 권한이 없습니다. 관리자에게 문의하세요."}
            )

        # 3. 로그인 성공
        return {
            "status": "success",
            "message": f"{result.user_id}님 환영합니다!",
            "user_id": result.user_id
        }

@app.post("/withdraw")
async def withdraw(user_id: str):
    with SessionLocal() as connection:
        query = text("UPDATE user_auth SET is_active = 0 WHERE user_id = :u_id")
        connection.execute(query, {"u_id": user_id})
        connection.commit()
        return {"message": "탈퇴 처리가 완료되었습니다."}

@app.post("/change-password")
async def change_password(
        user_id: str = Form(...),
        current_pw: str = Form(...),
        new_pw: str = Form(...)
):
    with SessionLocal() as connection:
        # 1. 먼저 현재 아이디와 비밀번호가 맞는지 확인
        check_query = text("""
            SELECT user_id FROM user_auth 
            WHERE user_id = :u_id AND user_pw = :pw
        """)
        user = connection.execute(check_query, {"u_id": user_id, "pw": current_pw}).fetchone()

        if not user:
            return JSONResponse(
                status_code=401,
                content={"status": "fail", "message": "현재 비밀번호가 일치하지 않습니다."}
            )

        # 2. 일치한다면 새 비밀번호로 업데이트
        try:
            update_query = text("""
                UPDATE user_auth 
                SET user_pw = :new_pw 
                WHERE user_id = :u_id
            """)
            connection.execute(update_query, {"new_pw": new_pw, "u_id": user_id})
            connection.commit()  # 데이터 변경이므로 반드시 commit 필요

            return {"status": "success", "message": "비밀번호가 성공적으로 변경되었습니다."}

        except Exception as e:
            connection.rollback()
            return JSONResponse(status_code=500, content={"message": "서버 오류로 변경에 실패했습니다."})
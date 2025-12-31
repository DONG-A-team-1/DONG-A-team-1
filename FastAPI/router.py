from fastapi import FastAPI, Form, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
# try:
#     import member
# except ModuleNotFoundError:
#     from . import member  # 분리한 파일 임포트
from . import member

app = FastAPI()
app.mount("/view", StaticFiles(directory="view"), name="view")
app.add_middleware(SessionMiddleware, secret_key="your_secret_key")


@app.get("/")
async def read_root():
    return RedirectResponse(url="/view/home.html") # 기본 메인페이지로 지정해야됨


@app.get("/check-id")
async def check_id(user_id: str):
    is_available = member.check_id(user_id)
    return {"available": is_available}


@app.post("/register")
async def register_user(
        user_id: str = Form(...),
        user_pw: str = Form(...),
        user_email: str = Form(...),
        security_question: str = Form(...),
        security_answer: str = Form(...),
        user_name: str = Form(...),
        birthdate: str = Form(...),
        user_gender: str = Form(...)
):
    # 데이터를 사전 형태로 묶어서 전달
    data = locals()
    success = member.register(data)
    if success:
        return {"status": "success", "message": "회원가입 완료"}
    return JSONResponse(status_code=500, content={"message": "가입 실패"})


@app.post("/login")  # GET보다는 POST 권장 (보안상)
async def login(
        req: Request,
        user_id: str = Form(...),
        password: str = Form(...),
):
    result = member.login(user_id, password, req.session)

    if result == "SUCCESS":
        return {"status": "success", "message": "로그인 성공"}
    elif result == "INACTIVE":
        return JSONResponse(status_code=403, content={"message": "비활성화된 계정"})
    else:
        return JSONResponse(status_code=401, content={"message": "정보 불일치"})


@app.get("/logout")
async def logout(req: Request):
    # 1. 세션 데이터 완전히 삭제
    req.session.clear()
    # 2. 삭제 후 메인페이지로 이동
    return RedirectResponse(url="/view/mainpage.html") # 메인페이지에 맞게 형식 조정 필요


@app.post("/change-password")
async def change_password(
        current_pw: str = Form(...),
        new_pw: str = Form(...),
        req: Request = None
):
    user_id = req.session.get('loginId')
    if not user_id: return JSONResponse(status_code=401, content={"message": "로그인 필요"})

    if member.change_pw(user_id, current_pw, new_pw):
        return {"status": "success", "message": "변경 완료"}
    return JSONResponse(status_code=400, content={"message": "현재 비밀번호 불일치"})


@app.post("/withdraw")
async def withdraw(req: Request):
    user_id = req.session.get('loginId')
    if member.withdraw(user_id):
        req.session.clear()  # 탈퇴 후 세션 비우기
        return {"message": "탈퇴 완료"}
    return None


@app.post("/find-id")
async def find_user_id(
    user_email: str = Form(...),
    security_answer: str = Form(...)
):
    user_id = member.find_id(user_email, security_answer)
    if user_id:
        return {"status": "success", "user_id": user_id}
    return JSONResponse(
        status_code=404,
        content={"status": "fail", "message": "일치하는 정보가 없습니다."}
)


@app.post("/find-pw")
async def find_user_pw(
        user_id: str = Form(...),
        user_email: str = Form(...)
):
    temp_pw = member.reset_to_temp_pw(user_id, user_email)

    if temp_pw:
        return {"status": "success", "temp_pw": temp_pw}

    return JSONResponse(
        status_code=404,
        content={"status": "fail", "message": "일치하는 정보가 없습니다."}
    )


@app.post("/change-information") # 회원 정보수정 만들어야됨  로그인 돼있는 아이디 받아와서 그거에 맞게 적용시키는 식
async def change_information(
    user_id: str = Form(...),
):
    pass
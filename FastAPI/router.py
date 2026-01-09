from fastapi import FastAPI, Form, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse

from starlette.middleware.sessions import SessionMiddleware
# router 연결(session)
from api.session_ping import router as session_ping_router
from api.session import router as session_router
from api.session_end import router as session_end_router
from pydantic import BaseModel, Field
from typing import List, Optional

import json

from . import member
from . import article
from . import topic
from . import search

from . import category
from wordcloud.wordCloudMaker import make_wordcloud_data
from util.logger import Logger
from util.elastic import es

logger = Logger().get_logger(__name__)
app = FastAPI()

# router 연결 === session 관련 ===
app.include_router(session_router)
app.include_router(session_ping_router)
app.include_router(session_end_router)
# static 파일
app.mount("/view", StaticFiles(directory="view"), name="view")
app.mount("/wordcloud", StaticFiles(directory="wordcloud"), name="wordcloud")

# middleware
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
    result, user_name = member.login(user_id, password, req.session)

    if result == "SUCCESS":
        return {"status": "success", "message": "로그인 성공","user_name": user_name}
    elif result == "INACTIVE":
        return JSONResponse(status_code=403, content={"message": "비활성화된 계정"})
    else:
        return JSONResponse(status_code=401, content={"message": "정보 불일치"})


@app.get("/logout")
async def logout(req: Request):
    # 1. 세션 데이터 완전히 삭제
    req.session.clear()
    # 2. 삭제 후 메인페이지로 이동
    return RedirectResponse(url="/view/home.html") # 메인페이지에 맞게 형식 조정 필요


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


# @app.post("/change-information") # 회원 정보수정 만들어야됨  로그인 돼있는 아이디 받아와서 그거에 맞게 적용시키는 식
# async def change_information(
#     user_id: str = Form(...),
# ):
#     pass
  
 
@app.get("/article/{article_id}", response_class=HTMLResponse)
async def article_page(request: Request, article_id: str):
    return RedirectResponse(
        url=f"/view/individual_article.html?article_id={article_id}",
        status_code=302
    )

@app.get("/api/article/{article_id}")
def get_article(article_id: str):
    # ES/DB에서 조회
    main = article.get_article(article_id)
    related = article.get_related(article_id)
    try:
        polar_topic = topic.get_opposite_topic(article_id)
        return {
            "article": main,
            "related": related,
            "polar": polar_topic
        }
    except Exception:
        return {
            "article": main,
            "related": related
        }


@app.post("/update-info")
async def api_update_info(request: Request):
    user_id = request.session.get('loginId')
    if not user_id:
        return JSONResponse(status_code=401, content={"message": "로그인이 필요합니다."})

    # JSON 데이터를 딕셔너리로 읽기
    data = await request.json()

    # JavaScript에서 보낸 key 값과 맞춰서 꺼내기 member 함수에 넣기 위함
    name = data.get('name')
    gender = data.get('gender')
    birth = data.get('birth')
    email = data.get('email')

    # 필수값 검증 (이름, 이메일, 생일, 성별 모두 필수라면)
    if not all([name, gender, birth, email]):
        return JSONResponse(status_code=400, content={"message": "모든 항목을 입력해주세요."})

    member.update_user_info(user_id, name, gender, birth, email)
    return {"status": "success"}


@app.post("/update-password")
async def api_update_pw(
        request: Request,
        current_pw: str = Form(...),
        new_pw: str = Form(...)
):
    user_id = request.session.get('loginId')
    success = member.update_pw(user_id, current_pw, new_pw)
    if success: return {"status": "success"}
    return JSONResponse(status_code=400, content={"message": "현재 비밀번호가 일치하지 않습니다."})


@app.get("/get-my-info")
async def get_my_info(request: Request):
    user_id = request.session.get('loginId')

    if not user_id:
        return JSONResponse(status_code=401, content={"message": "로그인이 필요합니다."})

    user = member.get_user_data(user_id)

    if user:

        birth_str = user.date_of_birth.strftime('%Y-%m-%d') if user.date_of_birth else ""

        return {
            "status": "success",
            "data": {
                "user_id":user_id,
                "name": user.user_name,
                "email": user.user_email,
                "birth": birth_str,
                "gender": user.user_gender
            }
        }

    return JSONResponse(status_code=404, content={"message": "유저 정보를 찾을 수 없습니다."})

@app.get("/topics", response_class=HTMLResponse)
async def topic_page(request: Request):
    return RedirectResponse(
        url=f"/view/polar.html",
        status_code=302
    )

@app.get("/api/topic")
def get_topics():
    result = topic.get_topic_from_es()
    return result

class TopicArticleReq(BaseModel):
    pos_ids: Optional[List[str]] = Field(default_factory=list)
    neg_ids: Optional[List[str]] = Field(default_factory=list)
    neu_ids: Optional[List[str]] = Field(default_factory=list)

@app.post("/api/topic_article")
def get_topic_article(body:TopicArticleReq):
    result = topic.get_topic_article(body)
    return result

@app.post("/api/search") # 검색 기능-----
async def api_search(request: Request):
    """기사 검색 API"""
    try:
        data = await request.json()# 데이터 다 읽을 때까지 기달
        search_type = data.get('search_type','all')
        # 프론트에서 all,title,content,keywords로 오는데 값이 없으면 all(제목+본문)으로
        query = data.get('query','').strip()
        # 사용자가 입력한 검색어
        size = data.get('size', 20)
        # 검색 결과 몇 개 가져올 지 결정하는 숫자

        if not query: # 검색어가 없다면.
            return JSONResponse(
                status_code=400,
                content={"success":False,"message":"검색어를 입력해주세요"}
            )
        results = member.search_articles(
            data.get('search_type','all'),
            data.get('query',''),
            data.get('size',20)
        )
        return results

    except Exception as e:
        print("search error: ",e)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e)}
        )

# 카테고리별로 불러오기------해정,하영님 합작

@app.get("/api/category/{category_name}")
async def get_category_articles(category_name: str, size: int = 20, page: int = 1):
    """카테고리별 기사 조회 API"""
    try:
        results = category.get_articles_by_category(category_name, size, page)
        return results
    except Exception as e:
        print(f"Category error: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e)}
        )

# main.py (FastAPI 예시)
@app.get("/api/wordcloud-data")
async def wordcloud_api():
    # 1. ES에서 실제 데이터 가져오기 (이전 단계에서 만든 로직)
    res = es.search(index="article_data", body={"size": 100, "sort": [{"collected_at": "desc"}]})
    bigkinds_data = [hit['_source'] for hit in res['hits']['hits']]

    # 2. 설계도(Option) 생성
    options_json = await make_wordcloud_data(bigkinds_data)
    # 3. 브라우저로 전송
    return json.loads(options_json)


@app.get("/api/main-trending")
async def get_main_trending():
    try:
        # search.py에서 결과 가져오기
        result = search.es_search_articles(search_type="all", query="", size=5)
        # 만약 결과가 딕셔너리고 그 안에 'articles' 리스트가 있다면
        if isinstance(result, dict) and result.get("success"):
            return {"success": True, "articles": result["articles"]}
        return {"success": False, "articles": [], "message": "No data found"}
    except Exception as e:
        return {"success": False, "articles": [], "error": str(e)}


@app.get("/api/related-articles")
async def get_related_articles(id: str, title: str = ""):  # 프론트에서 제목을 받아옵니다.
    try:
        # 제목(title)이 있다면 그 제목을 검색어로 사용해 연관된 기사를 찾습니다.
        # 검색어(title)가 있으면 'search_type="title"'로 유사도를 측정합니다.
        result = search.es_search_articles(search_type="title", query=title, size=5)

        if result.get("success"):
            # 현재 보고 있는 기사가 연관 기사 목록에 포함될 수 있으므로,
            # ID가 같은 기사는 제외하는 필터링을 거치면 더 완벽합니다.
            filtered_articles = [a for a in result["articles"] if a["article_id"] != id]
            return {"success": True, "articles": filtered_articles[:4]}  # 최종 4개 반환

        return {"success": False, "articles": []}
    except Exception as e:
        return {"success": False, "error": str(e)}
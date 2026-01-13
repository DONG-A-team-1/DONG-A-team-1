from fastapi import FastAPI, Form, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse

from starlette.middleware.sessions import SessionMiddleware
# router 연결(session)
from api.session_ping import router as session_ping_router
from api.session import router as session_router
from api.session_end import router as session_end_router
from api.recommend import router as recommend_router
from api.recommend_trend import recommend_trend_articles
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
app.include_router(recommend_router)
# static 파일
app.mount("/view", StaticFiles(directory="view"), name="view")
app.mount("/wordcloud", StaticFiles(directory="wordcloud"), name="wordcloud")
app.mount("/static", StaticFiles(directory="static"), name="static")

# middleware
app.add_middleware(SessionMiddleware, secret_key="your_secret_key")


@app.get("/")
async def read_root():
    return RedirectResponse(url="/view/home.html")  # 기본 메인페이지로 지정해야됨

@app.get("/api/recommend/trend")
def get_trend_recommend(limit: int = 5):
    return {
        "success": True,
        "articles": recommend_trend_articles(limit)
    }



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
        return {"status": "success", "message": "로그인 성공", "user_name": user_name}
    elif result == "INACTIVE":
        return JSONResponse(status_code=403, content={"message": "비활성화된 계정"})
    else:
        return JSONResponse(status_code=401, content={"message": "정보 불일치"})


@app.get("/logout")
async def logout(req: Request):
    # 1. 세션 데이터 완전히 삭제
    req.session.clear()
    # 2. 삭제 후 메인페이지로 이동
    return RedirectResponse(url="/view/home.html")  # 메인페이지에 맞게 형식 조정 필요


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
                "user_id": user_id,
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
        data = await request.json()  # 데이터 다 읽을 때까지 기달
        search_type = data.get('search_type', 'all')
        # 프론트에서 all,title,content,keywords로 오는데 값이 없으면 all(제목+본문)으로
        query = data.get('query', '').strip()
        # 사용자가 입력한 검색어
        size = data.get('size', 20)
        # 검색 결과 몇 개 가져올 지 결정하는 숫자

        if not query: # 검색어가 없다면.
            return JSONResponse(
                status_code=400,
                content={"success": False, "message": "검색어를 입력해주세요"}
            )
        results = member.search_articles(
            data.get('search_type', 'all'),
            data.get('query', ''),
            data.get('size', 20)
        )
        return results

    except Exception as e:
        print("search error: ", e)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e)}
        )

# 카테고리별로 불러오기------해정,하영님 합작

@app.get("/api/category/{category_name}")
async def get_category_articles(category_name: str, size: int = 20, page: int = 1, sort_type: str = "latest"):
    try:
        # sort_type을 넘겨줍니다.
        results = category.get_articles_by_category(category_name, size, page, sort_type)
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
        return search.es_search_trending_articles(size=5)
    except Exception as e:
        return {"success": False, "articles": [], "error": str(e)}


@app.get("/api/related-articles")
async def get_related_articles(id: str):
    """
    연관 기사 API
    기사 임베딩을 기준으로 의미가 비슷한 기사들을 반환
    """
    try:
        articles = search.es_search_related_by_embedding(
            article_id=id,
            size=4
        )

        return {
            "success": True,
            "articles": articles
        }

    except Exception as e:
        return {
            "success": False,
            "articles": [],
            "error": str(e)
        }

        return {"success": False, "error": str(e)}

@app.get("/api/user/history")
async def api_user_history(request: Request, date: str):
    user_id = request.session.get("loginId")

    if not user_id:
        return JSONResponse(status_code=401, content={"success": False})

    return member.get_user_history(user_id, date)

# 마이페이지 달력
@app.get("/api/user/monthly-activity")
async def get_activity(year: int, month: int, request: Request):
    # 1. 세션에서 로그인 아이디 가져오기
    user_id = request.session.get("loginId")

    if not user_id:
        return JSONResponse(
            status_code=401,
            content={"success": False, "message": "로그인이 필요합니다."}
        )

    try:
        # 2. member.py의 함수 호출 (이름 일치 확인: get_user_monthly_activity_stats)
        # member.py에서 (activity_data, total_views) 튜플을 반환하므로 두 변수로 받습니다.
        activity_map, total_views = member.get_user_monthly_activity_stats(user_id, year, month)

        return {
            "success": True,
            "activity": activity_map,
            "total_views": total_views
        }
    except Exception as e:
        logger.error(f"Monthly activity stats error: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "activity": {}, "total_views": 0, "error": str(e)}
        )
# 토픽 홈화면에 띄우기-------------------------------------
@app.get("/api/topic-opinion")
def get_topic_opinion():
    """
    홈 화면용: 1위 토픽과 해당 토픽의 긍정/부정 기사
    """
    try:
        # 1. 전체 토픽 가져오기
        topics = topic.get_topic_from_es()

        if not topics:
            return {
                "topic": "데이터 없음",
                "positive_articles": [],
                "negative_articles": []
            }

        # 2. 1위 토픽 선택
        top_topic = topics[0]
        topic_name = top_topic.get("topic_name", "토픽 없음")

        # 3. 긍정/부정 기사 객체 추출 (최대 2개씩)
        positive = top_topic.get("positive_articles", [])[:2]
        negative = top_topic.get("negative_articles", [])[:2]

        # 4. article_id 리스트 추출
        positive_ids = [art.get("article_id") for art in positive if art.get("article_id")]
        negative_ids = [art.get("article_id") for art in negative if art.get("article_id")]

        # 5. ES에서 실제 기사 정보 가져오기

        positive_articles = []
        if positive_ids:
            try:
                docs = article.get_article_from_es(
                    positive_ids,
                    SOURCE_FIELDS=["article_id", "article_title"],
                    max=len(positive_ids)
                )
                positive_articles = [
                    {
                        "article_id": doc.get("article_id"),
                        "title": doc.get("article_title", "제목 없음")
                    }
                    for doc in docs
                ]
            except Exception as e:
                logger.warning(f"긍정 기사 조회 실패: {e}")

        negative_articles = []
        if negative_ids:
            try:
                docs = article.get_article_from_es(
                    negative_ids,
                    SOURCE_FIELDS=["article_id", "article_title"],
                    max=len(negative_ids)
                )
                negative_articles = [
                    {
                        "article_id": doc.get("article_id"),
                        "title": doc.get("article_title", "제목 없음")
                    }
                    for doc in docs
                ]
            except Exception as e:
                logger.warning(f"부정 기사 조회 실패: {e}")

        return {
            "topic": topic_name,
            "positive_articles": positive_articles,
            "negative_articles": negative_articles
        }

    except Exception as e:
        logger.exception("topic-opinion 조회 실패")
        return JSONResponse(
            status_code=500,
            content={
                "topic": "에러 발생",
                "positive_articles": [],
                "negative_articles": []
            }
        )
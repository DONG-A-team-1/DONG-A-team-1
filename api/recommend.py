from fastapi import APIRouter, Query
from api.user_embedding import recommend_articles

router = APIRouter(prefix="/recommend", tags=["recommend"])

@router.get("")
def get_recommendations(
    user_id: str = Query(...),
    limit: int = Query(20)
):
    return {
        "user_id": user_id,
        "recommendations": recommend_articles(user_id, limit, random=True)
    }

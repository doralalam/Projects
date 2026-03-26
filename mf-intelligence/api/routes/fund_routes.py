from fastapi import APIRouter
from services.fund_service import fetch_funds_list, fetch_mom_pivot

router = APIRouter()

@router.get("/funds-list")
def get_funds():
    return fetch_funds_list()


@router.get("/fund/{amc}/{fund_name}/mom-pivot")
def get_pivot(amc: str, fund_name: str):
    return fetch_mom_pivot(amc, fund_name)
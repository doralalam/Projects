from fastapi import APIRouter
from services.mf_service import fetch_table, fetch_drilldown

router = APIRouter(prefix="/mf")

@router.get("/increased")
def increased(limit: int = 100):
    return fetch_table("mf_increased", limit)

@router.get("/decreased")
def decreased(limit: int = 100):
    return fetch_table("mf_decreased", limit)

@router.get("/fresh")
def fresh(limit: int = 100):
    return fetch_table("mf_fresh", limit)

@router.get("/exit")
def exit(limit: int = 100):
    return fetch_table("mf_exit", limit)

@router.get("/drilldown")
def drilldown(isin: str):
    return fetch_drilldown(isin)
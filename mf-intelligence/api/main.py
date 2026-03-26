from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes import fund_routes, mf_routes

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(fund_routes.router)
app.include_router(mf_routes.router)
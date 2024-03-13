from fastapi import FastAPI

from app.api.router import app

app = FastAPI()
app.include_router(app)
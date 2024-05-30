from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from app.api.router import router

templates = Jinja2Templates(directory="templates")

app = FastAPI()
app.include_router(router)
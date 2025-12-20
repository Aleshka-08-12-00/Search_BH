from typing import List, Dict, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
import pandas as pd

from app.database import get_dataframe
from app.functions import search_dataframe



router = APIRouter()
templates = Jinja2Templates(directory="app/templates")


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Простой рут для HTML-страницы поиска."""
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/search", response_class=HTMLResponse)
async def search_endpoint(request: Request, search: Optional[str] = None):
    """HTML-эндпойнт для поиска (форма на сайте)."""
    df = get_dataframe()

    if not search or not search.strip():
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "results": None,
                "message": "Empty field",
            },
        )

    result_df = search_dataframe(df, search)

    # финальный список полей в HTML
    if result_df.empty:
        results: List[Dict] = []
    else:
        cols = [c for c in ['code', 'name', 'barcode', 'Score'] if c in result_df.columns]
        results = result_df[cols].head(300).to_dict(orient="records")

    return templates.TemplateResponse(
        "index.html",
        {"request": request, "results": results, "message": "Success"},
    )


@router.get("/query", response_class=JSONResponse)
async def query_endpoint(request: Request, q: Optional[str] = None, producerids: Optional[str] = None):
    if q is None or not q.strip():
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required.")

    df = get_dataframe()

    if producerids:
        try:
            producer_ids = [int(num) for num in producerids.split(",") if str(num).strip().isdigit()]
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid 'producerids' format.")
        if "producerid" in df.columns:
            df = df[df["producerid"].isin(producer_ids)]

    result_df = search_dataframe(df, q)

    if result_df.empty:
        ids = None
    else:
        if "id" not in result_df.columns:
            raise HTTPException(status_code=500, detail="Dataframe does not contain 'id' column.")

        raw_ids = [item["id"] for item in result_df.to_dict(orient="records")]

        seen = set()
        unique_ids = []
        for x in raw_ids:
            key = x
            try:
                key = int(x)
            except Exception:
                pass
            if key in seen:
                continue
            seen.add(key)
            unique_ids.append(key)
            if len(unique_ids) >= 96:
                break

        ids = unique_ids

    return JSONResponse(content={"message": "ok", "data": ids})


@router.post("/batch_query")
async def batch_query(request: Request):
    payload = await request.json()
    items = payload.get("items")

    if not items or not isinstance(items, list):
        raise HTTPException(
            status_code=400,
            detail="Body must include 'items' as a non-empty list."
        )

    producerids = payload.get("producerids")

    df = get_dataframe()

    if producerids:
        producer_ids = [
            int(num)
            for num in str(producerids).split(",")
            if str(num).strip().isdigit()
        ]
        df_filtered_base = df[df["producerid"].isin(producer_ids)]
    else:
        df_filtered_base = df

    results = []

    for q in items:
        if not isinstance(q, str) or not q.strip():
            results.append({"query": q, "data": None, "name": None})
            continue

        # используем общий движок поиска с синонимами/фаззи/бустами
        result_df = search_dataframe(df_filtered_base, q)

        if result_df is None or result_df.empty:
            data = None
            name = None
        else:
            top_row = result_df.iloc[0]

            top_id = None
            if "id" in result_df.columns:
                try:
                    # приводим numpy.int64 -> обычный int
                    top_id = int(top_row["id"])
                except (TypeError, ValueError):
                    top_id = None

            data = [top_id] if top_id is not None else None

            if "name" in result_df.columns:
                name = str(top_row["name"])
            else:
                name = None

        results.append(
            {
                "query": q,
                "data": data,
                "name": name,
            }
        )

    return JSONResponse(
        content={
            "message": "ok",
            "results": results,
        }
    )
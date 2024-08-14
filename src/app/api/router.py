from fastapi import APIRouter, HTTPException, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from app.database import get_dataframe
import pandas as pd
from app.functions import search_with_fuzzy, convert_layout, transliterate, merge_and_sort_dataframes, sort_dataframes, simple_search


router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/search", response_class=HTMLResponse)
async def search_endpoint(request: Request, search: str = None):
    df = get_dataframe()
    if search:
        search_query = search.strip().split(' ')[0]
        # print("search query = ", search_query)
    else:
        return templates.TemplateResponse("index.html", {"request": request, "results": None, "message": "Empty field"})
    
    # print(simple_search(search_query, df))

    # Search logic
    if not search_query.isdigit():
        # print(simple_search(search_query, df))
        zero_df = simple_search(search_query, df)
        first_df = search_with_fuzzy(search_query, df)
        second_df = search_with_fuzzy(convert_layout(search_query), df)
        third_df = search_with_fuzzy(transliterate(search_query), df)
        fourth_df = search_with_fuzzy(transliterate(convert_layout(search_query)), df)

        pre_result_df = sort_dataframes(merge_and_sort_dataframes(zero_df, first_df, second_df, third_df, fourth_df))

        fifth_df = pd.DataFrame()
        try:
            # print(search.strip().split(' ')[1])
            fifth_df = pre_result_df[(pre_result_df['name'].astype(str).str.contains(str(search.strip().split(' ')[1]), case=False, na=False))]
            fifth_df['Score'] = fifth_df['Score'] + 20
            # print(len(fifth_df))
        except: 
            pass

        result_df = sort_dataframes(merge_and_sort_dataframes(zero_df, first_df, second_df, third_df, fourth_df, fifth_df))


        
    elif len(search_query) > 2:
        search_query = str(int(search_query))
        result_df = df[
            (df['code'].astype(str).str.contains(search_query, case=False, na=False)) |
            (df['name'].astype(str).str.contains(search_query, case=False, na=False)) |
            (df['barcode'].astype(str).str.contains(search_query, case=False, na=False))
        ]
        result_df['Score'] = 99
    else:
        search_query = str(int(search_query))
        result_df = df[
            (df['name'].astype(str).str.contains(search_query, case=False, na=False))
        ]
        result_df['Score'] = 99


    # Extract relevant fields and limit to first 100 results
    results = result_df[['code', 'name', 'barcode', 'Score']].head(300).to_dict(orient='records')

    return templates.TemplateResponse("index.html", {"request": request, "results": results, "message": "Success"})


@router.get("/query", response_class=HTMLResponse)
async def search_endpoint(request: Request, q: str = None, producerids: str = None):

    df = get_dataframe()

    if q is None:
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required.")
    
    elif producerids is None:
        df_filtered = df
        search_query = q.strip().split(' ')[0]
    
    else:
        search_query = q.strip().split(' ')[0]
        producer_ids = [int(num) for num in producerids.split(',')]
    
        df_filtered = df[df['producerid'].isin(producer_ids)]

    # Search logic
    if not search_query.isdigit():
        # print(simple_search(search_query, df))
        zero_df = simple_search(search_query, df_filtered)
        first_df = search_with_fuzzy(search_query, df_filtered)
        second_df = search_with_fuzzy(convert_layout(search_query), df_filtered)
        third_df = search_with_fuzzy(transliterate(search_query), df_filtered)
        fourth_df = search_with_fuzzy(transliterate(convert_layout(search_query)), df_filtered)

        pre_result_df = sort_dataframes(merge_and_sort_dataframes(zero_df, first_df, second_df, third_df, fourth_df))
        # print(search.strip().split(' '))
        fifth_df = pd.DataFrame()
        try:
            # print(search.strip().split(' ')[1])
            fifth_df = pre_result_df[(pre_result_df['name'].astype(str).str.contains(str(q.strip().split(' ')[1]), case=False, na=False))]
            fifth_df['Score'] = fifth_df['Score'] + 20
            # print(len(fifth_df))
        except: 
            pass# print('hui')
            pass # print('hui')

        result_df = sort_dataframes(merge_and_sort_dataframes(zero_df, first_df, second_df, third_df, fourth_df, fifth_df))
        
    elif len(search_query) > 2:
        search_query = str(int(search_query))
        result_df = df[
            (df['code'].astype(str).str.contains(search_query, case=False, na=False)) |
            (df['name'].astype(str).str.contains(search_query, case=False, na=False)) |
            (df['barcode'].astype(str).str.contains(search_query, case=False, na=False))
        ]
        result_df['Score'] = 99
    else:
        search_query = str(int(search_query))
        result_df = df[
            (df['name'].astype(str).str.contains(search_query, case=False, na=False))
        ]
        result_df['Score'] = 99

    # Extract relevant fields and limit to first 100 results
    results = [item['id'] for item in result_df.head(96).to_dict(orient='records')]

    response_data = {
        "message": "ok",
        "data": results
    }

    return JSONResponse(content=response_data)
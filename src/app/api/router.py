from fastapi import APIRouter
from app.schemas.response import ResponseOut
from app.database import df
from app.functions import search_with_fuzzy, convert_layout, transliterate, merge_and_sort_dataframes


app = APIRouter(responses={
        404: {"description": "Not found"}}
    )


@app.get("/hello/")
async def say_hello():
    ResponseOut(data='ok', message='ok')


@app.get("/search/")
async def search_endpoint(search):

    if search:
        search_query = search
    else:
        ResponseOut(data=[], message='empty field')

    # seach if not a number
    if not search_query.isdigit():
        # different types of input words 
        first_df = search_with_fuzzy(search_query, df)
        second_df = search_with_fuzzy(convert_layout(search_query), df)
        third_df = search_with_fuzzy(transliterate(search_query), df)
        fourth_df = search_with_fuzzy(transliterate(convert_layout(search_query)), df)

        # join all results
        result_df = merge_and_sort_dataframes(first_df, second_df, third_df, fourth_df)

    # search if a number 
    else:
        search_query = str(int(search_query))  # delete 0 from the beginning

        # search for an input number in 'code', 'name' and 'barcode'
        result_df = df[
            (df['code'].astype(str).str.contains(search_query, case=False, na=False)) |
            (df['name'].astype(str).str.contains(search_query, case=False, na=False)) |
            (df['barcode'].astype(str).str.contains(search_query, case=False, na=False))
        ]

    # make a list of blockElementIds 
    result = result_df['blockElementId'].tolist()

    return ResponseOut(data=result, message="success")

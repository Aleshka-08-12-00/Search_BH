from typing import Union
from pydantic import BaseModel


class ResponseOut(BaseModel):
    data: Union[str, list] = ""
    message: str = ""
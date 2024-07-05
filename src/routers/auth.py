from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi.security.oauth2 import OAuth2PasswordRequestForm

from ..supabase import login as supabase_login

router = APIRouter()

@router.post("/token")
async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]):
    """
    This login function is not used in the Forntend. 

    It is needed for the documentation, to actually test the API and for all 
    use-cases where a Python script is uploading data to the API.
    
    """
    # use the supabase login to verify the token there
    response = supabase_login(form_data.username, form_data.password)

    # return the response as FastAPI needs it
    return {"access_token": response.session.access_token, "token_type": "bearer"}

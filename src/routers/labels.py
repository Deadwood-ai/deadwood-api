from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer


# create the router for the labels
router = APIRouter()

# create the OAuth2 password scheme for supabase login
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


@router.post("/{dataset_id}/labels")
def create_new_labels(dataset_id: int, token: Annotated[str, Depends(oauth2_scheme)]):
    pass


@router.put("/{dataset_id}/labels/{label_id}")
def upsert_label(dataset_id: int, label_id: int, token: Annotated[str, Depends(oauth2_scheme)]):
    pass

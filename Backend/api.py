from fastapi import FastAPI, Depends, HTTPException, status, Body, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, timezone
from typing import Annotated

import sys
import asyncio
import json
import os

# Add parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from Backend.database import engine, get_db, Base
from Backend.models import User, Customer, Account, Transaction, Complaint 
from Backend.schemas import UserCreate, UserResponse, Token, UserLogin
from Backend.auth import authenticate_user, create_access_token, get_password_hash, ACCESS_TOKEN_EXPIRE_MINUTES, verify_password
import uvicorn

# Initialize DB Tables
Base.metadata.create_all(bind=engine)

router = APIRouter(tags=["Authentication"])





@router.post("/auth/register", response_model=UserResponse)
def register(user: UserCreate, db: Session = Depends(get_db)):
    db_user = db.query(User).filter((User.username == user.username) | (User.email == user.email)).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Username or Email already registered")
    
    hashed_password = get_password_hash(user.password)
    new_user = User(
        username=user.username,
        email=user.email,
        hashed_password=hashed_password,
        age=user.age
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user

@router.post("/auth/token", response_model=Token)
def login_for_access_token(form_data: Annotated[OAuth2PasswordRequestForm, Depends()], db: Session = Depends(get_db)):
    # Authenticate user
    # Note: OAuth2PasswordRequestForm puts the username/email in the `username` field
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username/email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

from Backend.middleware import get_current_user

# ...

@router.post("/auth/refresh", response_model=Token)
def refresh_token(current_user: Annotated[dict, Depends(get_current_user)]):
    """
    Refresh the access token.
    Uses the existing valid access token to generate a new one (Sliding Session).
    """
    username = current_user.get("sub")
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@router.post("/auth/forgot-password")
def forgot_password(email: str = Body(..., embed=True)):
    print(f"Password reset requested for {email}")
    return {"message": "If the email exists, a reset link has been sent."}


@router.get("/")
def home():
    return {"message": "Bank System API is Online. Go to /docs for Swagger UI."}



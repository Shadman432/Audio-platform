from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import Dict, Any
from pydantic import BaseModel, EmailStr
import logging

from ..database import get_db
from ..auth import create_user_token
from ..auth.dependencies import get_current_active_user
from ..models.users import User

logger = logging.getLogger(__name__)
router = APIRouter()

# Pydantic schemas
class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: str
    email: str

class UserResponse(BaseModel):
    id: str
    email: str
    full_name: str = None
    created_at: str = None

class CreateTokenRequest(BaseModel):
    user_id: str
    email: EmailStr

@router.post("/create-token", response_model=TokenResponse)
async def create_token_for_user(
    request: CreateTokenRequest,
    db: Session = Depends(get_db)
):
    """
    Create a JWT token for a user (for testing purposes)
    """
    try:
        # Verify user exists in the database
        user = db.query(User).filter(User.id == request.user_id).first()
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found in authentication system"
            )
        
        # Create token
        token = create_user_token(request.user_id, request.email)
        
        return TokenResponse(
            access_token=token,
            user_id=request.user_id,
            email=request.email
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating token: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not create token"
        )

@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: Dict[str, Any] = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """Get current authenticated user information"""
    try:
        user = db.query(User).filter(User.id == current_user["id"]).first()
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )

        return UserResponse(
            id=str(user.id),
            email=user.email,
            full_name=user.full_name,
            created_at=str(user.created_at)
        )
    except Exception as e:
        logger.error(f"Error getting user info: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Could not retrieve user information"
        )

@router.post("/verify-token")
async def verify_user_token(
    current_user: Dict[str, Any] = Depends(get_current_active_user)
):
    """Verify if the provided token is valid"""
    return {
        "valid": True,
        "user_id": current_user["id"],
        "email": current_user.get("email"),
        "expires_at": current_user.get("exp")
    }

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional, Dict, Any
import logging
from sqlalchemy.orm import Session

from .jwt_handler import get_current_user
from ..database import get_db
from ..models.users import User

logger = logging.getLogger(__name__)

# Security scheme
security = HTTPBearer(auto_error=False)

async def get_current_active_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """
    Dependency to get current authenticated user (required)
    Raises 401 if no valid token is provided
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    token_data = get_current_user(credentials.credentials)
    if not token_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user = db.query(User).filter(User.user_id == token_data["id"]).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return token_data

async def get_optional_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: Session = Depends(get_db)
) -> Optional[Dict[str, Any]]:
    """
    Dependency to get current user (optional)
    Returns None if no valid token is provided
    """
    if not credentials:
        return None
    
    token_data = get_current_user(credentials.credentials)
    if not token_data:
        return None
    
    user = db.query(User).filter(User.id == token_data["id"]).first()
    if not user:
        return None
    
    return token_data

async def get_user_id_from_token(
    current_user: Dict[str, Any] = Depends(get_current_active_user)
) -> str:
    """Extract user ID from authenticated user"""
    return current_user["id"]

async def get_optional_user_id(
    current_user: Optional[Dict[str, Any]] = Depends(get_optional_current_user)
) -> Optional[str]:
    """Extract user ID from optional authenticated user"""
    return current_user["id"] if current_user else None

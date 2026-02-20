from fastapi import Depends, HTTPException, status, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from Backend.auth import verify_access_token


oauth2_scheme = HTTPBearer()


#Protected routes
def get_current_user(token: HTTPAuthorizationCredentials = Security(oauth2_scheme) ):
    """FastAPI Dependency to protect routes. """
    payload = verify_access_token(token.credentials)
    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload

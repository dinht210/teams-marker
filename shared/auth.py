import os
import json
import jwt
from jwt import PyJWKClient

jwt_tenant_id = os.getenv("JWT_TENANT_ID")
jwt_audience = os.getenv("JWT_AUDIENCE")

def validate_bearer(auth_header: str) -> dict:
    if not auth_header or not auth_header.startswith("Bearer "):
        raise ValueError("Invalid or missing Authorization header")
    
    # gets token from authorization header
    token = auth_header.split(" ")[1]
    jwks_url = f"https://login.microsoftonline.com/{jwt_tenant_id}/discovery/v2.0/keys"
    jwk_client = PyJWKClient(jwks_url)
    signing_key = jwk_client.get_signing_key_from_jwt(token)
    
    # validates token
    try:
        decoded_token = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=jwt_audience,
            options={"verify_exp": True}
        )

        resp = {
            "sub": decoded_token.get("sub"),
            "oid": decoded_token.get("oid"),
            "preferred_username": decoded_token.get("preferred_username"),
            "name": decoded_token.get("name")
        }
        return json.dumps(resp)
    except jwt.ExpiredSignatureError:
        raise ValueError("Token has expired")
    except jwt.InvalidTokenError as e:
        raise ValueError(f"Invalid token: {str(e)}")
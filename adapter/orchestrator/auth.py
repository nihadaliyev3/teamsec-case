"""
Tenant API token handling - hash storage, constant-time verification.
Never store raw tokens; only store SHA-256 hashes.
"""
import hashlib
import hmac
import secrets


def hash_token(raw_token: str) -> str:
    """Produce SHA-256 hex digest of the token. Deterministic."""
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def verify_token(raw_token: str, stored_hash: str) -> bool:
    """
    Constant-time comparison to prevent timing attacks.
    Returns True iff raw_token hashes to stored_hash.
    """
    if not raw_token or not stored_hash:
        return False
    computed = hash_token(raw_token)
    return hmac.compare_digest(computed, stored_hash)


def generate_token() -> str:
    """Generate a cryptographically secure API token (64 hex chars)."""
    return secrets.token_hex(32)

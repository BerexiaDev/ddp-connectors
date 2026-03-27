import os
from cryptography.fernet import Fernet
from loguru import logger

def _get_key() -> bytes:
    key = os.environ.get("SECRET_KEY")
    if not key:
        raise RuntimeError("SECRET_KEY environment variable is not set")
    return key.encode()  # assumes key is stored as a base64 string

def encrypt_secret(secret: str) -> str:
    cipher = Fernet(_get_key())
    return cipher.encrypt(secret.encode()).decode()

def decrypt_secret(encrypted_secret: str | None) -> str | None:
    if not encrypted_secret:
        return None
    
    try:
        cipher = Fernet(_get_key())
        return cipher.decrypt(encrypted_secret.encode()).decode()
    except Exception as e:
        # Log the error and return None if decryption fails
        logger.error(f"Decryption failed: invalid token or wrong key: {e}")
        raise ValueError(f"Decryption failed: invalid token or wrong key: {e}")
"""
kalshi_io/client.py — Authenticated Kalshi SDK client and REST session.

Credentials are read lazily on first use, not at import time, so read-only
code paths (e.g. pull_audit) work without a configured .env.

Exports:
    get_client()  — KalshiClient (SDK, authenticated), cached after first call
    get_session() — requests.Session (REST fallback endpoints), cached
    BASE_URL      — API base URL
"""

import os
from functools import lru_cache

import requests
from dotenv import load_dotenv
from kalshi_python_sync import Configuration, KalshiClient

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


@lru_cache(maxsize=1)
def get_client() -> KalshiClient:
    """Build the authenticated SDK client on first use (reads .env)."""
    load_dotenv()

    key_path = os.getenv("KALSHI_KEY_PATH")
    if not key_path:
        raise RuntimeError("KALSHI_KEY_PATH not set in environment / .env")
    with open(key_path, "r") as f:
        private_key = f.read()

    api_key_id = os.getenv("KALSHI_API_KEY_ID")
    if not api_key_id:
        raise RuntimeError("KALSHI_API_KEY_ID not set in environment / .env")

    config = Configuration(host=BASE_URL)
    config.api_key_id = api_key_id
    config.private_key_pem = private_key
    return KalshiClient(config)


@lru_cache(maxsize=1)
def get_session() -> requests.Session:
    """Shared REST session for historical / public endpoints.

    These endpoints currently work without auth; if one starts returning
    401, add an auth adapter here.
    """
    return requests.Session()

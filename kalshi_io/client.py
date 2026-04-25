"""
kalshi_io/client.py — Authenticated Kalshi SDK client and REST session.

Exports:
    client   — KalshiClient (SDK, authenticated)
    session  — requests.Session (for REST fallback endpoints)
    BASE_URL — API base URL
"""

import os

import requests
from dotenv import load_dotenv
from kalshi_python_sync import Configuration, KalshiClient

load_dotenv()

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Read RSA private key
_key_path = os.getenv("KALSHI_KEY_PATH")
if not _key_path:
    raise RuntimeError("KALSHI_KEY_PATH not set in environment / .env")
with open(_key_path, "r") as f:
    _private_key = f.read()

_api_key_id = os.getenv("KALSHI_API_KEY_ID")
if not _api_key_id:
    raise RuntimeError("KALSHI_API_KEY_ID not set in environment / .env")

# SDK client (authenticated via RSA key)
_config = Configuration(host=BASE_URL)
_config.api_key_id = _api_key_id
_config.private_key_pem = _private_key

client: KalshiClient = KalshiClient(_config)

# REST session for historical / public endpoints.
# Reference scripts use bare requests.get() without auth and it works.
# If a REST endpoint returns 401 later, add auth adapter and log in DECISIONS.md.
session: requests.Session = requests.Session()

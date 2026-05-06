import os

QASE_TOKEN: str = os.environ.get(
    "QASE_TOKEN",
    "afc1d577c0b482d0f7b2e85f0183acf2aff13ddc86d7eb07d57f4be5ebc49161",
)
QASE_API_BASE = "https://api.qase.io/v1"

# Basic Auth отключён (оставьте BASIC_AUTH_USER пустым для отключения)
BASIC_AUTH_USER: str = os.environ.get("BASIC_AUTH_USER", "")
BASIC_AUTH_PASS: str = os.environ.get("BASIC_AUTH_PASS", "")

import json
import os
import sys
from urllib.error import URLError
from urllib.request import urlopen


def main() -> int:
    port = os.getenv("PORT", "8056").strip() or "8056"
    url = f"http://127.0.0.1:{port}/health"

    try:
        with urlopen(url, timeout=5) as response:
            if response.status != 200:
                return 1
            payload = json.load(response)
    except (OSError, URLError, ValueError, json.JSONDecodeError):
        return 1

    return 0 if payload.get("status") == "healthy" else 1


if __name__ == "__main__":
    raise SystemExit(main())

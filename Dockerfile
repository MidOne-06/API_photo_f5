FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8056 \
    SESSION_FILE=/app/data/session_bot_ft \
    STATE_FILE=/app/data/api_state.json

WORKDIR /app

RUN addgroup --system app && adduser --system --ingroup app app

COPY requirements.txt ./requirements.txt

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY main.py ./main.py
COPY docker/healthcheck.py ./docker/healthcheck.py

RUN mkdir -p /app/data \
    && chown -R app:app /app

USER app

EXPOSE 8056

HEALTHCHECK --interval=30s --timeout=5s --start-period=45s --retries=3 CMD ["python", "/app/docker/healthcheck.py"]

CMD ["python", "main.py"]

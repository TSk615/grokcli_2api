# grokcli-2api
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Shanghai \
    GROK2API_HOST=0.0.0.0 \
    GROK2API_PORT=3000 \
    GROK2API_OPEN_BROWSER=0 \
    GROK2API_STORE_BACKEND=hybrid \
    GROK2API_WORKERS=2 \
    HOME=/root \
    DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# App tools
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        tzdata \
    && ln -snf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo Asia/Shanghai > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
COPY requirements-store.txt /app/requirements-store.txt
RUN python -m pip install --no-cache-dir -U pip setuptools wheel \
    && python -m pip install --no-cache-dir -r /app/requirements.txt \
    && python -m pip install --no-cache-dir -r /app/requirements-store.txt

COPY . /app
RUN chmod +x /app/entrypoint.sh \
    && test -f /app/grok2api/app.py \
    && test -f /app/app.py \
    && python -c "import app; import grok2api.app as pkg_app; print('build-check', pkg_app.APP_VERSION, app.APP_VERSION)"

EXPOSE 3000

# data/ only for optional JSON import artifacts / models cache
VOLUME ["/app/data"]

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["python", "app.py"]

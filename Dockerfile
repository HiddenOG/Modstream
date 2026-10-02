# syntax=docker/dockerfile:1
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/opt/models

WORKDIR /app

# CPU-only PyTorch keeps the image ~4x smaller than the default CUDA build.
RUN pip install --index-url https://download.pytorch.org/whl/cpu torch
COPY requirements.txt .
RUN pip install -r requirements.txt

# Bake model weights into the image so containers start without a download.
RUN python -c "from detoxify import Detoxify; Detoxify('original')" \
    && chmod -R a+rX /opt/models

COPY . .
RUN useradd --create-home app && mkdir -p instance && chown -R app instance
USER app

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/v1/health')"

CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:8000", "--worker-class", "gthread", \
     "--workers", "1", "--threads", "8", "--timeout", "120"]

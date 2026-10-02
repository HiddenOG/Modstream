# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/opt/models \
    TORCH_HOME=/opt/models/torch

WORKDIR /app

# CPU-only PyTorch keeps the image ~4x smaller than the default CUDA build.
RUN pip install --index-url https://download.pytorch.org/whl/cpu torch
COPY requirements-core.txt requirements.txt ./
RUN pip install -r requirements.txt

# Bake model weights into the image so containers start without a download.
RUN python -c "from detoxify import Detoxify; Detoxify('unbiased')" && chmod -R a+rX /opt/models
# Weights are baked in, so never contact Hugging Face at runtime (an online check costs minutes per start).
ENV HF_HUB_OFFLINE=1

COPY . .
RUN useradd --create-home app && mkdir -p instance && chown -R app instance
USER app

EXPOSE 8000 9100
HEALTHCHECK --interval=15s --timeout=3s --start-period=60s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/v1/health')"

# One process per container; scale with replicas. uvloop + httptools come with uvicorn[standard].
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", \
     "--forwarded-allow-ips", "*", "--no-access-log", "--ws-per-message-deflate", "false"]

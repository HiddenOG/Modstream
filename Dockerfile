# syntax=docker/dockerfile:1
#
#   docker build .                              # rules + RoBERTa model (~2 GB image, ~1-1.5 GB RAM)
#   docker build --build-arg WITH_MODEL=false . # rules only (small image, ~150 MB RAM)
#
# On Railway, set a service variable WITH_MODEL=false to build the rules-only image.
FROM python:3.12-slim

ARG WITH_MODEL=true

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/opt/models \
    TORCH_HOME=/opt/models/torch \
    # auto = use the model if it is installed, otherwise rules only
    MODSTREAM_SCORER=auto \
    # Weights are baked in at build time; never contact Hugging Face at runtime
    # (an online check costs minutes per start).
    HF_HUB_OFFLINE=1

WORKDIR /app

COPY requirements-core.txt requirements.txt ./
# CPU-only PyTorch keeps the image ~4x smaller than the default CUDA build.
# Model weights are downloaded at build time (with HF_HUB_OFFLINE lifted) so containers start fast.
RUN if [ "$WITH_MODEL" = "true" ]; then \
        pip install --index-url https://download.pytorch.org/whl/cpu torch \
        && pip install -r requirements.txt \
        && HF_HUB_OFFLINE=0 python -c "from detoxify import Detoxify; Detoxify('unbiased')" \
        && chmod -R a+rX /opt/models; \
    else \
        pip install -r requirements-core.txt; \
    fi

COPY . .
RUN useradd --create-home app && mkdir -p instance && chown -R app instance
USER app

# Hosts such as Railway and Heroku say which port to listen on via $PORT; default 8000.
ENV PORT=8000
EXPOSE 8000 9100
HEALTHCHECK --interval=15s --timeout=3s --start-period=60s \
  CMD python -c "import os, urllib.request; urllib.request.urlopen(f'http://localhost:{os.environ[\"PORT\"]}/api/v1/health')"

# One process per container; scale with replicas. uvloop + httptools come with uvicorn[standard].
CMD ["sh", "-c", "exec uvicorn app:app --host 0.0.0.0 --port ${PORT} --proxy-headers --forwarded-allow-ips '*' --no-access-log --ws-per-message-deflate false"]

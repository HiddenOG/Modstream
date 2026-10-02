"""ASGI entry point: `uvicorn app:app`, or `python app.py` for local development."""

from modstream import create_app

app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="127.0.0.1", port=8000, ws_per_message_deflate=False)

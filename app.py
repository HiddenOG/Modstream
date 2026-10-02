"""Entry point: `python app.py` for development, `gunicorn app:app` in production."""

import os

from cybershield import create_app

app = create_app()

if __name__ == "__main__":
    app.run(debug=os.environ.get("FLASK_DEBUG") == "1", threaded=True)

# api/index.py
import os
import sys

# Ensure repo root is importable when running from /api
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app import app  # your root-level app.py defines app = create_app()
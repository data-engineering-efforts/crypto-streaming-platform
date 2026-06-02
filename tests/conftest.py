"""
Shared pytest setup.

The producers/ and flink_jobs/ modules use flat imports
(e.g. `from base_producer import ...`, `from shared.config import ...`),
so we put those package roots on sys.path for the test session.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for sub in ("producers", "flink_jobs"):
    path = str(ROOT / sub)
    if path not in sys.path:
        sys.path.insert(0, path)

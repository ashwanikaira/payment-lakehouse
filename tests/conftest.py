import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for sub in ("jobs", "producer"):
    sys.path.insert(0, str(ROOT / sub))

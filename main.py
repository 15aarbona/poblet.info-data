#!/usr/bin/env python3
"""
Neteja (staging) + transformació (magatzem + JSON per al dashboard web).

    .venv/bin/python main.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "pipeline"))

from extractors.DataCleaner import DataCleaner  # noqa: E402
from extractors.DataTransformer import DataTransformer  # noqa: E402


def main() -> None:
    DataCleaner().process()
    DataTransformer().process()


if __name__ == "__main__":
    main()

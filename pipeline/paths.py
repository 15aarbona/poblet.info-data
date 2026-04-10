"""Rutes estables respecte a l'arrel del repositori (independent del directori de treball)."""

from pathlib import Path


def repo_root() -> Path:
    """Retorna el directori arrel del projecte (pare de `pipeline/`)."""
    return Path(__file__).resolve().parent.parent

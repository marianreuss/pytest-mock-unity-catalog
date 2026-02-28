import os
from pathlib import Path

IS_DATABRICKS = os.getenv("DATABRICKS_RUNTIME_VERSION") is not None

_VOLUMES_PREFIX = "/Volumes/"


def _remap_volume_path(path: str, base: Path) -> str:
    """Remap a ``/Volumes/...`` string to the local temp volume root."""
    if path.startswith(_VOLUMES_PREFIX):
        return str(base / path[len(_VOLUMES_PREFIX) :])
    return path


def _table_name_to_path(base_path: Path, table_name: str) -> Path:
    """Map Unity Catalog table name to local directory path."""
    return base_path / table_name.replace(".", os.sep)

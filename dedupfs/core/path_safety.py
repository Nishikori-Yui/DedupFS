from __future__ import annotations

from pathlib import Path


class PathSafetyError(ValueError):
    pass


def validate_library_relative_path(raw_path: str) -> Path:
    if raw_path.startswith("/"):
        raise PathSafetyError("Path must be relative to /libraries")
    if ".." in Path(raw_path).parts:
        raise PathSafetyError("Path traversal is not allowed")
    if "~" in raw_path:
        raise PathSafetyError("Home expansion is not allowed")
    if "$" in raw_path:
        raise PathSafetyError("Environment variable expansion is not allowed")
    return Path(raw_path)


def resolve_under_libraries(libraries_root: Path, raw_path: str) -> Path:
    rel = validate_library_relative_path(raw_path)
    candidate = (libraries_root / rel).resolve(strict=False)
    root = libraries_root.resolve(strict=False)

    if candidate == root or root in candidate.parents:
        return candidate

    raise PathSafetyError("Path escapes libraries root")

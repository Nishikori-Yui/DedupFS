from __future__ import annotations

import pytest

from dedupfs.core.path_safety import PathSafetyError, validate_library_relative_path


@pytest.mark.parametrize(
    "raw_path",
    [
        "../evil.bin",
        "nested/../../escape.bin",
        "~/private.bin",
        "$HOME/private.bin",
        "/absolute/path.bin",
    ],
)
def test_validate_library_relative_path_rejects_unsafe_input(raw_path: str) -> None:
    with pytest.raises(PathSafetyError):
        validate_library_relative_path(raw_path)


def test_validate_library_relative_path_accepts_normal_relative_path() -> None:
    resolved = validate_library_relative_path("media/photo.jpg")
    assert resolved.as_posix() == "media/photo.jpg"

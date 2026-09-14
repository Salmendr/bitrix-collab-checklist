"""One naming policy for employee and public uploads."""
import re
from pathlib import Path


def safe_file_name(value: str) -> str:
    name = str(value or "file.bin").replace("\\", "/").rsplit("/", 1)[-1].strip()
    if not name or name in {".", ".."} or any(ord(c) < 32 for c in name):
        raise ValueError("Недопустимое имя файла")
    return name


def unique_file_name(file_name: str, occupied_names) -> str:
    original = safe_file_name(file_name)
    occupied = {str(n or "").strip().casefold() for n in occupied_names}
    if original.casefold() not in occupied:
        return original
    suffix = Path(original).suffix
    base = original[:-len(suffix)] if suffix else original
    match = re.match(r"^(.*) \((\d+)\)$", base)
    number = 2
    if match and int(match.group(2)) >= 2:
        base, number = match.group(1), int(match.group(2)) + 1
    while f"{base} ({number}){suffix}".casefold() in occupied:
        number += 1
    return f"{base} ({number}){suffix}"

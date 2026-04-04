import re
import time
import unicodedata
from collections.abc import Iterable


def transliterate_for_id(value: str) -> str:
    cyr_map = {
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e", "ж": "zh", "з": "z",
        "и": "i", "й": "y", "к": "k", "л": "l", "м": "m", "н": "n", "о": "o", "п": "p", "р": "r",
        "с": "s", "т": "t", "у": "u", "ф": "f", "х": "h", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch",
        "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
    }
    out = []
    for ch in str(value or ""):
        low = ch.lower()
        if low in cyr_map:
            chunk = cyr_map[low]
            if ch.isupper():
                chunk = chunk.capitalize()
            out.append(chunk)
        else:
            out.append(ch)
    txt = "".join(out)
    return unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii")


def profile_id_from_name(name: str) -> str:
    raw = transliterate_for_id(name).lower()
    raw = re.sub(r"[\s\-]+", "_", raw)
    raw = re.sub(r"[^a-z0-9_]+", "_", raw)
    raw = re.sub(r"_+", "_", raw).strip("_")
    if raw:
        return raw

    # Deterministic fallback for non-Latin names that cannot be ASCII-transliterated.
    tokens = []
    for ch in str(name or ""):
        if ch.isspace() or ch in "-_":
            continue
        tokens.append(f"u{ord(ch):x}")
    tokenized = "_".join(tokens).strip("_")
    if tokenized:
        return tokenized.lower()
    return f"profile_{int(time.time())}"


def unique_profile_id(base_id: str, existing_ids: Iterable[str], *, exclude_id: str | None = None) -> str:
    base = str(base_id or "").strip().lower() or f"profile_{int(time.time())}"
    occupied = {str(pid or "").strip().lower() for pid in existing_ids if str(pid or "").strip()}
    if exclude_id:
        occupied.discard(str(exclude_id).strip().lower())
    if base not in occupied:
        return base
    suffix = 2
    while f"{base}_{suffix}" in occupied:
        suffix += 1
    return f"{base}_{suffix}"

# Rules: edit rules_data.json (UTF-8). Values are ["base product name", factor].
from __future__ import annotations

import json
import os

_DIR = os.path.dirname(os.path.abspath(__file__))
_JSON_PATH = os.path.join(_DIR, "rules_data.json")


def _rules_rest_key(restaurant: str) -> str:
    return (
        str(restaurant or "")
        .lower()
        .replace("\u0131", "i")
        .replace("i\u0307", "i")
        .strip()
        .upper()
    )


def _as_pair(v):
    if isinstance(v, (list, tuple)) and len(v) == 2:
        return (str(v[0]), float(v[1]))
    raise TypeError("rule must be [name, factor]")


def _load_tables():
    if not os.path.isfile(_JSON_PATH):
        return {}, {}
    try:
        with open(_JSON_PATH, "rb") as f:
            data = json.loads(f.read().decode("utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return {}, {}
    try:
        cr = data.get("SPECIAL_RULES_COMMON") or {}
        br = data.get("SPECIAL_RULES_BY_RESTAURANT") or {}
        common = {str(k): _as_pair(v) for k, v in cr.items()}
        by_rest = {
            str(rk): {str(k): _as_pair(v) for k, v in (sub or {}).items()}
            for rk, sub in br.items()
        }
    except (TypeError, ValueError, KeyError):
        return {}, {}
    return common, by_rest


SPECIAL_RULES_COMMON, SPECIAL_RULES_BY_RESTAURANT = _load_tables()


def merged_special_rules(restaurant: str) -> dict:
    """COMMON + digər restoranların (çatışmayan açarlar) + son seçilmiş restoran üstündür.
    ROOM seçilib BIBLIOTEKA qaydaları COMMON-da yoxdursa, yenə tətbiq olunur."""
    rk = _rules_rest_key(restaurant)
    out = dict(SPECIAL_RULES_COMMON)
    for rname, block in sorted(SPECIAL_RULES_BY_RESTAURANT.items()):
        if _rules_rest_key(rname) == rk:
            continue
        for k, v in (block or {}).items():
            if k not in out:
                out[k] = v
    sel = SPECIAL_RULES_BY_RESTAURANT.get(rk) or {}
    for k, v in sel.items():
        out[k] = v
    return out


SPECIAL_RULES = SPECIAL_RULES_COMMON

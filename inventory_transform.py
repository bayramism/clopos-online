"""Clopos inventar export — addım 1: B,D,F,K,L,O,P,Q,U sütunlarının silinməsi + Kateqoriya A→Z."""

from __future__ import annotations

import io
from typing import Optional, Tuple

import pandas as pd

# Plan üzrə silinən başlıqlar (Excel B,D,F,K,L,O,P,Q,U)
_DROP_HEADERS = (
    "Type",
    "Vahid",
    "Son yoxlama",
    "Köçürmə",
    "İstehsal",
    "Hazırlamalardan gələn",
    "Toplam qalıq",
    "Ümumi maya dəyəri",
    "QUANTITY",
)

_SORT_COLUMN = "Kateqoriya"


def _norm_col(s: object) -> str:
    return str(s).strip().casefold()


def _map_drop_columns(df: pd.DataFrame) -> list[str]:
    norm_to_actual: dict[str, str] = {}
    for c in df.columns:
        norm_to_actual.setdefault(_norm_col(c), str(c).strip())
    drop_actual: list[str] = []
    for want in _DROP_HEADERS:
        nw = _norm_col(want)
        if nw in norm_to_actual:
            drop_actual.append(norm_to_actual[nw])
    return drop_actual


def process_inventory_categorization_step(
    orig_xlsx: bytes,
) -> Tuple[Optional[bytes], Optional[str]]:
    """Orijinal .xlsx → göstərilən sütunlar silinir, sonra Kateqoriya üzrə A–Z (mətn sırası).

    Qaytarır: (emal olunmuş faylın baytları, None) və ya (None, xəta mətni).
    """
    try:
        buf = io.BytesIO(orig_xlsx)
        xlf = pd.ExcelFile(buf, engine="openpyxl")
        sheet = xlf.sheet_names[0]
        df = pd.read_excel(xlf, sheet_name=sheet, engine="openpyxl")
    except Exception as e:
        return None, f"Excel oxunmadı: {e}"

    if df.empty:
        return None, "Cədvəl boşdur."

    sort_col = None
    for c in df.columns:
        if _norm_col(c) == _norm_col(_SORT_COLUMN):
            sort_col = c
            break
    if sort_col is None:
        preview = ", ".join(map(str, list(df.columns)[:20]))
        return None, f"'{_SORT_COLUMN}' sütunu tapılmadı. Sütunlar: {preview}"

    drop_cols = _map_drop_columns(df)
    df = df.drop(columns=drop_cols, errors="ignore")

    df = df.copy()
    df["__inv_sort_k"] = df[sort_col].map(
        lambda x: str(x).strip().casefold() if pd.notna(x) else "\uffff"
    )
    df = df.sort_values("__inv_sort_k", kind="mergesort").drop(
        columns=["__inv_sort_k"]
    )

    safe_sheet = str(sheet)[:31] if sheet else "Sheet1"
    out = io.BytesIO()
    try:
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=safe_sheet, index=False)
    except Exception as e:
        return None, f"Excel yazılmadı: {e}"
    return out.getvalue(), None

"""Clopos inventar export — addım 1: Excel A,B,D,F,K,L,O,P,Q,U silinir; A→Z sıra qalan A sütunundadır (Kateqoriya)."""

from __future__ import annotations

import io
from typing import Optional, Tuple

import pandas as pd

# Orijinal faylda Excel hərfləri A,B,D,F,K,L,O,P,Q,U → 0-based indekslər
_INVENTORY_DROP_COL_INDEXES: tuple[int, ...] = (
    0,
    1,
    3,
    5,
    10,
    11,
    14,
    15,
    16,
    20,
)

# Silinmələrdən sonra Excel A = cədvəlin birinci sütunu (şablonda Kateqoriya)
_SORT_COL_INDEX_AFTER_DROPS = 0


def _drop_columns_by_original_positions(
    df: pd.DataFrame, zero_based_indices: tuple[int, ...]
) -> pd.DataFrame:
    cols = list(df.columns)
    names: list[str] = []
    for i in zero_based_indices:
        if 0 <= i < len(cols):
            names.append(cols[i])
    if not names:
        return df
    return df.drop(columns=list(dict.fromkeys(names)), errors="ignore")


def process_inventory_categorization_step(
    orig_xlsx: bytes,
) -> Tuple[Optional[bytes], Optional[str]]:
    """A,B,D,F,K,L,O,P,Q,U mövqelərini sil; sonra qalan cədvəldə A sütunu üzrə A→Z sırala."""
    try:
        buf = io.BytesIO(orig_xlsx)
        xlf = pd.ExcelFile(buf, engine="openpyxl")
        sheet = xlf.sheet_names[0]
        df = pd.read_excel(xlf, sheet_name=sheet, engine="openpyxl")
    except Exception as e:
        return None, f"Excel oxunmadı: {e}"

    if df.empty:
        return None, "Cədvəl boşdur."

    df = _drop_columns_by_original_positions(df, _INVENTORY_DROP_COL_INDEXES)

    cols = list(df.columns)
    if len(cols) <= _SORT_COL_INDEX_AFTER_DROPS:
        preview = ", ".join(map(str, cols[:25]))
        return None, f"Sıralama üçün A sütunu yoxdur. Qalan sütunlar: {preview}"

    sort_col = cols[_SORT_COL_INDEX_AFTER_DROPS]

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

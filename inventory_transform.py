"""Clopos inventar export — addım 1: Excel A,D,K + adla silinən sütunlar; A→Z sıra son cədvəldə B sütunu üzrə."""

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

# Sıralama: bütün silinmələrdən sonra Excel B = ikinci sütun (indeks 1)
_SORT_COL_ZERO_BASED = 1

# Excel hərfi A, D, K — standart exportda indekslər 0, 3, 10 (başqa silinmələrdən əvvəl)
_EXCEL_ADK_ZERO_BASED = (0, 3, 10)


def _drop_columns_at_excel_positions(
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
    """Orijinal .xlsx → göstərilən sütunlar silinir, sonra qalan cədvəldə B sütunu üzrə A–Z (mətn sırası).

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

    df = _drop_columns_at_excel_positions(df, _EXCEL_ADK_ZERO_BASED)

    drop_cols = _map_drop_columns(df)
    df = df.drop(columns=drop_cols, errors="ignore")

    cols = list(df.columns)
    if len(cols) <= _SORT_COL_ZERO_BASED:
        preview = ", ".join(map(str, cols[:20]))
        return None, f"Sıralama üçün B sütunu (indeks {_SORT_COL_ZERO_BASED}) yoxdur. Sütunlar: {preview}"
    sort_col = cols[_SORT_COL_ZERO_BASED]

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

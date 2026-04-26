"""Clopos inventar: (1) Kateqoriya addımı — sütun silmə + A sütunu A→Z; (2) Filtr addımı — sətir filtrləri."""

from __future__ import annotations

import io
import math
from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd

# Kateqoriya addımından sonra «Fərqin dəyəri» adətən Excel J = indeks 9 (0-based)
_FERQIN_DEYERI_COL_INDEX_FALLBACK = 9

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


def _find_column(df: pd.DataFrame, *header_variants: str) -> Optional[str]:
    want = {h.strip().casefold() for h in header_variants if h.strip()}
    for c in df.columns:
        if str(c).strip().casefold() in want:
            return str(c).strip()
    return None


def _series_nonempty(s: pd.Series) -> pd.Series:
    t = s.map(lambda x: "" if pd.isna(x) else str(x).strip())
    return (t != "") & (t.str.casefold() != "nan") & (t.str.casefold() != "none")


def _parse_inv_decimal(val) -> float:
    """AZ Excel: vergül onluq; boş/mətn → nan (sıra filtrində nəzərə alınmır)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return math.nan
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return float(val)
    s = str(val).strip().replace("\u00a0", " ")
    if not s or s.lower() in ("nan", "none", "-", "—"):
        return math.nan
    s = s.replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return math.nan


def _resolve_ferqin_deyeri_column(df: pd.DataFrame) -> Optional[str]:
    col = _find_column(df, "Fərqin dəyəri", "Ferqin deyeri")
    if col is not None:
        return col
    if len(df.columns) > _FERQIN_DEYERI_COL_INDEX_FALLBACK:
        return str(df.columns[_FERQIN_DEYERI_COL_INDEX_FALLBACK]).strip()
    return None


@dataclass(frozen=True)
class InventoryFilterOptions:
    """Kateqoriya emalından sonrakı filtr addımı."""

    drop_empty_kateqoriya: bool = True
    drop_empty_mahsul: bool = True
    # «Fərqin dəyəri» (J): sıx (-10, 10) intervalındakı rəqəmlər çıxarılır; -10 və 10 saxlanılır
    exclude_farqin_open_interval_neg10_pos10: bool = True


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


def process_inventory_emal_pipeline(
    orig_xlsx: bytes,
    filter_options: Optional[InventoryFilterOptions] = None,
) -> Tuple[Optional[bytes], Optional[str]]:
    """Kateqoriya (sütun silmə + A sütunu A→Z), sonra filtr — tək çıxış faylı üçün."""
    proc, err = process_inventory_categorization_step(orig_xlsx)
    if err:
        return None, err
    return process_inventory_filter_step(proc, options=filter_options)


def process_inventory_filter_step(
    kateqoriya_xlsx: bytes,
    options: Optional[InventoryFilterOptions] = None,
) -> Tuple[Optional[bytes], Optional[str]]:
    """Kateqoriya emalı çıxışı (.xlsx) üzərində sətir filtrləri tətbiq edir."""
    opts = options or InventoryFilterOptions()
    try:
        buf = io.BytesIO(kateqoriya_xlsx)
        xlf = pd.ExcelFile(buf, engine="openpyxl")
        sheet = xlf.sheet_names[0]
        df = pd.read_excel(xlf, sheet_name=sheet, engine="openpyxl")
    except Exception as e:
        return None, f"Excel oxunmadı: {e}"

    if df.empty:
        return None, "Cədvəl boşdur."

    mask = pd.Series(True, index=df.index)

    if opts.drop_empty_kateqoriya:
        col_k = _find_column(df, "Kateqoriya")
        if col_k is not None:
            mask &= _series_nonempty(df[col_k])

    if opts.drop_empty_mahsul:
        col_m = _find_column(df, "Məhsul", "Mehsul")
        if col_m is not None:
            mask &= _series_nonempty(df[col_m])

    if opts.exclude_farqin_open_interval_neg10_pos10:
        col_fd = _resolve_ferqin_deyeri_column(df)
        if col_fd is not None:
            vals = df[col_fd].map(_parse_inv_decimal)
            # -10 < d < 10 — bu dəyərlər göstərilmir; -10 və 10 və nan saxlanılır
            hide = vals.notna() & (vals > -10.0) & (vals < 10.0)
            mask &= ~hide

    df = df.loc[mask].reset_index(drop=True)

    if df.empty:
        return None, "Filtr sonrası heç bir sətir qalmayıb."

    safe_sheet = str(sheet)[:31] if sheet else "Sheet1"
    out = io.BytesIO()
    try:
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=safe_sheet, index=False)
    except Exception as e:
        return None, f"Excel yazılmadı: {e}"
    return out.getvalue(), None

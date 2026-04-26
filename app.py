import io
import math
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import streamlit as st
from openpyxl.styles import Font
from rapidfuzz import fuzz, process

from rules import merged_special_rules  # ümumi + restoran qaydaları

# --- İnventar emalı (Streamlit Cloud üçün app.py daxilində; ayrıca .py faylı lazım deyil) ---
_FERQIN_DEYERI_COL_INDEX_FALLBACK = 9
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
_SORT_COL_INDEX_AFTER_DROPS = 0


def _inv_drop_columns_by_original_positions(
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


def _inv_find_column(df: pd.DataFrame, *header_variants: str) -> Optional[str]:
    want = {h.strip().casefold() for h in header_variants if h.strip()}
    for c in df.columns:
        if str(c).strip().casefold() in want:
            return str(c).strip()
    return None


def _inv_series_nonempty(s: pd.Series) -> pd.Series:
    t = s.map(lambda x: "" if pd.isna(x) else str(x).strip())
    return (t != "") & (t.str.casefold() != "nan") & (t.str.casefold() != "none")


def _inv_parse_decimal(val) -> float:
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


def _inv_resolve_ferqin_deyeri_column(df: pd.DataFrame) -> Optional[str]:
    col = _inv_find_column(df, "Fərqin dəyəri", "Ferqin deyeri")
    if col is not None:
        return col
    if len(df.columns) > _FERQIN_DEYERI_COL_INDEX_FALLBACK:
        return str(df.columns[_FERQIN_DEYERI_COL_INDEX_FALLBACK]).strip()
    return None


@dataclass(frozen=True)
class InventoryFilterOptions:
    drop_empty_kateqoriya: bool = True
    drop_empty_mahsul: bool = True
    exclude_farqin_open_interval_neg10_pos10: bool = True


def process_inventory_categorization_step(
    orig_xlsx: bytes,
) -> Tuple[Optional[bytes], Optional[str]]:
    try:
        buf = io.BytesIO(orig_xlsx)
        xlf = pd.ExcelFile(buf, engine="openpyxl")
        sheet = xlf.sheet_names[0]
        df = pd.read_excel(xlf, sheet_name=sheet, engine="openpyxl")
    except Exception as e:
        return None, f"Excel oxunmadı: {e}"

    if df.empty:
        return None, "Cədvəl boşdur."

    df = _inv_drop_columns_by_original_positions(df, _INVENTORY_DROP_COL_INDEXES)

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


def process_inventory_filter_step(
    kateqoriya_xlsx: bytes,
    options: Optional[InventoryFilterOptions] = None,
) -> Tuple[Optional[bytes], Optional[str]]:
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
        col_k = _inv_find_column(df, "Kateqoriya")
        if col_k is not None:
            mask &= _inv_series_nonempty(df[col_k])

    if opts.drop_empty_mahsul:
        col_m = _inv_find_column(df, "Məhsul", "Mehsul")
        if col_m is not None:
            mask &= _inv_series_nonempty(df[col_m])

    if opts.exclude_farqin_open_interval_neg10_pos10:
        col_fd = _inv_resolve_ferqin_deyeri_column(df)
        if col_fd is not None:
            vals = df[col_fd].map(_inv_parse_decimal)
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


def process_inventory_emal_pipeline(
    orig_xlsx: bytes,
    filter_options: Optional[InventoryFilterOptions] = None,
) -> Tuple[Optional[bytes], Optional[str]]:
    proc, err = process_inventory_categorization_step(orig_xlsx)
    if err:
        return None, err
    return process_inventory_filter_step(proc, options=filter_options)


st.set_page_config(page_title="ROOM CLOPOS Online", layout="wide")

if "selected_res" not in st.session_state:
    st.session_state.selected_res = "ROOM"
if "last_export" not in st.session_state:
    st.session_state.last_export = None


def _nfc(s: str) -> str:
    s = unicodedata.normalize("NFKC", str(s)).replace("\u00a0", " ")
    s = s.translate(dict.fromkeys(map(ord, "\u200b\u200c\u200d\ufeff"), None))
    return unicodedata.normalize("NFC", s)


def _strip_unicode_marks(s: str) -> str:
    """Rosé/Rose, görünməz fərqlər — Exceldən gələn latın aksentlərini çıxarır."""
    return "".join(
        ch
        for ch in unicodedata.normalize("NFD", s)
        if unicodedata.category(ch) != "Mn"
    )


def _clean_ad_choices(df_base) -> list:
    """Boş / nan sətirlər rapidfuzz-u pozur; bazadan yalnız etibarlı Ad siyahısı."""
    out = []
    for x in df_base["ad"].tolist():
        s = str(x).strip()
        if s and s.lower() not in ("nan", "none"):
            out.append(s)
    return out


def normalize_text(text):
    if not text:
        return ""
    text = _nfc(text).lower().strip()
    text = _strip_unicode_marks(text)
    text = re.sub(r"\(\s*(?:ed|kg|kq|lt|qr|gr|ml|l)\s*\)", "", text)
    text = re.sub(r"\d+\s*%", "", text)
    text = re.sub(r"\d+[\.,]\d+", "", text)
    text = re.sub(r"\b\d+\b", "", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = (
        text.replace("neü", "new")
        .replace("c", "k")
        .replace("w", "v")
        .replace("x", "ks")
    )
    text = (
        text.replace("ç", "c")
        .replace("ə", "e")
        .replace("ğ", "g")
        .replace("\u0131", "i")
        .replace("i\u0307", "i")
        .replace("ö", "o")
        .replace("ş", "s")
        .replace("ü", "u")
    )
    return " ".join(text.split())


def normalize_text_loose(text):
    """Rəqəmləri silmir — SKU/kod tipli adlar üçün; çek ilə baza fərqli olanda əsas xilaskar."""
    if not text:
        return ""
    text = _nfc(text).lower().strip()
    text = _strip_unicode_marks(text)
    text = re.sub(r"\(\s*(?:ed|kg|kq|lt|qr|gr|ml|l)\s*\)", "", text)
    text = re.sub(r"\d+\s*%", "", text)
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = (
        text.replace("ç", "c")
        .replace("ə", "e")
        .replace("ğ", "g")
        .replace("\u0131", "i")
        .replace("ö", "o")
        .replace("ş", "s")
        .replace("ü", "u")
        .replace("i\u0307", "i")
    )
    return " ".join(text.split())


def _all_rule_key_tokens_in_receipt(ks: str, n_strict: str) -> bool:
    """Çoxsözlü qayda açarı üçün: hər bir uzun kəlmə çekdə ayrıca token kimi olmalıdır.
    Əks halda token_set_ratio məs. yalnız «ananas» ilə «sandora ananas»ı səhv birləşdirir."""
    if not ks or not n_strict:
        return False
    receipt_tokens = set(n_strict.split())
    key_words = [w for w in ks.split() if len(w) > 2]
    if not key_words:
        return True
    for w in key_words:
        if w not in receipt_tokens:
            return False
    return True


def apply_special_logic(name, qty, restaurant: str):
    """rules.py açarı çek adının içində (normallaşdırılmış) axtarır.
    1) sıx + loose alt-sətir; 2) token_set yüksək olduqda (fərqli yazılış).
    Qaydalar: merged_special_rules(restaurant) — COMMON + yalnız həmin restoran."""
    if not name or not str(name).strip():
        return name, qty, 1
    raw = str(name).strip()
    n_strict = normalize_text(raw)
    n_loose = normalize_text_loose(raw)
    rules = merged_special_rules(restaurant)
    # Əvvəl uzun açar (məs. «zire zeytun yagi») — qısa alt-sətir təsadüfi tutulmasın
    rule_items = sorted(
        rules.items(),
        key=lambda kv: len(normalize_text(str(kv[0]))),
        reverse=True,
    )

    for key, val in rule_items:
        ks = normalize_text(str(key))
        kl = normalize_text_loose(str(key))
        if ks and (ks in n_strict or ks in n_loose):
            return str(val[0]).strip(), qty * val[1], val[1]
        if kl and (kl in n_loose or kl in n_strict):
            return str(val[0]).strip(), qty * val[1], val[1]

    for key, val in rule_items:
        ks = normalize_text(str(key))
        if len(ks) < 3:
            continue
        if fuzz.token_set_ratio(ks, n_strict) < 88:
            continue
        # 2+ kəlməli açar: bütün kəlmələr çekdə token kimi olmalı (yanlış Juice tutulmasının qarşısı)
        if len(ks.split()) >= 2 and not _all_rule_key_tokens_in_receipt(ks, n_strict):
            continue
        rk_sig = _extract_volume_signatures(str(key))
        rec_sig = _extract_volume_signatures(raw)
        if rk_sig and rk_sig != rec_sig:
            continue
        return str(val[0]).strip(), qty * val[1], val[1]

    return name, qty, 1


def _fuzz_proc(x):
    return normalize_text(str(x))


def _fuzz_loose(x):
    return normalize_text_loose(str(x))


def _extract_volume_signatures(text: str) -> frozenset[str]:
    """Çek/baza xam mətnindən həcm imzaları (l, oz). normalize_text rəqəmləri sildiyi üçün
    uyğunluq qapısı xam sətirdən oxuyur — 0,33l vs 0,75l, 8 oz vs 12 oz, 2l."""
    if not text:
        return frozenset()
    s = _nfc(str(text)).lower().replace(",", ".")
    out: set[str] = set()
    for m in re.finditer(r"(\d+\.\d+)\s*l\b", s):
        out.add(f"{float(m.group(1))}l")
    for m in re.finditer(r"(?<![\d.])(\d+)\s*l\b", s):
        out.add(f"{float(int(m.group(1)))}l")
    for m in re.finditer(r"(\d+)\s*oz\b", s):
        out.add(f"{int(m.group(1))}oz")
    return frozenset(out)


def _volume_pack_signature_gate(q_raw: str, m_raw: str) -> bool:
    """Həcm fərqi olan bazaya səhv birləşmənin qarşısı (Sirab 0,33 vs 0,75, oz)."""
    ql = _nfc(str(q_raw)).lower()
    aq = _extract_volume_signatures(q_raw)
    am = _extract_volume_signatures(m_raw)
    if aq and am:
        return aq == am
    if aq and not am:
        return False
    if not aq and am:
        if "cola" in ql or "sprite" in ql:
            return False
        if "sirab" in ql and "premium" in ql:
            return False
    return True


def _soft_word_gate(q_norm, m_norm, score, strict=False):
    q_words = [w for w in q_norm.split() if len(w) > 2]
    high = 82 if strict else 76
    pr_min = 62 if strict else 52
    if not q_words or score >= high:
        return True
    if any(w in m_norm for w in q_words):
        return True
    return fuzz.partial_ratio(q_norm, m_norm) >= pr_min


def _bar_drink_packaging_gate(q_norm: str, m_norm: str) -> bool:
    """Bar içkiləri: Cola vs Cola 2l, Sirab Premium vs Sirab Qazli token_set qarışıqlığı."""
    if not q_norm or not m_norm:
        return True
    qc = q_norm.replace(" ", "")
    mc = m_norm.replace(" ", "")
    for needle in ("premium", "zero"):
        if needle in q_norm and needle not in m_norm:
            return False
    for needle in ("2l", "19l"):
        if needle in qc and needle not in mc:
            return False
    if "sirab" in q_norm and "premium" not in q_norm and "premium" in m_norm:
        return False
    if ("cola" in q_norm or "sprite" in q_norm) and "2l" not in qc and "2l" in mc:
        return False
    if ("cola" in q_norm or "sprite" in q_norm) and "2l" in qc and "2l" not in mc:
        return False
    if "sirab" in q_norm:
        q_li = "qazli" in q_norm
        q_siz = "qazsiz" in q_norm
        m_li = "qazli" in m_norm
        m_siz = "qazsiz" in m_norm
        if q_li and (not q_siz) and m_siz and (not m_li):
            return False
        if q_siz and m_li and (not m_siz):
            return False
    return True


def _bar_and_volume_gate(q_raw: str, q_norm: str, m_raw: str, m_norm: str) -> bool:
    return _bar_drink_packaging_gate(q_norm, m_norm) and _volume_pack_signature_gate(
        q_raw, m_raw
    )


def _pick_by_volume_signature(q_raw: str, candidate_strings: list) -> str | None:
    """Eyni normalize_text nəticəsi olan adlar arasından çek həcmi ilə tək baza sətri."""
    if not candidate_strings:
        return None
    if len(candidate_strings) == 1:
        return candidate_strings[0]
    qs = _extract_volume_signatures(q_raw)
    if qs:
        ok = [c for c in candidate_strings if _extract_volume_signatures(c) == qs]
        if len(ok) == 1:
            return ok[0]
        return None
    return candidate_strings[0]


def _match_with_processor(
    q_raw,
    choices,
    threshold,
    proc_fn,
    skip_word_gate=False,
    strict_gate=False,
    score_margin=None,
):
    if not choices:
        return None, 0

    q = str(q_raw).strip()
    if not q or q.lower() == "nan":
        return None, 0

    q_norm = proc_fn(q)
    if not q_norm:
        return None, 0

    norm_equal = [ch for ch in choices if proc_fn(ch) == q_norm]
    if norm_equal:
        gated = [
            str(ch)
            for ch in norm_equal
            if _bar_and_volume_gate(q, q_norm, str(ch), proc_fn(ch))
        ]
        if gated:
            picked = _pick_by_volume_signature(q, gated)
            if picked is not None:
                return picked, 100.0

    # Sıx norm yoxdursa, loose (aksent/rəqəm saxlanma) ilə tam uyğun — Rosé vs Rose
    if proc_fn is _fuzz_proc:
        q_lo = normalize_text_loose(q)
        loose_equal = [ch for ch in choices if normalize_text_loose(str(ch)) == q_lo]
        if loose_equal:
            gated = [
                str(ch)
                for ch in loose_equal
                if _bar_and_volume_gate(q, q_norm, str(ch), _fuzz_proc(str(ch)))
            ]
            if gated:
                picked = _pick_by_volume_signature(q, gated)
                if picked is not None:
                    return picked, 100.0

    # Çox yaxın tam uyğunluq (məs. bazada «6 li» / çekdə «6li») — kiçik fərqləri tutur
    if len(choices) <= 4000 and len(q_norm) >= 6:
        ratio_pass: list[tuple[str, float]] = []
        for choice in choices:
            cn = proc_fn(choice)
            r = fuzz.ratio(q_norm, cn)
            if r < 98:
                continue
            if not _bar_and_volume_gate(q, q_norm, str(choice), cn):
                continue
            ratio_pass.append((str(choice), float(r)))
        if ratio_pass:
            max_r = max(x[1] for x in ratio_pass)
            top = [x[0] for x in ratio_pass if x[1] == max_r]
            picked = _pick_by_volume_signature(q, top)
            if picked is not None:
                return picked, float(min(max_r, 99.9))

    # 3+ kəlmə: WRatio tam ifadəni (məs. «sensoy sweet chili») token_set-dən yaxşı tuta bilər
    n_words = len(q_norm.split())
    primary_scorer = fuzz.WRatio if n_words >= 3 else fuzz.token_set_ratio

    best = process.extractOne(
        q,
        choices,
        scorer=primary_scorer,
        processor=proc_fn,
    )
    if not best:
        return None, 0

    best_match = str(best[0])
    score = float(best[1])
    used_alt_scorer = False

    if score < threshold:
        alt_scorer = (
            fuzz.token_set_ratio if primary_scorer == fuzz.WRatio else fuzz.WRatio
        )
        best2 = process.extractOne(q, choices, scorer=alt_scorer, processor=proc_fn)
        if best2 and float(best2[1]) >= threshold:
            best_match = str(best2[0])
            score = float(best2[1])
            used_alt_scorer = True

    m_norm = proc_fn(best_match)
    if not skip_word_gate and not _soft_word_gate(
        q_norm, m_norm, score, strict=strict_gate
    ):
        return None, score

    if score < threshold:
        return None, score

    if not _bar_and_volume_gate(q, q_norm, best_match, m_norm):
        alt_found = None
        alt_score = 0.0
        for cand, sc, _ in process.extract(
            q,
            choices,
            scorer=primary_scorer,
            processor=proc_fn,
            limit=35,
        ):
            scf = float(sc)
            if scf < threshold:
                continue
            cn = proc_fn(cand)
            if not skip_word_gate and not _soft_word_gate(
                q_norm, cn, scf, strict=strict_gate
            ):
                continue
            if not _bar_and_volume_gate(q, q_norm, str(cand), cn):
                continue
            alt_found, alt_score = str(cand), scf
            break
        if alt_found is None:
            return None, score
        best_match, score, m_norm = alt_found, alt_score, proc_fn(alt_found)
        used_alt_scorer = True

    # İki baza sətri eyni xala yaxındırsa — səhv seçim riski; çox yüksək xalda margin tətbiq olunmur
    if (
        score_margin is not None
        and score < 99.9
        and score < 88
        and not used_alt_scorer
        and not skip_word_gate
    ):
        topn = process.extract(
            q, choices, scorer=primary_scorer, processor=proc_fn, limit=2
        )
        if (
            len(topn) >= 2
            and str(topn[0][0]) == best_match
            and (float(topn[0][1]) - float(topn[1][1])) < score_margin
        ):
            return None, float(topn[0][1])

    return best_match, score


def get_best_match(
    query_name, choices, threshold=74, *, safe_mode=True, score_margin=7.0
):
    """Sıx → loose; təhlükəsiz rejimdə son çarə və şübhəli «ikinci yer» yaxınlığı söndürülür."""
    sm = float(score_margin) if (safe_mode and score_margin is not None) else None
    sg = bool(safe_mode)
    r = _match_with_processor(
        query_name, choices, threshold, _fuzz_proc, strict_gate=sg, score_margin=sm
    )
    if r[0]:
        return r
    loose_thr = max(56, int(threshold) - 6)
    r = _match_with_processor(
        query_name,
        choices,
        loose_thr,
        _fuzz_loose,
        strict_gate=False,
        score_margin=sm,
    )
    if r[0]:
        return r
    if safe_mode:
        return None, 0
    qn = _fuzz_proc(query_name)
    if len(qn) < 5 or len(qn.split()) < 2:
        return None, 0
    last_thr = max(40, int(threshold) - 22)
    return _match_with_processor(
        query_name,
        choices,
        last_thr,
        _fuzz_loose,
        skip_word_gate=True,
        strict_gate=False,
        score_margin=None,
    )


def explain_match(query_name, choices, limit=5, processor=None):
    q = str(query_name).strip()
    if not choices or not q:
        return []
    proc = processor if processor is not None else _fuzz_proc
    return process.extract(
        q,
        choices,
        scorer=fuzz.token_set_ratio,
        processor=proc,
        limit=limit,
    )


def parse_az_number(val):
    """Excel AZ formatı: vergül onluq (1,135), boşluqlu minlik nadir."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0.0
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return float(val)
    s = str(val).strip().replace("\u00a0", " ")
    if not s or s.lower() in ("nan", "none", "-", "—"):
        return 0.0
    s = s.replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def standardize_columns(df, chek_fayli=False):
    """chek_fayli=True: sklad çekindəki sətir «ID» sütunu bazanın Clopos id-si ilə qarışmasın."""
    df = df.copy()
    df.columns = [str(c).strip().lstrip("\ufeff") for c in df.columns]
    renamed = {}
    for col in df.columns:
        key = normalize_text(col)
        col_l = str(col).lower()
        if key == "ad" or "mehsul" in key or "nomenkl" in key or key in ("mal", "title", "name"):
            renamed[col] = "ad"
        elif (
            "miqdar" in key
            or "kemiyyat" in key
            or key in ("say", "qty", "qty.")
            or "eded" in key
        ):
            renamed[col] = "miqdar"
        elif "umumi" in key or ("maya" in key and "dey" in key):
            renamed[col] = "line_total_src"
        elif key == "vahid" and "₼" not in col_l and "azn" not in col_l:
            # Yalnız ölçü vahidi (kg/pcs) — qiymət deyil
            renamed[col] = "unit_kind"
        elif "vahid" in key and ("₼" in col_l or "azn" in col_l or "qiym" in key):
            renamed[col] = "price"
        elif any(k in key for k in ["qiym", "azn"]) or "₼" in col_l:
            renamed[col] = "price"
        elif key == "id":
            renamed[col] = "cek_line_id" if chek_fayli else "id"
    return df.rename(columns=renamed)


def normalize_restaurant_name(name):
    return str(name).lower().replace("\u0131", "i").replace("i\u0307", "i").strip()


def discover_restaurants():
    restaurants = set()
    for file_name in os.listdir("."):
        lower_name = file_name.lower()
        if not lower_name.startswith("ana_"):
            continue
        if not (lower_name.endswith(".xlsx") or lower_name.endswith(".csv")):
            continue
        if "_horeca" in lower_name:
            restaurants.add(file_name[4:].rsplit("_horeca", 1)[0].upper())
        elif "_dk" in lower_name:
            restaurants.add(file_name[4:].rsplit("_dk", 1)[0].upper())
    return sorted(restaurants) if restaurants else ["ROOM", "BIBLIOTEKA", "FINESTRA"]


def _resolve_db_path_for_suffix(res_name, suffix: str):
    """suffix: 'horeca' və ya 'dk' — fayl adı ana_<restoran>_<suffix>."""
    target_prefix = f"ana_{normalize_restaurant_name(res_name)}_{suffix}"
    for file_name in os.listdir("."):
        normalized_file = normalize_restaurant_name(file_name)
        if normalized_file.startswith(target_prefix):
            if file_name.lower().endswith((".xlsx", ".csv")):
                return file_name
    return None


def _read_single_db_path(path):
    if not path:
        return None
    try:
        if path.lower().endswith(".xlsx"):
            return pd.read_excel(path)
        return pd.read_csv(path)
    except Exception:
        return None


@st.cache_data(ttl=30, show_spinner=False)
def get_db(res_name, category):
    """Horeca: yalnız *_horeca. Dark Kitchen: *_dk + *_horeca birləşik (eyni Ad təkrarlanmasa, dk üstün)."""
    if category == "Horeca":
        raw = _read_single_db_path(_resolve_db_path_for_suffix(res_name, "horeca"))
        if raw is None or raw.empty:
            return None
        return standardize_columns(raw, chek_fayli=False)
    parts = []
    for suffix in ("dk", "horeca"):
        raw = _read_single_db_path(_resolve_db_path_for_suffix(res_name, suffix))
        if raw is None or raw.empty:
            continue
        parts.append(standardize_columns(raw, chek_fayli=False))
    if not parts:
        return None
    out = pd.concat(parts, ignore_index=True)
    # Eyni başlıqla iki sütun (məs. biri «Ad», biri «ad») olanda df["ad"] DataFrame olur → .str xətası
    out = out.loc[:, ~out.columns.duplicated(keep="first")]
    if "ad" in out.columns:
        out = out.drop_duplicates(subset=["ad"], keep="first")
    return out


def _clean_receipt_no(v: str) -> str:
    s = _nfc(str(v)).strip()
    s = re.sub(r"[^\w\-]+", " ", s, flags=re.UNICODE)
    s = " ".join(s.split())
    return s[:40]


def _extract_export_receipt_no(df_c, uploaded_name: str = "") -> str:
    if isinstance(df_c, pd.DataFrame) and not df_c.empty:
        cols = [str(c) for c in df_c.columns]
        priority = [
            "cek_no",
            "check_no",
            "chek_no",
            "sened_no",
            "nomre",
            "id",
            "cek_line_id",
        ]
        ordered = [c for c in priority if c in cols]
        ordered += [
            c
            for c in cols
            if c not in ordered
            and (
                re.search(r"(cek|check|chek).*(no|nom|nmr)", c, flags=re.IGNORECASE)
                or re.search(r"(no|nom|nmr).*(cek|check|chek)", c, flags=re.IGNORECASE)
                or re.search(r"nomre|sened", c, flags=re.IGNORECASE)
            )
        ]
        for c in ordered:
            s = (
                df_c[c]
                .dropna()
                .astype(str)
                .map(lambda x: _nfc(x).strip())
            )
            s = s[~s.str.lower().isin(["", "nan", "none"])]
            if s.empty:
                continue
            counts = s.value_counts(dropna=True)
            top = str(counts.index[0]).strip()
            if len(counts) == 1 or int(counts.iloc[0]) >= max(2, int(len(s) * 0.6)):
                cleaned = _clean_receipt_no(top)
                if cleaned:
                    return cleaned
    if uploaded_name:
        m = re.search(r"(\d{4,})", _nfc(uploaded_name))
        if m:
            return m.group(1)
    return datetime.now().strftime("%Y%m%d_%H%M")


def build_export_file_name(restaurant, category, receipt_no=None):
    rec = _clean_receipt_no(receipt_no or "")
    if rec:
        return f"clopos import {rec}.xlsx"
    category_tag = "horeca" if category == "Horeca" else "dk"
    restaurant_tag = normalize_restaurant_name(restaurant).replace(" ", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    return f"clopos_{restaurant_tag}_{category_tag}_{timestamp}.xlsx"


def _excel_sheet_no_bold(writer, sheet_name):
    """Bütün xanalar normal şrift — ID/QUANTITY/COST başlığı da qalın olmasın."""
    ws = writer.sheets[sheet_name]
    plain = Font(bold=False)
    for row in ws.iter_rows(
        min_row=1,
        max_row=ws.max_row,
        min_col=1,
        max_col=ws.max_column,
    ):
        for cell in row:
            cell.font = plain


def to_bold_excel_bytes(dataframe):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        dataframe.to_excel(writer, index=False, sheet_name="CLOPOS")
        _excel_sheet_no_bold(writer, "CLOPOS")
    output.seek(0)
    return output.getvalue()


def to_tapilmayan_only_bytes(unmatched_df):
    """Yalnız əl ilə iş üçün Tapılmayanlar vərəqi."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        unmatched_df.to_excel(writer, index=False, sheet_name="Tapilmayanlar")
        _excel_sheet_no_bold(writer, "Tapilmayanlar")
    output.seek(0)
    return output.getvalue()


def _resolve_id_for_product(df_base, name_query):
    """Bazada Ad ilə ID tapır: dəqiq, böyük/kiçik, NFC, normallaşdırılmış, son çarə ratio.
    Qaydadan gələn ad Exceldə «eyni görünən» amma Unicode fərqli olanda kömək edir."""
    if name_query is None:
        return None, None
    raw = str(name_query).strip()
    if not raw or raw.lower() in ("nan", "none"):
        return None, None

    ads = df_base["ad"].astype(str).str.strip()

    def _pick(mask):
        if not mask.any():
            return None, None
        row0 = df_base.loc[mask].iloc[0]
        return int(row0["id"]), str(row0["ad"]).strip()

    m, ad = _pick(ads == raw)
    if m is not None:
        return m, ad

    m, ad = _pick(ads.str.lower() == raw.lower())
    if m is not None:
        return m, ad

    raw_nfc = _strip_unicode_marks(_nfc(raw).strip().lower())
    nfc_match = ads.map(
        lambda x: _strip_unicode_marks(_nfc(str(x)).strip().lower())
    ) == raw_nfc
    m, ad = _pick(nfc_match)
    if m is not None:
        return m, ad

    q_nl = normalize_text_loose(raw)
    loose_match = ads.map(lambda x: normalize_text_loose(str(x))) == q_nl
    m, ad = _pick(loose_match)
    if m is not None:
        return m, ad

    qn = normalize_text(raw)
    norm_match = ads.map(lambda x: normalize_text(str(x))) == qn
    m, ad = _pick(norm_match)
    if m is not None:
        return m, ad

    raw_cmp = _strip_unicode_marks(_nfc(raw).strip().lower())
    best_r, best_id, best_ad = 0, None, None
    for _, row in df_base.iterrows():
        ad = str(row["ad"]).strip()
        r = fuzz.ratio(raw_cmp, _strip_unicode_marks(_nfc(ad).strip().lower()))
        if r > best_r:
            best_r = r
            best_id = int(row["id"])
            best_ad = ad
    if best_id is not None and best_r >= 96:
        return best_id, best_ad
    if len(raw_cmp) >= 10:
        best_ts, best_id2, best_ad2 = 0, None, None
        for _, row in df_base.iterrows():
            ad = str(row["ad"]).strip()
            ts = float(
                fuzz.token_set_ratio(
                    raw_cmp, _strip_unicode_marks(_nfc(ad).strip().lower())
                )
            )
            if ts > best_ts:
                best_ts, best_id2, best_ad2 = ts, int(row["id"]), ad
        if best_id2 is not None and best_ts >= 93.0:
            return best_id2, best_ad2
    return None, None


def _first_id_for_name(df_base, m_name):
    """Köhnə çağırışlar üçün; uğursuzdursa KeyError."""
    mid, _ = _resolve_id_for_product(df_base, m_name)
    if mid is None:
        raise KeyError(f"id tapılmadı: {m_name!r}")
    return mid


def _render_restoran_online_panel() -> None:
    """Analiz və Kontrol — yalnız Restoran şöbəsi aktiv olanda göstərilir."""
    curr = st.session_state.selected_res
    st.markdown(
        f"<h3 style='text-align: center;'>{curr} | Online Panel</h3>",
        unsafe_allow_html=True,
    )
    tab1, tab2 = st.tabs(["🚀 ANALİZ", "🔍 KONTROL"])
    
    with tab1:
        st.caption(
            "Mexanizm: çekdəki **Ad** ilə ana bazada **eyni məhsul adı** tapılır → export **ID** "
            "yalnız bazadandır. **QUANTITY** = çek miqdarı (xüsusi qayda varsa çevrilmiş miqdar). "
            "**COST** = (çekdəki vahid qiymət ÷ Miqdar) ÷ **qayda faktoru** (əgər 1-dirsə yalnız birinci bölmə): "
            "məs. 1 paket 5 kq = 7 ₼ → çek vahidi 7 ₼, faktor 5 → **1 kq üçün 1,4 ₼**; QUANTITY 5 kq olunca sətir cəmi 7 ₼. "
            "**Təhlükəsiz rejim**: yalnız aydın uyğunluq qəbul edilir; qalanlar **Tapılmayanlar** "
            "vərəqində əl ilə doldurmaq üçündür. **Dark Kitchen** üçün ana baza həm `ana_<rest>_dk`, "
            "həm `ana_<rest>_horeca` faylından oxunur və birləşdirilir; **Horeca** üçün yalnız `_horeca`."
        )
        col_a, col_b, col_c = st.columns([1, 1, 1])
        cat = col_a.selectbox("Sahə:", ["Horeca", "Dark Kitchen"])
        aggressive_match = col_a.checkbox(
            "Agressiv uyğunluq (daha çox avtomatik sətir, daha çox səhv riski)",
            value=False,
            help="Söndürülmüşdə: yüksək hədd, ikinci yerə yaxın nəticələr rədd, son çarə yoxdur.",
        )
        match_thr = col_c.slider(
            "Uyğunluq həddi (%) — təhlükəsiz rejimdə təklif: 72–80",
            min_value=60,
            max_value=95,
            value=74,
            help="Aşağı = daha çox sətir; təhlükəsiz rejimdə şübhəli yaxınlıqlar yenə rədd edilə bilər.",
        )
        cek = col_b.file_uploader("📄 Sklad Çekini Yüklə", type=["xlsx"])
    
        if cek and st.button("⚡ Başlat"):
            df_base = get_db(curr, cat)
            if df_base is not None:
                df_c = pd.read_excel(cek)
                df_c = standardize_columns(df_c, chek_fayli=True)
                df_base = standardize_columns(df_base, chek_fayli=False)
    
                required_cek = {"ad", "miqdar"}
                required_base = {"ad", "id"}
                if not required_cek.issubset(set(df_c.columns)):
                    st.markdown("**Problem:** Çek faylında `Ad` və `Miqdar` sütunları tapılmadı.")
                    st.stop()
                if not required_base.issubset(set(df_base.columns)):
                    st.markdown("**Problem:** Baza faylında `Ad` və `id` sütunları tapılmadı.")
                    st.stop()
    
                # choices strip olunur; df_base["ad"] də eyni olmalıdır — əks halda id tapılmır
                df_c["ad"] = df_c["ad"].astype(str).str.strip()
                for _col in ("miqdar", "price"):
                    if _col in df_c.columns:
                        df_c[_col] = df_c[_col].map(parse_az_number)
                df_base["ad"] = df_base["ad"].astype(str).str.strip()
                df_base["id"] = pd.to_numeric(df_base["id"], errors="coerce")
                df_base = df_base.dropna(subset=["id", "ad"])
                df_base["id"] = df_base["id"].astype(int)
                df_base = df_base.drop_duplicates(subset=["ad"], keep="first")
    
                final_list = []
                errors = 0
                choices = _clean_ad_choices(df_base)
                fail_debug = []
                tapilmayan_rows = []
                skipped_rows = []
                safe_mode = not aggressive_match
                for row_idx, (_, row) in enumerate(df_c.iterrows(), start=1):
                    o_name = ""
                    p_name = ""
                    try:
                        o_name = str(row.get("ad", "")).strip()
                        if not o_name or o_name.lower() in ("nan", "none"):
                            skipped_rows.append(
                                {
                                    "Sətir": row_idx,
                                    "Çekdəki_ad": str(row.get("ad", ""))[:240],
                                    "Səbəb": "Boş / etibarsız ad",
                                }
                            )
                            continue
                        o_qty = parse_az_number(row.get("miqdar", 0))
                        unit_price = parse_az_number(row.get("price", 0))
                        if o_qty == 0:
                            skipped_rows.append(
                                {
                                    "Sətir": row_idx,
                                    "Çekdəki_ad": o_name,
                                    "Səbəb": "Miqdar 0",
                                }
                            )
                            continue
    
                        p_name, p_qty, fct = apply_special_logic(o_name, o_qty, curr)
                        # Çek miqdarına görə bir fiziki vahidin (paket, ed) qiyməti; qayda ilə miqdar fct vurulanda
                        # Clopos COST = baza vahidinin (məs. 1 kq) qiyməti → paket qiyməti / fct (7/5=1.4).
                        price_per_cheque_unit = (unit_price / o_qty) if o_qty != 0 else 0.0
                        cost = (
                            (price_per_cheque_unit / fct)
                            if fct not in (None, 0)
                            else price_per_cheque_unit
                        )
                        m_name, _score = get_best_match(
                            p_name,
                            choices,
                            threshold=match_thr,
                            safe_mode=safe_mode,
                        )
                        mid, _canon = (None, None)
                        if m_name:
                            mid, _canon = _resolve_id_for_product(df_base, m_name)
                        if mid is None:
                            mid, _canon = _resolve_id_for_product(df_base, p_name)
                        if mid is not None:
                            final_list.append(
                                {
                                    "ID": mid,
                                    "QUANTITY": p_qty,
                                    "COST": round(cost, 4),
                                    "LINE_TOTAL": round(p_qty * cost, 4),
                                }
                            )
                        else:
                            errors += 1
                            hits = explain_match(p_name, choices, limit=5)
                            hits_l = explain_match(
                                p_name, choices, limit=3, processor=_fuzz_loose
                            )
                            row_dbg = {
                                "Çekdə ad": o_name,
                                "Miqdar": o_qty,
                                "Bir_vahid_COST": round(cost, 4),
                                "Qaydadan sonra": p_name,
                                "Ən yaxın (token_set)": hits[0][0] if hits else "",
                                "Xal": round(float(hits[0][1]), 1) if hits else "",
                                "2-ci": hits[1][0] if len(hits) > 1 else "",
                                "2 xal": round(float(hits[1][1]), 1) if len(hits) > 1 else "",
                                "Loose 1": hits_l[0][0] if hits_l else "",
                                "Loose xal": round(float(hits_l[0][1]), 1) if hits_l else "",
                            }
                            fail_debug.append(row_dbg)
                            tapilmayan_rows.append(
                                {
                                    "Sətir": row_idx,
                                    "Çekdəki_ad": o_name,
                                    "Qaydadan_sonra": p_name,
                                    "Miqdar": o_qty,
                                    "Bir_vahid_COST": round(cost, 4),
                                    "ID_əl_ile": "",
                                    "Baza_Adı_əl_ile": "",
                                }
                            )
                    except (ValueError, TypeError, KeyError) as ex:
                        errors += 1
                        eq_x = parse_az_number(row.get("miqdar", 0))
                        up_x = parse_az_number(row.get("price", 0))
                        cst_x = (up_x / eq_x) if eq_x else 0.0
                        fail_debug.append(
                            {
                                "Çekdə ad": o_name,
                                "Miqdar": eq_x,
                                "Bir_vahid_COST": round(cst_x, 4) if eq_x else "",
                                "Qaydadan sonra": p_name,
                                "Ən yaxın (token_set)": f"(xəta) {type(ex).__name__}",
                                "Xal": "",
                                "2-ci": str(ex)[:120],
                                "2 xal": "",
                            }
                        )
                        tapilmayan_rows.append(
                            {
                                "Sətir": row_idx,
                                "Çekdəki_ad": o_name
                                or str(row.get("ad", "")).strip()
                                or "(xəta)",
                                "Qaydadan_sonra": p_name,
                                "Miqdar": eq_x if eq_x else "",
                                "Bir_vahid_COST": round(cst_x, 4) if eq_x else "",
                                "ID_əl_ile": "",
                                "Baza_Adı_əl_ile": "",
                            }
                        )
                        continue
    
                n_cek = len(df_c)
                n_tutulan = len(final_list)
                n_tapilmayan = len(tapilmayan_rows)
                n_kecilen = len(skipped_rows)
                st.info(
                    f"**Çek sətri (cəmi):** {n_cek} | **Avtomatik tutulan:** {n_tutulan} | "
                    f"**Tapılmayan:** {n_tapilmayan} | **Keçilən (boş ad / miqdar 0):** {n_kecilen}  \n"
                    f"*(Yoxlama: {n_tutulan} + {n_tapilmayan} + {n_kecilen} = {n_tutulan + n_tapilmayan + n_kecilen} — çeklə uyğun gəlməlidir.)*"
                )
                if skipped_rows:
                    with st.expander("Keçilən sətirlər (boş ad və ya miqdar 0)", expanded=False):
                        st.dataframe(pd.DataFrame(skipped_rows), use_container_width=True)
    
                if not final_list:
                    st.markdown(
                        "**Diqqət:** Uyğun məhsul tapılmadı. Ad yazılışları fərqli ola bilər və ya baza faylı uyğun deyil."
                    )
                    st.info(
                        f"Baza məhsulu: {len(df_base)} | Uğursuz match cəhdi: {errors}"
                    )
                    if tapilmayan_rows:
                        st.markdown("### Tapılmayanlar")
                        st.caption(
                            "Bu sətirləri əl ilə bazada tapıb `rules.py` və ya ana bazaya əlavə edin; "
                            "boş sütunları Exceldə doldura bilərsiniz."
                        )
                        um_only = pd.DataFrame(tapilmayan_rows)
                        st.dataframe(um_only, use_container_width=True)
                        st.download_button(
                            "📥 Tapılmayanlar (Excel)",
                            to_tapilmayan_only_bytes(um_only),
                            f"tapilmayanlar_{curr}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                            key="download_um_only_fail",
                        )
                    sample = df_c[["ad", "miqdar"]].dropna(subset=["ad"]).head(10).rename(
                        columns={"ad": "Çekdə ad", "miqdar": "Miqdar"}
                    )
                    if not sample.empty:
                        st.markdown("İlk 10 çek adı (baza ilə vizual müqayisə üçün):")
                        st.dataframe(sample, use_container_width=True)
                    if fail_debug:
                        dbg_df = pd.DataFrame(fail_debug)
                        with st.expander(
                            "Diaqnostika: hər sətir üçün bazadan ən yaxın 2 variant (xal aşağıdırsa həddi sal)",
                            expanded=True,
                        ):
                            st.dataframe(dbg_df, use_container_width=True)
                        dbg_bytes = to_bold_excel_bytes(
                            dbg_df.rename(
                                columns={
                                    "Çekdə ad": "cek_ad",
                                    "Qaydadan sonra": "qayda_sonra",
                                    "Ən yaxın (token_set)": "en_yaxin_1",
                                    "Xal": "xal_1",
                                    "2-ci": "en_yaxin_2",
                                    "2 xal": "xal_2",
                                    "Loose 1": "loose_1",
                                    "Loose xal": "loose_xal",
                                }
                            )
                        )
                        st.download_button(
                            "📥 Diaqnostika cədvəlini Excel kimi endir",
                            dbg_bytes,
                            f"clopos_diag_{curr}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                            key="download_diag",
                        )
                    st.caption(
                        "Əsas export yalnız ən azı bir sətir uğurla uyğunlaşanda çıxır. "
                        "Yuxarıdakı sürgü ilə həddi azaldıb ⚡ Başlat-a yenidən bas."
                    )
                    st.stop()
    
                res_df = (
                    pd.DataFrame(final_list)
                    .groupby("ID", as_index=False)
                    .agg({"QUANTITY": "sum", "LINE_TOTAL": "sum"})
                )
                res_df["COST"] = (res_df["LINE_TOTAL"] / res_df["QUANTITY"]).round(4)
                res_df = res_df[["ID", "QUANTITY", "COST"]]
    
                st.markdown(f"**Hazırdır:** {len(res_df)} məhsul hazırlandı.")
                if errors:
                    st.info(
                        f"{errors} sətir avtomatik tutulmadı — **Tapılmayanlar** bölməsində əl ilə işləyin."
                    )
    
                st.dataframe(res_df, use_container_width=True)
    
                um_df = pd.DataFrame(tapilmayan_rows) if tapilmayan_rows else pd.DataFrame()
                if not um_df.empty:
                    st.markdown("### Tapılmayanlar")
                    st.caption(
                        "Bu sətirlər təhlükəsiz rejimdə baza ilə avtomatik birləşdirilmədi (və ya uyğunluq "
                        "şübhəli sayıldı). **ID_əl_ile** / **Baza_Adı_əl_ile** sütunlarını Exceldə doldurub "
                        "sonra `rules.py`-ə qayda əlavə edin və ya bazanı yeniləyin."
                    )
                    st.dataframe(um_df, use_container_width=True)
                    st.download_button(
                        "📥 Yalnız Tapılmayanlar (Excel)",
                        to_tapilmayan_only_bytes(um_df),
                        f"tapilmayanlar_{curr}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                        key="download_um_only_ok",
                    )
    
                receipt_no = _extract_export_receipt_no(df_c, getattr(cek, "name", ""))
                export_name = build_export_file_name(curr, cat, receipt_no=receipt_no)
                export_bytes = to_bold_excel_bytes(res_df)
                um_bytes = (
                    to_tapilmayan_only_bytes(um_df) if not um_df.empty else None
                )
                um_name = (
                    f"tapilmayanlar_{curr}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
                    if um_bytes
                    else None
                )
                st.session_state.last_export = {
                    "restaurant": curr,
                    "category": cat,
                    "rows": len(res_df),
                    "unmatched": len(um_df),
                    "file_name": export_name,
                    "file_bytes": export_bytes,
                    "unmatched_bytes": um_bytes,
                    "unmatched_file_name": um_name,
                    "preview_df": res_df,
                }
                st.markdown(
                    "**Clopos import:** `"
                    + export_name
                    + "`"
                    + (
                        f" — əlavə olaraq **Tapılmayanlar** üçün ayrı Excel ({len(um_df)} sətir) endir."
                        if not um_df.empty
                        else "."
                    )
                )
                st.download_button(
                    "📥 Clopos import (yalnız ID, QUANTITY, COST)",
                    export_bytes,
                    export_name,
                    key="download_current",
                )
            else:
                dk_hint = (
                    " **Dark Kitchen** üçün ən azı biri olmalıdır: `ana_<restoran>_dk` və/və ya "
                    "`ana_<restoran>_horeca`."
                    if cat == "Dark Kitchen"
                    else " **Horeca** üçün `ana_<restoran>_horeca` faylı lazımdır."
                )
                st.markdown(
                    "**Problem:** Uyğun ana baza tapılmadı. Repo kökündə fayl adı `ana_<restoran>_horeca` "
                    "və ya `ana_<restoran>_dk` formatında olmalıdır."
                    + dk_hint
                )
    
        saved_export = st.session_state.get("last_export")
        if saved_export:
            st.markdown("---")
            st.markdown("### Son hazırlanmış fayl")
            st.write(
                f"Restoran: **{saved_export['restaurant']}** | "
                f"Sahə: **{saved_export['category']}** | "
                f"Sətir sayı: **{saved_export['rows']}**"
            )
            if saved_export.get("unmatched"):
                st.caption(
                    f"Son analizdə **tapılmayan** sətir: **{saved_export['unmatched']}** — Clopos faylına "
                    "daxil edilmir; ayrıca Excel aşağıdan endir."
                )
            st.write(f"Clopos faylı: `{saved_export['file_name']}`")
            st.dataframe(saved_export["preview_df"], use_container_width=True)
            st.download_button(
                "📥 Clopos faylını yenidən endir",
                saved_export["file_bytes"],
                saved_export["file_name"],
                key="download_saved",
            )
            ub = saved_export.get("unmatched_bytes")
            ufn = saved_export.get("unmatched_file_name")
            if ub and ufn:
                st.download_button(
                    "📥 Tapılmayanlar faylını yenidən endir",
                    ub,
                    ufn,
                    key="download_saved_um",
                )
    
    with tab2:
        ctrl_cat = st.selectbox(
            "Kontrol üçün baza sahəsi:",
            ["Horeca", "Dark Kitchen"],
            key="tab2_cat",
        )
        f_orig = st.file_uploader("1. Orijinal Çek", type=["xlsx"], key="ko")
        f_bot = st.file_uploader("2. Analiz Faylı", type=["xlsx"], key="kb")
        if f_orig and f_bot and st.button("🔍 Yoxla"):
            df_o, df_b = pd.read_excel(f_orig), pd.read_excel(f_bot)
            df_o = standardize_columns(df_o, chek_fayli=True)
            df_b = standardize_columns(df_b, chek_fayli=False)
            db = get_db(curr, ctrl_cat)
            if db is not None:
                db = standardize_columns(db, chek_fayli=False)
                db["ad"] = db["ad"].astype(str).str.strip()
                db["id"] = pd.to_numeric(db["id"], errors="coerce")
                db = db.dropna(subset=["id", "ad"])
                db["id"] = db["id"].astype(int)
                db = db.drop_duplicates(subset=["ad"], keep="first")
                if "id" not in df_b.columns:
                    st.markdown("**Problem:** Analiz faylında `ID` / `id` sütunu tapılmadı.")
                    st.stop()
                bot_ids = set(df_b["id"].astype(int).tolist())
                missing = []
                for _, row in df_o.iterrows():
                    name = str(row.get("ad", ""))
                    p_name, _, _ = apply_special_logic(name, 1, curr)
                    m_name, _ = get_best_match(
                        p_name,
                        _clean_ad_choices(db),
                        threshold=74,
                        safe_mode=True,
                    )
                    tid, _ = (None, None)
                    if m_name:
                        tid, _ = _resolve_id_for_product(db, m_name)
                    if tid is None:
                        tid, _ = _resolve_id_for_product(db, p_name)
                    if tid is not None:
                        if tid not in bot_ids:
                            missing.append(name)
                    else:
                        missing.append(f"{name} (Bazada yoxdur)")
                st.table(pd.DataFrame(missing, columns=["Tapılmayanlar"]))
            else:
                st.markdown("**Problem:** Uyğun ana baza tapılmadı.")

# --- SİDEBAR ---
if "panel_branch" not in st.session_state:
    st.session_state.panel_branch = "restoran"

res_options = discover_restaurants()
if st.session_state.selected_res not in res_options:
    st.session_state.selected_res = res_options[0]

st.sidebar.markdown("#### Şöbə")
st.sidebar.radio(
    "Şöbə",
    ["restoran", "inventar"],
    horizontal=True,
    key="panel_branch",
    label_visibility="collapsed",
    format_func=lambda x: "🏢 Restoran" if x == "restoran" else "📦 İnventarizasiya",
)

if st.session_state.panel_branch == "restoran":
    st.sidebar.markdown("##### Restoran seçimi")
    for res_opt in res_options:
        label = f"{res_opt} ✅" if st.session_state.selected_res == res_opt else res_opt
        if st.sidebar.button(label, key=f"btn_{res_opt}", use_container_width=True):
            st.session_state.selected_res = res_opt
            st.rerun()
    st.sidebar.info("Ana baza faylları GitHub mənbəsindən avtomatik oxunur.")
else:
    st.sidebar.info(
        "İnventarizasiya üçün faylları əsas iş sahəsindəki pəncərələrə yükləyin."
    )

# --- PANELLƏR ---
if st.session_state.panel_branch == "inventar":
    st.markdown(
        "<h3 style='text-align: center;'>📦 İnventarizasiya</h3>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "**1–4 həftə:** yüklənən faylın **orijinalı** yadda saxlanılır; **ikinci endirmə** — əvvəl "
        "Excel **A,B,D,F,K,L,O,P,Q,U** silinir, **A sütunu** (Kateqoriya) üzrə A→Z sıra, sonra **filtr**: boş sətirlər "
        "və **Fərqin dəyəri** (J): **-10-dan böyük və 10-dan kiçik** sıx intervaldakı rəqəmlər silinir (**-10** və **10** saxlanır) (tək `.xlsx`). "
        "**MONTH** müvəqqəti eyni zəncirdir."
    )
    with st.expander("🔎 Filtr parametrləri (ikinci endirmədə)", expanded=False):
        st.caption(
            "Sıra (A→Z) tətbiq olunduqdan sonra: boş sətirlər və **Fərqin dəyəri** üzrə sıra filtri."
        )
        st.checkbox(
            "Boş **Kateqoriya** sətirlərini sil",
            value=True,
            key="inv_filter_drop_empty_kat",
        )
        st.checkbox(
            "Boş **Məhsul** sətirlərini sil",
            value=True,
            key="inv_filter_drop_empty_mah",
        )
        st.checkbox(
            "**Fərqin dəyəri** (J): -10 ilə 10 arası **sıx** intervaldakı sətirləri sil (-10 və 10 saxla)",
            value=True,
            key="inv_filter_exclude_farqin_mid",
        )

    def _inv_fingerprint(up):
        return f"{up.name}:{up.size}" if up is not None else None

    def _sync_inv_original(uploaded, uploader_key: str) -> None:
        fp = _inv_fingerprint(uploaded)
        prev = st.session_state.get(f"_inv_fp_{uploader_key}")
        if fp != prev:
            st.session_state[f"_inv_fp_{uploader_key}"] = fp
            if uploaded is not None:
                st.session_state[f"_inv_orig_{uploader_key}"] = uploaded.getvalue()
            else:
                st.session_state.pop(f"_inv_orig_{uploader_key}", None)

    inv_slots = [
        ("1Week", "inv_week1"),
        ("2Week", "inv_week2"),
        ("3Week", "inv_week3"),
        ("4Week", "inv_week4"),
        ("MONTH", "inv_month"),
    ]
    inv_cols = st.columns(5)
    for inv_col, (inv_label, inv_key) in zip(inv_cols, inv_slots):
        with inv_col:
            with st.container(border=True):
                up = st.file_uploader(inv_label, type=["xlsx"], key=inv_key)
                _sync_inv_original(up, inv_key)
                orig = st.session_state.get(f"_inv_orig_{inv_key}")
                if orig is None:
                    continue
                stem = (
                    os.path.splitext(up.name)[0][:80]
                    if up is not None
                    else inv_label
                )
                st.download_button(
                    "📥 Orijinal",
                    data=orig,
                    file_name=f"{inv_label}_{stem}_orijinal.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"inv_dl_orig_{inv_key}",
                    use_container_width=True,
                )
                _inv_opts = InventoryFilterOptions(
                    drop_empty_kateqoriya=st.session_state.get(
                        "inv_filter_drop_empty_kat", True
                    ),
                    drop_empty_mahsul=st.session_state.get(
                        "inv_filter_drop_empty_mah", True
                    ),
                    exclude_farqin_open_interval_neg10_pos10=st.session_state.get(
                        "inv_filter_exclude_farqin_mid", True
                    ),
                )
                proc_emal, err_emal = process_inventory_emal_pipeline(
                    orig, filter_options=_inv_opts
                )
                if err_emal:
                    st.caption(f"⚠ {err_emal}")
                else:
                    st.download_button(
                        "📥 Kateqoriya + sıra + filtr",
                        data=proc_emal,
                        file_name=f"{inv_label}_{stem}_kateqoriya_emal.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"inv_dl_proc_{inv_key}",
                        use_container_width=True,
                    )
elif st.session_state.panel_branch == "restoran":
    _render_restoran_online_panel()

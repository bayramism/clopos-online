import io
import os
import re
import unicodedata
from datetime import datetime

import pandas as pd
import streamlit as st
from openpyxl.styles import Font
from rapidfuzz import fuzz, process

from rules import merged_special_rules  # ümumi + restoran qaydaları

st.set_page_config(page_title="ROOM CLOPOS Online", layout="wide")

if "selected_res" not in st.session_state:
    st.session_state.selected_res = "ROOM"
if "last_export" not in st.session_state:
    st.session_state.last_export = None


def _nfc(s: str) -> str:
    return unicodedata.normalize("NFC", str(s)).replace("\u00a0", " ")


def normalize_text(text):
    if not text:
        return ""
    text = _nfc(text).lower().strip()
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
        .replace("ı", "i")
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
    text = re.sub(r"\(\s*(?:ed|kg|kq|lt|qr|gr|ml|l)\s*\)", "", text)
    text = re.sub(r"\d+\s*%", "", text)
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    text = (
        text.replace("ç", "c")
        .replace("ə", "e")
        .replace("ğ", "g")
        .replace("ı", "i")
        .replace("ö", "o")
        .replace("ş", "s")
        .replace("ü", "u")
        .replace("i̇", "i")
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
    Qaydalar: merged_special_rules(restaurant) — ümumi + həmin restoran."""
    if not name or not str(name).strip():
        return name, qty, 1
    raw = str(name).strip()
    n_strict = normalize_text(raw)
    n_loose = normalize_text_loose(raw)
    rules = merged_special_rules(restaurant)

    for key, val in rules.items():
        ks = normalize_text(str(key))
        kl = normalize_text_loose(str(key))
        if ks and (ks in n_strict or ks in n_loose):
            return val[0], qty * val[1], val[1]
        if kl and (kl in n_loose or kl in n_strict):
            return val[0], qty * val[1], val[1]

    for key, val in rules.items():
        ks = normalize_text(str(key))
        if len(ks) < 3:
            continue
        if fuzz.token_set_ratio(ks, n_strict) < 88:
            continue
        # 2+ kəlməli açar: bütün kəlmələr çekdə token kimi olmalı (yanlış Juice tutulmasının qarşısı)
        if len(ks.split()) >= 2 and not _all_rule_key_tokens_in_receipt(ks, n_strict):
            continue
        return val[0], qty * val[1], val[1]

    return name, qty, 1


def _fuzz_proc(x):
    return normalize_text(str(x))


def _fuzz_loose(x):
    return normalize_text_loose(str(x))


def _soft_word_gate(q_norm, m_norm, score, strict=False):
    q_words = [w for w in q_norm.split() if len(w) > 2]
    high = 82 if strict else 76
    pr_min = 62 if strict else 52
    if not q_words or score >= high:
        return True
    if any(w in m_norm for w in q_words):
        return True
    return fuzz.partial_ratio(q_norm, m_norm) >= pr_min


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

    for choice in choices:
        if proc_fn(choice) == q_norm:
            return str(choice), 100.0

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

    # İki baza sətri eyni xala yaxındırsa — səhv seçim riski; təhlükəsiz rejimdə rədd
    if (
        score_margin is not None
        and score < 99.9
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


def standardize_columns(df):
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
            renamed[col] = "id"
    return df.rename(columns=renamed)


def normalize_restaurant_name(name):
    return str(name).lower().replace("ı", "i").replace("i̇", "i").strip()


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


def _resolve_db_path(res_name, category):
    suffix = "horeca" if category == "Horeca" else "dk"
    target_prefix = f"ana_{normalize_restaurant_name(res_name)}_{suffix}"
    for file_name in os.listdir("."):
        normalized_file = normalize_restaurant_name(file_name)
        if normalized_file.startswith(target_prefix):
            if file_name.lower().endswith((".xlsx", ".csv")):
                return file_name
    return None


@st.cache_data(ttl=30, show_spinner=False)
def get_db(res_name, category):
    path = _resolve_db_path(res_name, category)
    if not path:
        return None
    try:
        if path.lower().endswith(".xlsx"):
            return pd.read_excel(path)
        return pd.read_csv(path)
    except Exception:
        return None


def build_export_file_name(restaurant, category):
    category_tag = "horeca" if category == "Horeca" else "dk"
    restaurant_tag = normalize_restaurant_name(restaurant).replace(" ", "_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    return f"clopos_{restaurant_tag}_{category_tag}_{timestamp}.xlsx"


def to_bold_excel_bytes(dataframe):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        dataframe.to_excel(writer, index=False, sheet_name="CLOPOS")
        sheet = writer.sheets["CLOPOS"]
        for cell in sheet[1]:
            cell.font = Font(bold=True)
    output.seek(0)
    return output.getvalue()


def to_clopos_workbook_bytes(clopos_df, unmatched_df=None):
    """CLOPOS + istəyə görə Tapılmayanlar vərəqi (ASCII ad — Excel uyğunluğu)."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        clopos_df.to_excel(writer, index=False, sheet_name="CLOPOS")
        ws = writer.sheets["CLOPOS"]
        for cell in ws[1]:
            cell.font = Font(bold=True)
        if unmatched_df is not None and not unmatched_df.empty:
            unmatched_df.to_excel(writer, index=False, sheet_name="Tapilmayanlar")
            ws2 = writer.sheets["Tapilmayanlar"]
            for cell in ws2[1]:
                cell.font = Font(bold=True)
    output.seek(0)
    return output.getvalue()


def to_tapilmayan_only_bytes(unmatched_df):
    """Yalnız əl ilə iş üçün Tapılmayanlar vərəqi."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        unmatched_df.to_excel(writer, index=False, sheet_name="Tapilmayanlar")
        ws = writer.sheets["Tapilmayanlar"]
        for cell in ws[1]:
            cell.font = Font(bold=True)
    output.seek(0)
    return output.getvalue()


def _first_id_for_name(df_base, m_name):
    m = str(m_name).strip()
    sub = df_base.loc[df_base["ad"].astype(str).str.strip() == m, "id"]
    if sub.empty:
        raise KeyError(f"id tapılmadı: {m!r}")
    return int(sub.iloc[0])


# --- SİDEBAR ---
st.sidebar.markdown("#### 🏢 Restoran seçimi")
res_options = discover_restaurants()
if st.session_state.selected_res not in res_options:
    st.session_state.selected_res = res_options[0]

for res_opt in res_options:
    label = f"{res_opt} ✅" if st.session_state.selected_res == res_opt else res_opt
    if st.sidebar.button(label, key=f"btn_{res_opt}", use_container_width=True):
        st.session_state.selected_res = res_opt
        st.rerun()

st.sidebar.info("Ana baza faylları GitHub mənbəsindən avtomatik oxunur.")

# --- PANELLƏR ---
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
        "**COST** = çekdə **1 vahid ₼** (sətirin ümumi miqdarına görə qiymət) **÷ Miqdar** "
        "= **bir vahidin qiyməti** (toplama yox, bölmə). Clopos faylında bu dəyər lazımdır. "
        "**Təhlükəsiz rejim**: yalnız aydın uyğunluq qəbul edilir; qalanlar **Tapılmayanlar** "
        "vərəqində əl ilə doldurmaq üçündür."
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
            df_c = standardize_columns(df_c)
            df_base = standardize_columns(df_base)

            required_cek = {"ad", "miqdar"}
            required_base = {"ad", "id"}
            if not required_cek.issubset(set(df_c.columns)):
                st.error("Çek faylında `Ad` və `Miqdar` sütunları tapılmadı.")
                st.stop()
            if not required_base.issubset(set(df_base.columns)):
                st.error("Baza faylında `Ad` və `id` sütunları tapılmadı.")
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
            choices = df_base["ad"].tolist()
            fail_debug = []
            tapilmayan_rows = []
            safe_mode = not aggressive_match
            for _, row in df_c.iterrows():
                o_name = ""
                p_name = ""
                try:
                    o_name = str(row.get("ad", "")).strip()
                    if not o_name or o_name.lower() in ("nan", "none"):
                        continue
                    o_qty = parse_az_number(row.get("miqdar", 0))
                    unit_price = parse_az_number(row.get("price", 0))
                    if o_qty == 0:
                        continue

                    p_name, p_qty, _fct = apply_special_logic(o_name, o_qty, curr)
                    # Çekdəki «1 vahid ₼» = həmin sətirdəki ümumi miqdarın qiyməti → Clopos üçün
                    # bir vahidin qiyməti: həmin məbləğ ÷ çek miqdarı (fct yalnız miqdarı dəyişir, COST-a vurulmur).
                    cost = (unit_price / o_qty) if o_qty != 0 else 0
                    m_name, _score = get_best_match(
                        p_name,
                        choices,
                        threshold=match_thr,
                        safe_mode=safe_mode,
                    )
                    if m_name:
                        mid = _first_id_for_name(df_base, m_name)
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
                                "Çekdəki_ad": o_name,
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
                            "Çekdəki_ad": o_name
                            or str(row.get("ad", "")).strip()
                            or "(xəta)",
                            "Miqdar": eq_x if eq_x else "",
                            "Bir_vahid_COST": round(cst_x, 4) if eq_x else "",
                            "ID_əl_ile": "",
                            "Baza_Adı_əl_ile": "",
                        }
                    )
                    continue

            if not final_list:
                st.warning(
                    "Uyğun məhsul tapılmadı. Ad yazılışları fərqli ola bilər və ya baza faylı uyğun deyil."
                )
                st.info(
                    f"Yoxlanan çek sətri: {len(df_c)} | Baza məhsulu: {len(df_base)} | "
                    f"Uğursuz emal/match sayı: {errors}"
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
                sample = (
                    df_c[["ad", "miqdar"]]

import io
import os
import re
from datetime import datetime

import pandas as pd
import streamlit as st
from openpyxl.styles import Font
from rapidfuzz import fuzz, process

from rules import SPECIAL_RULES  # Qaydaları digər fayldan çəkir

st.set_page_config(page_title="ROOM CLOPOS Online", layout="wide")

if "selected_res" not in st.session_state:
    st.session_state.selected_res = "ROOM"
if "last_export" not in st.session_state:
    st.session_state.last_export = None


def normalize_text(text):
    if not text:
        return ""
    text = str(text).lower().strip()
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
    text = str(text).lower().strip()
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


def apply_special_logic(name, qty):
    n_norm = normalize_text(name)
    for key, val in SPECIAL_RULES.items():
        if normalize_text(key) in n_norm:
            return val[0], qty * val[1], val[1]
    return name, qty, 1


def _fuzz_proc(x):
    return normalize_text(str(x))


def _fuzz_loose(x):
    return normalize_text_loose(str(x))


def _soft_word_gate(q_norm, m_norm, score):
    q_words = [w for w in q_norm.split() if len(w) > 2]
    if not q_words or score >= 76:
        return True
    if any(w in m_norm for w in q_words):
        return True
    return fuzz.partial_ratio(q_norm, m_norm) >= 52


def _match_with_processor(q_raw, choices, threshold, proc_fn, skip_word_gate=False):
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

    best = process.extractOne(
        q,
        choices,
        scorer=fuzz.token_set_ratio,
        processor=proc_fn,
    )
    if not best:
        return None, 0

    best_match = str(best[0])
    score = float(best[1])

    if score < threshold:
        best2 = process.extractOne(
            q, choices, scorer=fuzz.WRatio, processor=proc_fn
        )
        if best2 and float(best2[1]) >= threshold:
            best_match = str(best2[0])
            score = float(best2[1])

    m_norm = proc_fn(best_match)
    if not skip_word_gate and not _soft_word_gate(q_norm, m_norm, score):
        return None, score

    if score < threshold:
        return None, score

    return best_match, score


def get_best_match(query_name, choices, threshold=68):
    """Sıx → loose → son çarə (daha aşağı hədd, söz süzgəci olmadan)."""
    r = _match_with_processor(query_name, choices, threshold, _fuzz_proc)
    if r[0]:
        return r
    loose_thr = max(52, int(threshold) - 7)
    r = _match_with_processor(query_name, choices, loose_thr, _fuzz_loose)
    if r[0]:
        return r
    # Heç biri keçmirsə: ən yaxın variantı yalnız xalla qəbul et (səhv uyğun riski var, amma boş qalmır)
    last_thr = max(32, int(threshold) - 26)
    return _match_with_processor(
        query_name, choices, last_thr, _fuzz_loose, skip_word_gate=True
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
        "= **bir vahidin qiyməti** (toplama yox, bölmə). Clopos faylında bu dəyər lazımdır."
    )
    col_a, col_b, col_c = st.columns([1, 1, 1])
    cat = col_a.selectbox("Sahə:", ["Horeca", "Dark Kitchen"])
    match_thr = col_c.slider(
        "Uyğunluq həddi (%) — aşağı = daha çox sətir keçər, risk artar",
        min_value=50,
        max_value=92,
        value=58,
        help="Son çarə mərhələ avtomatikdir; yenə azdırsa sürgünü 50–55ə sal.",
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

                    p_name, p_qty, _fct = apply_special_logic(o_name, o_qty)
                    # Çekdəki «1 vahid ₼» = həmin sətirdəki ümumi miqdarın qiyməti → Clopos üçün
                    # bir vahidin qiyməti: həmin məbləğ ÷ çek miqdarı (fct yalnız miqdarı dəyişir, COST-a vurulmur).
                    cost = (unit_price / o_qty) if o_qty != 0 else 0
                    m_name, _score = get_best_match(
                        p_name, choices, threshold=match_thr
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
                            "Qaydadan sonra": p_name,
                            "Ən yaxın (token_set)": hits[0][0] if hits else "",
                            "Xal": round(float(hits[0][1]), 1) if hits else "",
                            "2-ci": hits[1][0] if len(hits) > 1 else "",
                            "2 xal": round(float(hits[1][1]), 1) if len(hits) > 1 else "",
                            "Loose 1": hits_l[0][0] if hits_l else "",
                            "Loose xal": round(float(hits_l[0][1]), 1) if hits_l else "",
                        }
                        fail_debug.append(row_dbg)
                except (ValueError, TypeError, KeyError) as ex:
                    errors += 1
                    fail_debug.append(
                        {
                            "Çekdə ad": o_name,
                            "Qaydadan sonra": p_name,
                            "Ən yaxın (token_set)": f"(xəta) {type(ex).__name__}",
                            "Xal": "",
                            "2-ci": str(ex)[:120],
                            "2 xal": "",
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
                sample = (
                    df_c[["ad", "miqdar"]]
                    .dropna(subset=["ad"])
                    .head(10)
                    .rename(columns={"ad": "Çekdə ad", "miqdar": "Miqdar"})
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

            st.success(f"{len(res_df)} məhsul hazırlandı.")
            if errors:
                st.info(f"{errors} sətir format xətasına görə keçildi.")

            st.dataframe(res_df, use_container_width=True)
            export_name = build_export_file_name(curr, cat)
            export_bytes = to_bold_excel_bytes(res_df)
            st.session_state.last_export = {
                "restaurant": curr,
                "category": cat,
                "rows": len(res_df),
                "file_name": export_name,
                "file_bytes": export_bytes,
                "preview_df": res_df,
            }
            st.success(f"Hazır fayl: `{export_name}`")
            st.download_button(
                "📥 Endir",
                export_bytes,
                export_name,
                key="download_current",
            )
        else:
            st.error(
                "Uyğun ana baza tapılmadı. Repo daxilində fayl adı `ana_<restoran>_<horeca/dk>` formatında olmalıdır."
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
        st.write(f"Fayl adı: `{saved_export['file_name']}`")
        st.dataframe(saved_export["preview_df"], use_container_width=True)
        st.download_button(
            "📥 Son faylı yenidən endir",
            saved_export["file_bytes"],
            saved_export["file_name"],
            key="download_saved",
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
        df_o = standardize_columns(df_o)
        df_b = standardize_columns(df_b)
        db = get_db(curr, ctrl_cat)
        if db is not None:
            db = standardize_columns(db)
            db["ad"] = db["ad"].astype(str).str.strip()
            db["id"] = pd.to_numeric(db["id"], errors="coerce")
            db = db.dropna(subset=["id", "ad"])
            db["id"] = db["id"].astype(int)
            db = db.drop_duplicates(subset=["ad"], keep="first")
            if "id" not in df_b.columns:
                st.error("Analiz faylında `ID` / `id` sütunu tapılmadı.")
                st.stop()
            bot_ids = set(df_b["id"].astype(int).tolist())
            missing = []
            for _, row in df_o.iterrows():
                name = str(row.get("ad", ""))
                p_name, _, _ = apply_special_logic(name, 1)
                m_name, _ = get_best_match(
                    p_name, db["ad"].astype(str).str.strip().tolist(), threshold=72
                )
                if m_name:
                    tid = _first_id_for_name(db, m_name)
                    if tid not in bot_ids:
                        missing.append(name)
                else:
                    missing.append(f"{name} (Bazada yoxdur)")
            st.table(pd.DataFrame(missing, columns=["Tapılmayanlar"]))
        else:
            st.error("Uyğun ana baza tapılmadı.")

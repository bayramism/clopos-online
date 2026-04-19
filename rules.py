# Çekdəki ad bu lüğətdəki açarla uyğun gələndə: (bazada axtarılacaq ad, miqdar əmsalı)
# FORMAT: "Çekdə görünən hissə": ("Bazadakı dəqiq ad", əmsal)
#
# — SPECIAL_RULES_COMMON: bütün restoranlara şamil olunur (təkrar yazmırsan).
# — SPECIAL_RULES_BY_RESTAURANT: yalnız həmin restoran üçün əlavə və ya eyni açarda
#   ümumi qaydanı əvəzləyir (restoran sətiri üstün gəlir).
# Açar sırası: əvvəl restoran lüğəti, sonra ümumidə olmayan açarlar — ilk uyğun qalibdir.
#
# Restoran açarı paneldəki adla eyni olmalıdır (məs. ROOM, BIBLIOTEKA, FINESTRA) — böyük hərf.

from __future__ import annotations


def _rules_rest_key(restaurant: str) -> str:
    return (
        str(restaurant or "")
        .lower()
        .replace("ı", "i")
        .replace("i̇", "i")
        .strip()
        .upper()
    )


def merged_special_rules(restaurant: str) -> dict:
    """Ümumi qaydalar + seçilmiş restoran qaydaları (eyni açarda restoran üstündür)."""
    rk = _rules_rest_key(restaurant)
    per = SPECIAL_RULES_BY_RESTAURANT.get(rk) or {}
    out = dict(per)
    for k, v in SPECIAL_RULES_COMMON.items():
        if k not in out:
            out[k] = v
    return out


# --- Bütün restoranlar üçün (təkrarlamamaq üçün bura yaz) ---
SPECIAL_RULES_COMMON = {
    "sunger": ("Sanitex 6li sunger", 1),
    "tabosco": ("Tabosco (kg)", 0.06),
    "worcestershire": ("Worcestershire sauce (kg)", 0.265),
    "craft": ("Craft mehsullar (lt)", 0.7),
    "Sandora Albali (ed)": ("Juice (l)", 1),
    "Sandora Shaftali (ed)": ("Juice (l)", 1),
    "Sandora Ananas (ed)": ("Juice (l)", 1),
    "Sandora Alma (ed)": ("Juice (l)", 1),
    "Sandora Portagal -mandarin": ("Juice (l)", 1),
    "Sandora gilemeyve": ("Juice (l)", 1),
}

# --- Yalnız həmin restoran + ümumi ilə birləşəndə (boş ola bilər) ---
SPECIAL_RULES_BY_RESTAURANT = {
    # "ROOM": {
    #     "lokal mehsul": ("Bazada dəqiq ad", 1),
    # },
    "BIBLIOTEKA": {
        "avokado": ("Avocado new", 1),
        "mango": ("Mango new", 1),
        "kelem": ("Kelem", 1),
        "sunger": ("Sanitex 6li sunger", 1),
        "Pemalux toz 400 qr": ("Pemalux toz 400 qr", 0.4),
        "qatiq": ("Qatiq 450gr", 0.45),
        "Dondurulmush shabalid (kg)": ("Dondurulmush shabalid (kg)", 0.5),
        "Qaymaq Petmol 33%": ("Qaymaq Petmol 33% (kg)", 0.5),
        "narsherab": ("Narsherab (kg)", 0.345),
        "sirab qazli": ("Sirab Qazli (kg)", 0.5),
        "Tsar un": ("Tsar un (kg)", 5),
        "zire zeytun yagi": ("Zeytun Yağı", 1),
        "borges zeytun yagi": ("Zeytun Yağı", 1),
        "cola 2l": ("cola 2l", 2),
        "sprite 2l": ("sprite 2l", 2),
        "ice cream": ("Ice cream Room (kq)", 1),
        "gwen": ("Ice cream Room (kq)", 1),
        "puste": ("Fistiq (kg)", 1),
        "fistiq": ("Fistiq (kg)", 1),
        "Yakamoz Kornishon 370gr (ed)": ("Yakamoz Kornishon 370gr (ed)", 0.28),
        "Mara Balsamic Vinegar 0,5l (ed)": ("Mara Balsamic Vinegar 0,5l (ed)", 0.5),
        "Osvejitel": ("Air Wick 260ml (ed)", 1),
        "Bonduelle Tumsuz Qara Zeytun 300g (ed)": ("Bonduelle Tumsuz Qara Zeytun 300g (ed)", 0.11),
        "Bonduelle Qorox Yashil 420g (ed)": ("bonduelle qorox yashil 420", 0.42),
        "Bonduelle Qargidali 425g (ed)": ("Bonduelle Qargidali 425g (ed)", 0.425),
        "Gobelek 0.5kg (ed)": ("Gobelek 0.5kg (ed)", 0.5),
        "Bizim Tarla Tomat Pastasi 720gr (ed)": ("Bizim Tarla Tomat Pastasi 720gr (ed)", 0.72),
        "President Qaymaq 200gr (ed)": ("President Qaymaq 200gr (ed)", 0.2),
        "Sutash Qaymaq 200gr (ed)": ("President Qaymaq 200gr (ed)", 0.2),
        "Encir qurusu 500gr (ed)": ("Encir qurusu 500gr (ed)", 0.5),
        "Barilla Fettucini n166 500g (ed)": ("Barilla Fettucini n166 500g (ed)", 0.5),
        "Zire Mehsul Qara Zeytun 720g (ed)": ("Zire Mehsul Qara Zeytun 720g (ed)", 072),
        "Tiramisu pecenye 400 gr": ("Tiramisu pecenye 400 gr", 0.4),  
        "Final Qargidali Yag 5l (ed)": ("Final Qargidali Yag 5l (ed)", 5),
        "Kent bulyon 90qr (ed)": ("Kent bulyon 90qr (ed)", 0.09),
        "Marini Savoiardi 400gr (ed)" ("Tiramisu pecenye 400 gr", 0.4), 
        ),
    },
}

# Köhnə importlar üçün: yalnız ümumi siyahı (restoran birləşməsi app-də merged_special_rules ilə)
SPECIAL_RULES = SPECIAL_RULES_COMMON

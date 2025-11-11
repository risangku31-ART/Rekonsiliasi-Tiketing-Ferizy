# app.py — Hanya Tabel Utama
import io, zipfile, calendar
from datetime import date
import pandas as pd, numpy as np, streamlit as st

st.set_page_config(page_title="Rekon Ferizy", layout="wide")

# =========================
# Konstanta & Util
# =========================
CHANNEL_COLS = [
    "Cash","Prepaid - BRI","Prepaid - Mandiri","Prepaid - BNI","Prepaid - BCA",
    "SKPT","IFCS","Reedem","ESPAY","FINNET"
]
COL_LETTERS = ["B","H","K","AA","Q"]        # Tanggal, Kanal, Amount, Deskripsi, Pelabuhan
CSV_USECOLS = [1,7,10,26,16]                # posisi 0-based utk B,H,K,AA,Q

def normalize_str_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower()

def format_id_number(v, decimals=0):
    if pd.isna(v): return ""
    try: n = float(v)
    except: return v
    s = f"{n:,.{decimals}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def df_format_id(df, cols, decimals=0):
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].apply(lambda x: format_id_number(x, decimals))
    return out

# =========================
# Reader hemat RAM (ambil hanya B,H,K,AA,Q)
# =========================
def _read_excel_subset(b: bytes) -> pd.DataFrame:
    bio = io.BytesIO(b)
    df = pd.read_excel(bio, usecols="B,H,K,AA,Q", dtype=object, engine=None)
    df.columns = COL_LETTERS[:df.shape[1]]
    for c in COL_LETTERS:
        if c not in df.columns: df[c] = np.nan
    return df[COL_LETTERS]

def _read_csv_subset(b: bytes) -> pd.DataFrame:
    bio = io.BytesIO(b)
    # Coba pyarrow bila ada, fallback engine python/default
    try:
        import pyarrow  # optional
        df = pd.read_csv(bio, engine="pyarrow", usecols=CSV_USECOLS)
    except Exception:
        bio.seek(0)
        try:
            df = pd.read_csv(bio, engine="python", on_bad_lines="skip", low_memory=False, usecols=CSV_USECOLS)
        except Exception:
            bio.seek(0)
            df = pd.read_csv(bio, low_memory=False)
            if df.shape[1] >= 27:  # potong jika kolom cukup
                df = df.iloc[:, CSV_USECOLS]
    df.columns = COL_LETTERS[:df.shape[1]]
    for c in COL_LETTERS:
        if c not in df.columns: df[c] = np.nan
    return df[COL_LETTERS]

@st.cache_data(show_spinner=False)
def read_single(uploaded_name: str, b: bytes):
    name = uploaded_name.lower()
    if name.endswith((".xlsx",".xls")):
        return _read_excel_subset(b)
    elif name.endswith(".csv"):
        return _read_csv_subset(b)
    else:
        return pd.DataFrame(columns=COL_LETTERS)

@st.cache_data(show_spinner=False)
def read_zip(archive_bytes: bytes):
    zf = zipfile.ZipFile(io.BytesIO(archive_bytes))
    frames = []
    for info in zf.infolist():
        if info.is_dir(): continue
        content = zf.read(info)
        low = info.filename.lower()
        try:
            if low.endswith(".csv"):
                frames.append(_read_csv_subset(content))
            elif low.endswith((".xlsx",".xls")):
                frames.append(_read_excel_subset(content))
        except Exception:
            pass
    if frames:
        df = pd.concat(frames, ignore_index=True, sort=False)
        return df[COL_LETTERS]
    return pd.DataFrame(columns=COL_LETTERS)

# =========================
# Agregasi: Tabel Utama (harian semua pelabuhan)
# =========================
def build_daily_table(df_month: pd.DataFrame, year_sel: int, month_sel: int) -> pd.DataFrame:
    """Output: Tanggal + tiap kanal (sum K) + Total."""
    last_day = calendar.monthrange(year_sel, month_sel)[1]
    all_days = pd.date_range(f"{year_sel}-{month_sel:02d}-01", periods=last_day, freq="D").date
    res = pd.DataFrame({"Tanggal": all_days})

    if df_month.empty:
        for c in CHANNEL_COLS: res[c] = 0.0
        res["Total"] = 0.0
        return res

    h  = normalize_str_series(df_month["H"]).fillna("")
    aa = normalize_str_series(df_month["AA"]).fillna("") if "AA" in df_month.columns else pd.Series([""]*len(df_month))
    amt = pd.to_numeric(df_month["K"], errors="coerce").fillna(0.0)
    tgl = pd.to_datetime(df_month["B"], errors="coerce").dt.date

    # Aturan kanal
    mask_esp = aa.str.contains("esp", na=False)
    masks = {
        "Cash": h.eq("cash"),
        "Prepaid - BRI": h.eq("prepaid-bri"),
        "Prepaid - Mandiri": h.eq("prepaid-mandiri"),
        "Prepaid - BNI": h.eq("prepaid-bni"),
        "Prepaid - BCA": h.eq("prepaid-bca"),
        "SKPT": h.eq("skpt"),
        "IFCS": h.eq("cash"),   # IFCS = Cash
        "Reedem": (
            h.str.contains("reedem", na=False) | aa.str.contains("reedem", na=False) |
            h.str.contains("redeem", na=False) | aa.str.contains("redeem", na=False)
        ),
        "ESPAY":  h.eq("finpay") & mask_esp,          # H = finpay & AA mengandung 'esp'
        "FINNET": h.eq("finpay") & ~mask_esp,         # H = finpay & AA tidak mengandung 'esp'
    }

    for key, m in masks.items():
        s = pd.Series(np.where(m, amt, 0.0)).groupby(tgl).sum()
        res[key] = s.reindex(all_days, fill_value=0.0).values

    res["Total"] = res[CHANNEL_COLS].sum(axis=1)
    return res

# =========================
# Sidebar: Upload & Periode
# =========================
df = None
with st.sidebar:
    upl = st.file_uploader("Upload Excel/CSV/ZIP (ambil kolom B,H,K,AA,Q saja)", type=["xlsx","xls","csv","zip"])
    if upl:
        by = upl.getvalue()
        if upl.name.lower().endswith(".zip"):
            df = read_zip(by)
        else:
            df = read_single(upl.name, by)

    # Parameter bulan/tahun (minimalis)
    today = date.today()
    if upl and df is not None and not df.empty and df["B"].notna().any():
        dmin = pd.to_datetime(df["B"], errors="coerce").min()
        dmax = pd.to_datetime(df["B"], errors="coerce").max()
        years = list(range(int((dmin or today).year), int((dmax or today).year)+1))
        default_year = int((dmax or today).year); default_month = int((dmax or today).month)
    else:
        years=[today.year]; default_year=today.year; default_month=today.month

    bulan_id = ["Januari","Februari","Maret","April","Mei","Juni","Juli","Agustus","September","Oktober","November","Desember"]
    year_sel = st.selectbox("Tahun", years, index=years.index(default_year))
    month_sel_name = st.selectbox("Bulan", bulan_id, index=default_month-1)
    month_sel = bulan_id.index(month_sel_name)+1

# =========================
# Main: TAMPILKAN HANYA TABEL UTAMA
# =========================
if not upl or df is None or df.empty:
    st.stop()

# Filter ke bulan/tahun pilihan berdasarkan kolom B (tanggal asli)
df_valid = df[df["B"].notna()].copy()
df_valid["Tanggal_ts"] = pd.to_datetime(df_valid["B"], errors="coerce")
df_month = df_valid[
    (df_valid["Tanggal_ts"].dt.year == year_sel) &
    (df_valid["Tanggal_ts"].dt.month == month_sel)
].copy()

# Tabel Utama (harian) + Sub Total — SATU-SATUNYA OUTPUT
daily = build_daily_table(df_month, year_sel, month_sel)
subtotal = daily[CHANNEL_COLS+["Total"]].sum(numeric_only=True)
daily_with_sub = pd.concat([daily, pd.DataFrame([{"Tanggal":"Sub Total", **subtotal.to_dict()}])], ignore_index=True)

st.dataframe(df_format_id(daily_with_sub, CHANNEL_COLS+["Total"], 0), use_container_width=True)

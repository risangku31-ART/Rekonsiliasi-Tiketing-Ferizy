# app.py
import io, zipfile, calendar
from datetime import date
import pandas as pd, numpy as np, streamlit as st

st.set_page_config(page_title="Rekon Ferizy - Tabel Utama", layout="wide")
st.title("Detail Tiket from Payment Report Ferizy — Tabel Utama")
st.caption(
    "Hanya Tabel Harian (1–28/29/30/31) + Sub Total. "
    "Baca kolom B (Tanggal), H (Kanal), K (Nominal), AA (Deskripsi), Q (Pelabuhan). "
    "Deteksi ESPAY/FINNET/reedem diperluas agar tidak tercampur. Angka tampil dengan titik ribuan."
)

# =========================
# Konstanta & Util
# =========================
CHANNEL_COLS = ["Cash","Prepaid - BRI","Prepaid - Mandiri","Prepaid - BNI","Prepaid - BCA","SKPT","IFCS","reedem","ESPAY","FINNET"]
COL_LETTERS = ["B","H","K","AA","Q"]           # Tanggal, Kanal, Amount, Deskripsi, Pelabuhan
CSV_USECOLS = [1,7,10,26,16]                   # index 0-based utk B,H,K,AA,Q

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
# Reader hemat RAM: ambil hanya B,H,K,AA,Q
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
            if df.shape[1] >= 27:
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
    frames, manifest = [], []
    for info in zf.infolist():
        if info.is_dir(): continue
        try:
            content = zf.read(info)
            if info.filename.lower().endswith(".csv"):
                frames.append(_read_csv_subset(content))
                manifest.append({"file": info.filename, "type": "csv"})
            elif info.filename.lower().endswith((".xlsx",".xls")):
                frames.append(_read_excel_subset(content))
                manifest.append({"file": info.filename, "type": "excel"})
        except Exception as e:
            manifest.append({"file": info.filename, "error": str(e)})
    df = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame(columns=COL_LETTERS)
    return df[COL_LETTERS], manifest

# =========================
# Agregasi: Tabel Utama (harian semua pelabuhan)
# =========================
def build_daily_table(df_month, year_sel, month_sel):
    """Hasil: Tanggal + tiap kanal (sum K) + Total."""
    last_day = calendar.monthrange(year_sel, month_sel)[1]
    all_days = pd.date_range(f"{year_sel}-{month_sel:02d}-01", periods=last_day, freq="D").date
    res = pd.DataFrame({"Tanggal": all_days})

    if df_month.empty:
        for c in CHANNEL_COLS: res[c] = 0.0
        res["Total"] = 0.0
        return res

    h  = normalize_str_series(df_month["H"])
    aa = normalize_str_series(df_month["AA"]) if "AA" in df_month.columns else pd.Series([""]*len(df_month))
    amt = pd.to_numeric(df_month["K"], errors="coerce").fillna(0.0)
    tgl = pd.to_datetime(df_month["B"], errors="coerce").dt.date

    # ====== DETEKSI YANG LEBIH ROBUST ======
    # Finpay/ESP/ESPAY/Finnet bisa muncul di H atau AA; kita gabungkan aturan & hindari tabrakan.
    mask_finpay     = h.str.contains("finpay", na=False)
    mask_esp_token  = aa.str.contains("espay", na=False) | aa.str.contains("esp", na=False) \
                      | h.str.contains("espay", na=False) | h.str.contains(r"\besp\b", na=False)
    espay_mask      = mask_esp_token | (mask_finpay & (aa.str.contains("espay", na=False) | aa.str.contains("esp", na=False)))
    finnet_mask     = (h.str.contains("finnet", na=False) | (mask_finpay & ~(aa.str.contains("espay", na=False) | aa.str.contains("esp", na=False)))) & ~espay_mask
    reedem_mask     = h.str.contains("reedem", na=False) | aa.str.contains("reedem", na=False)

    masks = {
        "Cash": h.eq("cash"),
        "Prepaid - BRI": h.eq("prepaid-bri"),
        "Prepaid - Mandiri": h.eq("prepaid-mandiri"),
        "Prepaid - BNI": h.eq("prepaid-bni"),
        "Prepaid - BCA": h.eq("prepaid-bca"),
        "SKPT": h.eq("skpt"),
        "IFCS": h.eq("cash"),     # IFCS = Cash
        "reedem": reedem_mask,
        "ESPAY": espay_mask,
        "FINNET": finnet_mask,
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
    st.header("📤 Upload & Periode")
    upl = st.file_uploader("Upload Excel/CSV/ZIP (ambil kolom B,H,K,AA,Q saja)", type=["xlsx","xls","csv","zip"])
    if upl:
        by = upl.getvalue()
        if upl.name.lower().endswith(".zip"):
            df, manifest = read_zip(by)
            with st.expander("Daftar isi ZIP"): st.write(manifest)
        else:
            df = read_single(upl.name, by)
        st.caption(f"Baris dibaca: {len(df)}")

    st.markdown("---")
    st.subheader("🗓️ Periode")
    today = date.today()
    if upl and not df.empty and df["B"].notna().any():
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
# Main: TABEL UTAMA SAJA
# =========================
if not upl:
    st.info("Silakan upload file di sidebar untuk memulai."); st.stop()
if df is None or df.empty:
    st.error("Tidak ada data yang terbaca."); st.stop()

st.write(":small_blue_diamond: Baris data terunggah:", len(df))

# Filter ke bulan/tahun pilihan berdasarkan kolom B (tanggal asli)
df_valid = df[df["B"].notna()].copy()
df_valid["Tanggal_ts"] = pd.to_datetime(df_valid["B"], errors="coerce")
df_month = df_valid[
    (df_valid["Tanggal_ts"].dt.year == year_sel) &
    (df_valid["Tanggal_ts"].dt.month == month_sel)
].copy()

# Tabel Utama (harian) + Sub Total
st.subheader(f"Tabel Utama — Harian {bulan_id[month_sel-1]} {year_sel} + Sub Total")
daily = build_daily_table(df_month, year_sel, month_sel)
subtotal = daily[CHANNEL_COLS+["Total"]].sum(numeric_only=True)
daily_with_sub = pd.concat([daily, pd.DataFrame([{"Tanggal":"Sub Total", **subtotal.to_dict()}])], ignore_index=True)

st.dataframe(df_format_id(daily_with_sub, CHANNEL_COLS+["Total"], 0), use_container_width=True)
st.download_button(
    "Unduh Tabel Utama (CSV)",
    daily_with_sub.to_csv(index=False).encode("utf-8"),
    file_name=f"tabel_utama_harian_{year_sel}_{month_sel:02d}.csv",
    mime="text/csv"
)

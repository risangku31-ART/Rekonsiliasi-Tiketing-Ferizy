# app.py
import io
import zipfile
from datetime import date
import pandas as pd
import streamlit as st

# ================= App Config =================
st.set_page_config(page_title="Tabel Rekon Otomatis - Ferizy", layout="wide")
st.title("Detail Tiket from Payment Report Ferizy")
st.caption(
    "Upload Payment Report (Excel/CSV/ZIP) di sidebar kiri. "
    "Aplikasi menambahkan kolom **Tanggal** (dari **kolom B**, tanpa jam), "
    "menjumlahkan **nominal** dari **kolom K** per kanal, "
    "menyediakan parameter **bulan/tahun**, dan menampilkan **Rekap Bulanan — Semua Pelabuhan** saja."
)

# ================= Helpers =================
CHANNEL_COLS = [
    "Cash", "Prepaid - BRI", "Prepaid - Mandiri", "Prepaid - BNI",
    "Prepaid - BCA", "SKPT", "IFCS", "Redeem", "ESPAY", "FINNET"
]

def resolve_column(df: pd.DataFrame, letter: str, pos_index: int, fallback_contains=None):
    # why: dukung file dengan header huruf (B/H/K/AA/Q) atau nama bebas
    for c in df.columns:
        if str(c).strip().lower() == letter.lower():
            return c, f"named '{letter}'"
    if fallback_contains:
        for c in df.columns:
            if fallback_contains.lower() in str(c).strip().lower():
                return c, f"semantic match contains '{fallback_contains}'"
    if 0 <= pos_index < len(df.columns):
        return df.columns[pos_index], f"position index {pos_index} ({letter})"
    return None, "missing"

def normalize_str_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower()

def format_id_number(v, decimals: int = 0):
    if pd.isna(v):
        return ""
    try:
        n = float(v)
    except Exception:
        return v
    s = f"{n:,.{decimals}f}"                      # 1,234,567.89
    return s.replace(",", "X").replace(".", ",").replace("X", ".")  # 1.234.567,89

def df_format_id(df: pd.DataFrame, cols, decimals: int = 0) -> pd.DataFrame:
    disp = df.copy()
    for c in cols:
        if c in disp.columns:
            disp[c] = disp[c].apply(lambda x: format_id_number(x, decimals))
    return disp

def _read_csv_bytes(b: bytes) -> pd.DataFrame:
    # why: hindari engine pyarrow agar kompatibel di env tanpa pyarrow/pandas 2.x
    bio = io.BytesIO(b)
    return pd.read_csv(bio, low_memory=False)

def _read_excel_bytes(b: bytes, sheet_name=None) -> pd.DataFrame:
    # why: gunakan parser default; biarkan pandas pilih engine (openpyxl/odf/xlrd sesuai ekstensi)
    bio = io.BytesIO(b)
    try:
        if sheet_name:
            return pd.read_excel(bio, sheet_name=sheet_name, dtype=object)
        return pd.read_excel(bio, dtype=object)
    except Exception:
        # fallback via ExcelFile bila perlu
        bio.seek(0)
        xl = pd.ExcelFile(bio)
        target = sheet_name if (sheet_name in xl.sheet_names) else xl.sheet_names[0]
        return xl.parse(target, dtype=object)

def list_excel_sheets(b: bytes):
    try:
        return pd.ExcelFile(io.BytesIO(b)).sheet_names
    except Exception:
        return []

def read_single_file(uploaded_name: str, b: bytes, sheet=None):
    name = uploaded_name.lower()
    if name.endswith((".xlsx", ".xls")):
        df = _read_excel_bytes(b, sheet_name=sheet)
        sheets = list_excel_sheets(b)
        chosen = sheet if (sheet in sheets) else (sheets[0] if sheets else None)
        return df, sheets, chosen, [{"file": uploaded_name, "type": "excel", "sheet": chosen}]
    elif name.endswith(".csv"):
        df = _read_csv_bytes(b)
        return df, None, None, [{"file": uploaded_name, "type": "csv"}]
    else:
        return None, None, None, []

def read_zip(archive_bytes: bytes):
    zf = zipfile.ZipFile(io.BytesIO(archive_bytes))
    frames, manifest = [], []
    for info in zf.infolist():
        if info.is_dir():
            continue
        fname = info.filename
        lower = fname.lower()
        try:
            with zf.open(info) as f:
                content = f.read()
            if lower.endswith(".csv"):
                df = _read_csv_bytes(content)
                frames.append(df)
                manifest.append({"file": fname, "type": "csv"})
            elif lower.endswith((".xlsx", ".xls")):
                try:
                    sheets = list_excel_sheets(content)
                    sheet = sheets[0] if sheets else None
                    df = _read_excel_bytes(content, sheet_name=sheet)
                    frames.append(df)
                    manifest.append({"file": fname, "type": "excel", "sheet": sheet})
                except Exception as e:
                    manifest.append({"file": fname, "type": "excel", "error": str(e)})
        except Exception as e:
            manifest.append({"file": fname, "type": "unknown", "error": str(e)})
    df_all = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    return df_all, manifest

def build_metrics(df, h_col, aa_col=None, amount_col=None):
    cols_order = CHANNEL_COLS.copy()
    if df.empty:
        out = pd.DataFrame({c: [0.0] for c in cols_order})
        out["Total"] = out.sum(axis=1)
        return out

    h_vals = normalize_str_series(df[h_col])

    def metric_for(mask):
        sub = df[mask] if (mask is not None and mask.any()) else df.iloc[0:0]
        if amount_col and amount_col in df.columns:
            vals = pd.to_numeric(sub[amount_col], errors='coerce')
            return float(vals.sum(skipna=True))
        return float(len(sub))

    cash_mask = h_vals.eq('cash')
    data = {
        'Cash': metric_for(cash_mask),
        'Prepaid - BRI': metric_for(h_vals.eq('prepaid-bri')),
        'Prepaid - Mandiri': metric_for(h_vals.eq('prepaid-mandiri')),
        'Prepaid - BNI': metric_for(h_vals.eq('prepaid-bni')),
        'Prepaid - BCA': metric_for(h_vals.eq('prepaid-bca')),
        'SKPT': metric_for(h_vals.eq('skpt')),
        'IFCS': metric_for(cash_mask),  # why: IFCS = cash
        'Redeem': metric_for(h_vals.eq('redeem')),
        'ESPAY': 0.0,
        'FINNET': 0.0,
    }

    if aa_col is not None and aa_col in df.columns:
        aa_vals = normalize_str_series(df[aa_col])
        data['ESPAY'] = metric_for(h_vals.eq('finpay') & aa_vals.str.contains('esp', na=False))
        data['FINNET'] = metric_for(h_vals.eq('finpay') & ~aa_vals.str.contains('esp', na=False))

    out = pd.DataFrame([data], columns=cols_order)
    out["Total"] = out[cols_order].sum(axis=1)
    return out

# ================= Sidebar: Upload & Periode =================
with st.sidebar:
    st.header("📤 Upload & Parameter")
    uploaded = st.file_uploader(
        "Upload Payment Report (Excel/CSV/ZIP)",
        type=["xlsx", "xls", "csv", "zip"]
    )

    sheet_choice = None
    manifest_info = None

    if uploaded:
        name = uploaded.name.lower()
        data_bytes = uploaded.getvalue()

        if name.endswith(".zip"):
            df, manifest_info = read_zip(data_bytes)
            st.caption(f"ZIP terdeteksi. Tergabung: {len(df)} baris dari {len(manifest_info)} file.")
            with st.expander("Daftar isi ZIP"):
                st.write(manifest_info)
        else:
            df_tmp, sheets, chosen_sheet, mf = read_single_file(uploaded.name, data_bytes, sheet=None)
            manifest_info = mf
            if sheets:
                idx = sheets.index(chosen_sheet) if chosen_sheet in sheets else 0
                sheet_choice = st.selectbox("Pilih sheet:", sheets, index=idx)
                df, _, _, _ = read_single_file(uploaded.name, data_bytes, sheet=sheet_choice)
            else:
                df = df_tmp
            st.caption(f"File: {uploaded.name} | Baris: {len(df)}")

    st.markdown("---")
    st.subheader("🗓️ Periode")

    if uploaded is not None and 'df' in locals() and isinstance(df, pd.DataFrame) and not df.empty:
        b_col, _ = resolve_column(df, 'B', 1)
        if b_col is not None and b_col in df.columns:
            df['Tanggal'] = pd.to_datetime(df[b_col], errors='coerce').dt.date
        else:
            df['Tanggal'] = pd.NaT

        if df['Tanggal'].notna().any():
            dmin = pd.to_datetime(df['Tanggal']).min()
            dmax = pd.to_datetime(df['Tanggal']).max()
            years = list(range(int(dmin.year), int(dmax.year) + 1))
            default_year = int(dmax.year)
            default_month = int(dmax.month)
        else:
            today = date.today()
            years = [today.year]
            default_year = today.year
            default_month = today.month
    else:
        years = [date.today().year]
        default_year = years[0]
        default_month = date.today().month

    bulan_id = ["Januari","Februari","Maret","April","Mei","Juni","Juli","Agustus","September","Oktober","November","Desember"]
    year_sel = st.selectbox("Tahun", years, index=years.index(default_year))
    month_sel_name = st.selectbox("Bulan", bulan_id, index=default_month-1)
    month_sel = bulan_id.index(month_sel_name) + 1

# ================= Main =================
if not uploaded:
    st.info("Silakan upload file di sidebar kiri untuk memulai.")
    st.stop()

if df is None or not isinstance(df, pd.DataFrame) or df.empty:
    st.warning("Tidak ada data yang bisa dibaca dari file yang diunggah.")
    st.stop()

# Pemetaan kolom
h_col, h_found = resolve_column(df, 'H', 7)
k_col, k_found = resolve_column(df, 'K', 10)
aa_col, aa_found = resolve_column(df, 'AA', 26)
b_col, b_found = resolve_column(df, 'B', 1)

if 'Tanggal' not in df.columns:
    if b_col is not None and b_col in df.columns:
        df['Tanggal'] = pd.to_datetime(df[b_col], errors='coerce').dt.date
    else:
        df['Tanggal'] = pd.NaT

with st.expander("Pemetaan kolom (opsional)"):
    st.write({
        "B (Tanggal)": {"mapped_to": b_col, "how": b_found},
        "H (Kanal)": {"mapped_to": h_col, "how": h_found},
        "K (Amount)": {"mapped_to": k_col, "how": k_found},
        "AA (Deskripsi)": {"mapped_to": aa_col, "how": aa_found},
    })

if h_col is None:
    st.error("Kolom H (kanal) tidak ditemukan.")
    st.stop()

# Filter ke bulan/tahun terpilih
if df['Tanggal'].notna().any():
    df_valid = df[df['Tanggal'].notna()].copy()
    df_valid['Tanggal_ts'] = pd.to_datetime(df_valid['Tanggal'])
    df_month = df_valid[
        (df_valid['Tanggal_ts'].dt.year == year_sel) &
        (df_valid['Tanggal_ts'].dt.month == month_sel)
    ].copy()
else:
    df_month = df.iloc[0:0].copy()

# ======= Tabel Utama Saja =======
st.subheader(f"Rekap Bulanan — Semua Pelabuhan (sum kolom K) + Total — {month_sel_name} {year_sel}")
main_metrics_month = build_metrics(df_month, h_col=h_col, aa_col=aa_col, amount_col=k_col)
main_display = df_format_id(main_metrics_month, cols=CHANNEL_COLS + ["Total"], decimals=0)
st.dataframe(main_display, use_container_width=True)

main_month_csv = main_metrics_month.to_csv(index=False).encode('utf-8')
st.download_button(
    "Unduh Rekap Bulanan (CSV)",
    main_month_csv,
    file_name=f"rekap_bulanan_{year_sel}_{month_sel:02d}.csv",
    mime="text/csv"
)

st.success("Selesai. Hanya tabel utama yang ditampilkan.")

# app.py
import io
import calendar
from datetime import date, datetime
import pandas as pd
import numpy as np
import streamlit as st

st.set_page_config(page_title="Tabel Rekon Otomatis - Ferizy", layout="wide")

st.title("Detail Tiket from Payment Report Ferizy")
st.caption(
    "Upload Payment Report (Excel/CSV). Aplikasi menambahkan kolom **Tanggal** (dari kolom **B**, tanpa jam), "
    "menjumlahkan nominal dari **kolom K** untuk setiap kanal, dan menyediakan parameter **bulan/tahun** "
    "supaya tabel harian otomatis terisi 1–28/29/30/31."
)

# -----------------------------
# Helpers
# -----------------------------
def resolve_column(df: pd.DataFrame, letter: str, pos_index: int, fallback_contains=None):
    """
    Cari kolom berdasarkan:
    1) Nama huruf persis (mis. 'H', 'AA', 'Q', 'B', 'K')
    2) (opsional) nama yang mengandung kata kunci
    3) Posisi 0-based (fallback)
    Return: (nama_kolom_ditemukan, cara_menemukan)
    """
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

@st.cache_data(show_spinner=False)
def read_any_file(uploaded_file, sheet=None):
    """
    Baca Excel/CSV dari Streamlit UploadedFile secara aman (bytes buffer),
    dan kembalikan (df, sheets, chosen_sheet). Untuk CSV, sheets=None.
    """
    name = uploaded_file.name.lower()
    data = uploaded_file.getvalue()  # bytes

    if name.endswith(('.xlsx', '.xls')):
        xl = pd.ExcelFile(io.BytesIO(data))
        sheets = xl.sheet_names
        chosen = sheet if (sheet in sheets) else sheets[0]
        df = xl.parse(chosen, dtype=object)
        return df, sheets, chosen
    elif name.endswith('.csv'):
        df = pd.read_csv(io.BytesIO(data), dtype=object)
        return df, None, None
    else:
        st.error("Format tidak didukung. Unggah file Excel (.xlsx) atau CSV.")
        return None, None, None

def build_metrics(df, h_col, aa_col=None, amount_col=None):
    """
    Kembalikan DataFrame satu baris berisi total nominal per kanal.
    - amount_col: kolom nominal yang dijumlahkan (kolom K). Jika None, fallback hitung baris.
    """
    if df.empty:
        return pd.DataFrame({
            'Cash':[0], 'Prepaid - BRI':[0], 'Prepaid - Mandiri':[0], 'Prepaid - BNI':[0],
            'Prepaid - BCA':[0], 'SKPT':[0], 'IFCS':[0], 'Redeem':[0], 'ESPAY':[0], 'FINNET':[0],
        })

    h_vals = normalize_str_series(df[h_col])

    def metric_for(mask):
        sub = df[mask] if (mask is not None and mask.any()) else df.iloc[0:0]
        if amount_col and amount_col in df.columns:
            vals = pd.to_numeric(sub[amount_col], errors='coerce')
            return float(vals.sum(skipna=True))
        else:
            return int(len(sub))

    # Base masks sesuai ketentuan
    cash_mask = h_vals.eq('cash')
    prepaid_bri_mask = h_vals.eq('prepaid-bri')
    prepaid_mandiri_mask = h_vals.eq('prepaid-mandiri')
    prepaid_bni_mask = h_vals.eq('prepaid-bni')
    prepaid_bca_mask = h_vals.eq('prepaid-bca')
    skpt_mask = h_vals.eq('skpt')
    ifcs_mask = h_vals.eq('cash')  # IFCS = ambil dari 'cash'
    redeem_mask = h_vals.eq('redeem')

    # ESPAY / FINNET perlu AA
    if aa_col is not None and aa_col in df.columns:
        aa_vals = normalize_str_series(df[aa_col])
        esp_mask = aa_vals.str.contains('esp', na=False)
        finnet_mask = ~aa_vals.str.contains('esp', na=False)
        espay_mask = h_vals.eq('finpay') & esp_mask
        finnet2_mask = h_vals.eq('finpay') & finnet_mask
    else:
        espay_mask = h_vals == '__no_matches__'    # False
        finnet2_mask = h_vals == '__no_matches__'  # False

    data = {
        'Cash': [metric_for(cash_mask)],
        'Prepaid - BRI': [metric_for(prepaid_bri_mask)],
        'Prepaid - Mandiri': [metric_for(prepaid_mandiri_mask)],
        'Prepaid - BNI': [metric_for(prepaid_bni_mask)],
        'Prepaid - BCA': [metric_for(prepaid_bca_mask)],
        'SKPT': [metric_for(skpt_mask)],
        'IFCS': [metric_for(ifcs_mask)],
        'Redeem': [metric_for(redeem_mask)],
        'ESPAY': [metric_for(espay_mask)],
        'FINNET': [metric_for(finnet2_mask)],
    }
    return pd.DataFrame(data)

def build_daily_table(df_month, h_col, aa_col, amount_col, date_col='Tanggal'):
    """
    Bangun tabel harian (baris = setiap tanggal di bulan terpilih).
    Kolom diisi agregasi nominal (sum kolom K) untuk masing-masing kanal.
    """
    # Tentukan rentang hari dari df_month (sudah difilter ke satu bulan)
    if df_month[date_col].notna().any():
        start_day = pd.to_datetime(df_month[date_col]).min()
        end_day = pd.to_datetime(df_month[date_col]).max()
        # Pastikan benar-benar mencakup seluruh hari dalam bulan terpilih:
        y, m = start_day.year, start_day.month
        last_day = calendar.monthrange(y, m)[1]
        start = pd.Timestamp(year=y, month=m, day=1)
        end = pd.Timestamp(year=y, month=m, day=last_day)
    else:
        return pd.DataFrame(columns=[
            "Tanggal", "Cash", "Prepaid - BRI", "Prepaid - Mandiri", "Prepaid - BNI",
            "Prepaid - BCA", "SKPT", "IFCS", "Redeem", "ESPAY", "FINNET"
        ])

    all_days = pd.date_range(start, end, freq='D').date
    result = pd.DataFrame({"Tanggal": all_days})

    if df_month.empty:
        for c in ["Cash", "Prepaid - BRI", "Prepaid - Mandiri", "Prepaid - BNI",
                  "Prepaid - BCA", "SKPT", "IFCS", "Redeem", "ESPAY", "FINNET"]:
            result[c] = 0.0
        return result

    # Siapkan series bantu
    h_vals = normalize_str_series(df_month[h_col])
    aa_vals = normalize_str_series(df_month[aa_col]) if (aa_col is not None and aa_col in df_month.columns) else pd.Series([None] * len(df_month))
    amt = pd.to_numeric(df_month[amount_col], errors='coerce').fillna(0.0)
    tgl = pd.to_datetime(df_month[date_col]).dt.date

    # Definisikan masker per kanal
    mask = {
        "Cash": h_vals.eq('cash'),
        "Prepaid - BRI": h_vals.eq('prepaid-bri'),
        "Prepaid - Mandiri": h_vals.eq('prepaid-mandiri'),
        "Prepaid - BNI": h_vals.eq('prepaid-bni'),
        "Prepaid - BCA": h_vals.eq('prepaid-bca'),
        "SKPT": h_vals.eq('skpt'),
        "IFCS": h_vals.eq('cash'),
        "Redeem": h_vals.eq('redeem'),
        "ESPAY": (h_vals.eq('finpay') & aa_vals.str.contains('esp', na=False)) if (aa_col is not None and aa_col in df_month.columns) else (h_vals == '__no_matches__'),
        "FINNET": (h_vals.eq('finpay') & ~aa_vals.str.contains('esp', na=False)) if (aa_col is not None and aa_col in df_month.columns) else (h_vals == '__no_matches__'),
    }

    # Hitung sum per hari untuk tiap kanal
    for key, m in mask.items():
        s = pd.Series(np.where(m, amt, 0.0)).groupby(tgl).sum()
        s = s.reindex(all_days, fill_value=0.0)
        result[key] = s.values

    return result

def filter_port(df, q_col, port_name):
    q_vals = normalize_str_series(df[q_col])
    return df[q_vals.eq(port_name.strip().lower())]

# -----------------------------
# Upload
# -----------------------------
uploaded = st.file_uploader("Upload Payment Report (Excel/CSV)", type=["xlsx", "xls", "csv"])

if not uploaded:
    st.info("Silakan upload file Payment Report untuk memulai.")
    st.stop()

df, sheets, chosen_sheet = read_any_file(uploaded)
if df is None:
    st.stop()

if sheets:
    chosen_sheet = st.selectbox(
        "Pilih sheet: ",
        sheets,
        index=(sheets.index(chosen_sheet) if chosen_sheet in sheets else 0)
    )
    df, _, _ = read_any_file(uploaded, sheet=chosen_sheet)

st.write(":small_blue_diamond: Baris data:", len(df))

# -----------------------------
# Pemetaan kolom (B, H, K, AA, Q)
# -----------------------------
# B  -> Tanggal (ambil tanggal saja, abaikan jam)
# H  -> kanal/payment code (cash, prepaid-xxx, skpt, finpay, redeem, ...)
# K  -> amount/nominal
# AA -> deskripsi (untuk deteksi 'ESP' untuk ESPAY vs FINNET)
# Q  -> Nama Pelabuhan
b_col, b_found = resolve_column(df, 'B', 1)
h_col, h_found = resolve_column(df, 'H', 7)
k_col, k_found = resolve_column(df, 'K', 10)
aa_col, aa_found = resolve_column(df, 'AA', 26)
q_col, q_found = resolve_column(df, 'Q', 16)

# Buat kolom Tanggal dari B (tanggal saja)
if b_col is not None and b_col in df.columns:
    tanggal_parsed = pd.to_datetime(df[b_col], errors='coerce')
    df['Tanggal'] = tanggal_parsed.dt.date  # hanya tanggalnya
else:
    df['Tanggal'] = pd.NaT

with st.expander("Lihat pemetaan kolom (opsional)"):
    st.write({
        "B (Tanggal)": {"mapped_to": b_col, "how": b_found},
        "H (Kanal)": {"mapped_to": h_col, "how": h_found},
        "K (Amount)": {"mapped_to": k_col, "how": k_found},
        "AA (Deskripsi)": {"mapped_to": aa_col, "how": aa_found},
        "Q (Pelabuhan)": {"mapped_to": q_col, "how": q_found},
    })
    if h_col is None:
        st.error("Kolom H (kanal) tidak ditemukan. Pastikan kolom ini ada.")
    if k_col is None:
        st.warning("Kolom K (amount) tidak ditemukan. Akan fallback ke hitung baris, bukan jumlah nominal.")
    if b_col is None:
        st.warning("Kolom B (tanggal) tidak ditemukan. Kolom 'Tanggal' akan kosong.")
    if q_col is None:
        st.warning("Kolom Q (Nama Pelabuhan) tidak ditemukan. Tabel per pelabuhan tidak dapat dibuat.")

if h_col is None:
    st.stop()

# -----------------------------
# Parameter Bulan/Tahun
# -----------------------------
st.subheader("Parameter Periode (Bulan/Tahun)")
if df['Tanggal'].notna().any():
    dmin = pd.to_datetime(df['Tanggal']).min()
    dmax = pd.to_datetime(df['Tanggal']).max()
    years = list(range(int(dmin.year), int(dmax.year) + 1))
    default_year = int(dmax.year)
    default_month = int(dmax.month)
else:
    # fallback bila tidak ada tanggal valid
    today = date.today()
    years = [today.year]
    default_year = today.year
    default_month = today.month

# Nama bulan Indonesia (opsional)
bulan_id = ["Januari","Februari","Maret","April","Mei","Juni","Juli","Agustus","September","Oktober","November","Desember"]

col1, col2 = st.columns(2)
with col1:
    year_sel = st.selectbox("Tahun", years, index=years.index(default_year))
with col2:
    month_sel_name = st.selectbox("Bulan", bulan_id, index=default_month-1)
month_sel = bulan_id.index(month_sel_name) + 1

# Filter df ke bulan & tahun terpilih
if df['Tanggal'].notna().any():
    df_valid = df[df['Tanggal'].notna()].copy()
    df_valid['Tanggal_ts'] = pd.to_datetime(df_valid['Tanggal'])
    df_month = df_valid[
        (df_valid['Tanggal_ts'].dt.year == year_sel) &
        (df_valid['Tanggal_ts'].dt.month == month_sel)
    ].copy()
else:
    df_month = df.iloc[0:0].copy()  # kosong

# -----------------------------
# Tabel Harian Otomatis (1–28/29/30/31)
# -----------------------------
st.subheader(f"Tabel Harian - {bulan_id[month_sel-1]} {year_sel} (Nominal dari Kolom K)")
daily_table = build_daily_table(df_month, h_col=h_col, aa_col=aa_col, amount_col=k_col, date_col='Tanggal')
st.dataframe(daily_table, use_container_width=True)

csv_daily = daily_table.to_csv(index=False).encode("utf-8")
st.download_button(
    "Unduh Tabel Harian (CSV)",
    csv_daily,
    file_name=f"rekon_harian_{year_sel}_{month_sel:02d}.csv",
    mime="text/csv"
)

# -----------------------------
# Tabel total bulan (semua pelabuhan)
# -----------------------------
st.subheader("Rekap Bulanan (Semua Pelabuhan)")
main_metrics_month = build_metrics(df_month, h_col=h_col, aa_col=aa_col, amount_col=k_col)
st.dataframe(main_metrics_month, use_container_width=True)
main_month_csv = main_metrics_month.to_csv(index=False).encode('utf-8')
st.download_button("Unduh Rekap Bulanan (CSV)", main_month_csv, file_name=f"rekap_bulanan_{year_sel}_{month_sel:02d}.csv", mime="text/csv")

# -----------------------------
# Per Pelabuhan (Merak, Bakauheni, Ketapang) - terfilter bulan
# -----------------------------
if q_col is not None and not df_month.empty:
    st.subheader("Tabel Per Pelabuhan (Bulan Terpilih)")
    tabs = st.tabs(["Merak", "Bakauheni", "Ketapang"])
    for tab, port in zip(tabs, ["merak", "bakauheni", "ketapang"]):
        with tab:
            port_df = filter_port(df_month, q_col, port)
            met = build_metrics(port_df, h_col=h_col, aa_col=aa_col, amount_col=k_col)
            st.caption(f"Total baris {port.title()} (bulan ini): {len(port_df)}")
            st.dataframe(met, use_container_width=True)
            csv_bytes = met.to_csv(index=False).encode('utf-8')
            st.download_button(
                f"Unduh Rekon {port.title()} (CSV)",
                csv_bytes,
                file_name=f"rekon_ferizy_{port}_{year_sel}_{month_sel:02d}.csv",
                mime="text/csv"
            )

# -----------------------------
# Preview detail baris per channel (bulan terpilih)
# -----------------------------
st.subheader("Preview Baris Detail per Channel (bulan terpilih)")
channel_choice = st.selectbox(
    "Pilih channel untuk preview:",
    ["Cash", "Prepaid - BRI", "Prepaid - Mandiri", "Prepaid - BNI", "Prepaid - BCA",
     "SKPT", "IFCS", "Redeem", "ESPAY", "FINNET"]
)

if not df_month.empty:
    h_vals = normalize_str_series(df_month[h_col])
    aa_vals = normalize_str_series(df_month[aa_col]) if (aa_col is not None and aa_col in df_month.columns) else pd.Series([None] * len(df_month))

    mask_map = {
        "Cash": h_vals.eq('cash'),
        "Prepaid - BRI": h_vals.eq('prepaid-bri'),
        "Prepaid - Mandiri": h_vals.eq('prepaid-mandiri'),
        "Prepaid - BNI": h_vals.eq('prepaid-bni'),
        "Prepaid - BCA": h_vals.eq('prepaid-bca'),
        "SKPT": h_vals.eq('skpt'),
        "IFCS": h_vals.eq('cash'),
        "Redeem": h_vals.eq('redeem'),
        "ESPAY": (h_vals.eq('finpay') & aa_vals.str.contains('esp', na=False)) if (aa_col is not None and aa_col in df_month.columns) else (h_vals == '__no_matches__'),
        "FINNET": (h_vals.eq('finpay') & ~aa_vals.str.contains('esp', na=False)) if (aa_col is not None and aa_col in df_month.columns) else (h_vals == '__no_matches__'),
    }

    preview_cols = ["Tanggal"] + [c for c in [h_col, k_col, aa_col, q_col] if c in df_month.columns]
    preview = df_month[mask_map[channel_choice]].copy()
    if not preview.empty:
        if "Tanggal" in preview.columns:
            preview = preview.sort_values(by="Tanggal", ascending=False)
        preview = preview[[c for c in preview_cols if c in preview.columns] + [c for c in preview.columns if c not in preview_cols]]

    st.write(f"Menampilkan {len(preview)} baris (maks 200).")
    st.dataframe(preview.head(200), use_container_width=True)
else:
    st.info("Tidak ada data pada bulan yang dipilih.")

st.success("Selesai. Tabel harian otomatis mengikuti jumlah hari di bulan terpilih, dan seluruh rekap menggunakan nominal dari kolom K.")

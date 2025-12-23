# path: streamlit_app.py
# (potongan lengkap app; bagian lain tidak berubah dari versi Anda sebelumnya kecuali fungsi summary FINNET dan utilnya)

import io, re, csv, zipfile
from datetime import date
from calendar import monthrange
from collections import defaultdict, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple, Dict, Iterable, Union

import pandas as pd
import streamlit as st
from openpyxl import load_workbook

st.set_page_config(page_title="Rekonsiliasi Payment Report", layout="wide")
st.set_option("client.showErrorDetails", True)

# ========= Konstanta umum =========
COL_H, COL_B, COL_AA = "TIPE PEMBAYARAN", "TANGGAL PEMBAYARAN", "REF NO"
COL_K, COL_X, COL_ASAL = "TOTAL TARIF TANPA BIAYA ADMIN (Rp.)", "SOF ID", "ASAL"
REQUIRED_COLS = [COL_H, COL_B, COL_AA, COL_K, COL_X, COL_ASAL]
CAT_COLS = ["Cash","Prepaid BRI","Prepaid BNI","Prepaid Mandiri","Prepaid BCA","SKPT","IFCS","Reedem","ESPAY","Finnet"]
NON_COMPONENTS = ["Cash","Prepaid BRI","Prepaid BNI","Prepaid Mandiri","Prepaid BCA","SKPT","IFCS","Reedem"]
DEFAULT_CSV_CHUNK_ROWS = 200_000
XLSX_BATCH_ROWS = 50_000
NONBCA_CREDIT_COL_INDEX = 9

_HAS_PYARROW = False
try:
    import pyarrow  # noqa: F401
    _HAS_PYARROW = True
except Exception:
    pass

# ========= Utils =========
def ss_get_set(key: str, default):
    if key not in st.session_state: st.session_state[key] = default
    return st.session_state[key]

def _style_table(df_display: pd.DataFrame, highlight: bool):
    numeric_cols = df_display.select_dtypes(include="number").columns.tolist()
    styler = df_display.style.format("{:,.0f}", subset=numeric_cols)
    if highlight:
        for col in ["Selisih Tiket Detail vs Settlement Report","Selisih Dana Masuk vs Settlement Report","Selisih"]:
            if col in df_display.columns:
                styler = styler.apply(
                    lambda s: ["background-color:#fdecea; color:#b71c1c; font-weight:600;"
                               if (pd.notna(v) and float(v) != 0) else "" for v in s],
                    subset=[col],
                )
    return styler

def _add_subtotal_row(df_display: pd.DataFrame, label: str = "Subtotal", date_col: str = "Tanggal") -> pd.DataFrame:
    numeric_cols = df_display.select_dtypes(include="number").columns.tolist()
    totals = df_display[numeric_cols].sum()
    subtotal = {c: (totals[c] if c in totals else None) for c in df_display.columns}
    subtotal[date_col] = label
    return pd.concat([df_display, pd.DataFrame([subtotal])], ignore_index=True)

def _norm_colname(name: str) -> str:
    s = str(name)
    return "".join(ch.lower() for ch in s if ch.isalnum())

def _canonical_port_name(name: Optional[str]) -> str:
    if name is None: return "Tidak diketahui"
    s, up = str(name).strip(), str(name).upper()
    if "BAKAUHENI" in up: return "ASDP Bakauheni"
    if "GILIMANUK" in up: return "ASDP Gilimanuk"
    if "KETAPANG" in up:  return "ASDP Ketapang"
    if "MERAK" in up:     return "ASDP Merak"
    return s

def _parse_amount_credit_series(s: pd.Series) -> pd.Series:
    x = s.astype(str)
    neg = (
        x.str.contains(r"\(", regex=True, na=False)
        | x.str.contains(r"\bDR\b", flags=re.I, regex=True, na=False)
        | x.str.contains("\u2212", regex=False, na=False)
        | x.str.strip().str.startswith("-")
    )
    x = x.str.replace(r"[()]", "", regex=True)
    x = x.str.replace("\u2212", "-", regex=False)
    x = x.str.replace(r"\b(CR|DR)\b", "", flags=re.I, regex=True)
    x = x.str.replace(r"[^0-9,.\-]", "", regex=True)

    def _to_float(val: str) -> float:
        if not val: return 0.0
        if "," in val and "." in val:
            if val.rfind(",") > val.rfind("."):
                val = val.replace(".", "").replace(",", ".")
            else:
                val = val.replace(",", "")
        else:
            if "," in val:
                a, b = val.rsplit(",", 1)
                val = a.replace(",", "") + ("." + b if len(b) in (2, 3) else "")
            elif "." in val:
                a, b = val.rsplit(".", 1)
                val = a.replace(".", "") + ("." + b if len(b) in (2, 3) else "")
        try: return float(val)
        except Exception: return 0.0

    vals = x.apply(_to_float)
    vals = vals.where(~neg, -vals.abs())
    return vals.round(0).astype("Int64")

def _parse_money_series(s: pd.Series) -> pd.Series:
    try:
        if pd.api.types.is_numeric_dtype(s):
            return pd.to_numeric(s, errors="coerce").fillna(0.0).round(0).astype("Int64").astype("float64")
    except Exception:
        pass
    return _parse_amount_credit_series(s).astype("float64")

def _mask_remark_contains(remark: pd.Series, keywords: List[str]) -> pd.Series:
    if not keywords: return pd.Series([True] * len(remark), index=remark.index)
    norm = remark.astype(str).str.upper()
    mask = pd.Series(False, index=remark.index)
    for k in keywords: mask |= norm.str.contains(str(k).upper(), na=False)
    return mask

def _sniff_delimiter(sample: bytes) -> str:
    try: return csv.Sniffer().sniff(sample.decode("utf-8","ignore")).delimiter
    except Exception: return ","

def _to_date_minus1(v) -> pd.Series:
    t = pd.to_datetime(v, errors="coerce", dayfirst=True)
    return t - pd.Timedelta(days=1)

def _port_from_filename(fname: str) -> str:
    up = str(fname).upper()
    if "MERAK" in up: return "ASDP Merak"
    if "BAKAUHENI" in up: return "ASDP Bakauheni"
    if "GILIMANUK" in up: return "ASDP Gilimanuk"
    if "KETAPANG" in up: return "ASDP Ketapang"
    return "ASDP Lainnya"

def _port_from_bca_filename(fname: str) -> str:
    up = str(fname).upper()
    if "MERAK" in up: return "ASDP Merak"
    if ("BEKAUHENI" in up) or ("BAKAUHENI" in up): return "ASDP Bakauheni"
    if "KETAPANG" in up: return "ASDP Bakauheni"
    if "GILIMANUK" in up: return "ASDP Gilimanuk"
    return "ASDP Lainnya"

def _period_label(y: int, m: int) -> str:
    return pd.Timestamp(y, m, 1).strftime("%b-%y")

# ========= CSV chunk iterator, Payment loaders, Settlement readers, RK parsers, Summary ESPAY dll =========
# (==> Bagian-bagian ini sama persis seperti versi Anda sebelumnya yang sudah jalan.
#      Scroll ke bawah untuk bagian yang diubah/tambahan: builder Summary FINNET dan pemanggilannya.)

# ……………………… (seluruh fungsi Anda sebelumnya tetap) ………………………

# ========= ——— FUNGSI TAMBAHAN KHUSUS SUMMARY FINNET ——— =========

def _amount_exc_finnet_telkom(df_raw: pd.DataFrame, year: int, month: int) -> Dict[str, float]:
    """Sum Merchant Amount per pelabuhan dari uploader FINNET by Telkom (exc fee)."""
    if df_raw is None or df_raw.empty: return {}
    needed = ["Payment Date Time", "Merchant Amount", "Merchant Name"]
    if any(c not in df_raw.columns for c in needed): return {}
    dt = pd.to_datetime(df_raw["Payment Date Time"].astype(str).str.slice(0, 10), errors="coerce")
    m = (dt.dt.year == year) & (dt.dt.month == month)
    sub = df_raw.loc[m].copy()
    if sub.empty: return {}
    def map_port(name: str) -> str:
        n = str(name).upper()
        if "BAKAUHENI" in n: return "ASDP Bakauheni"
        if "GILIMANUK" in n: return "ASDP Gilimanuk"
        if "KETAPANG" in n:  return "ASDP Ketapang"
        if "MERAK" in n:     return "ASDP Merak"
        return "ASDP Lainnya"
    sub["Pelabuhan"] = sub["Merchant Name"].map(map_port)
    amt = pd.to_numeric(sub["Merchant Amount"].astype(str).str.replace(r"[^\d\-]", "", regex=True), errors="coerce").fillna(0.0)
    sub["amt"] = amt
    g = sub.groupby("Pelabuhan")["amt"].sum()
    return {k: float(v) for k, v in g.items()}

def _totals_from_rekon(df_rekon: pd.DataFrame, col: str) -> Dict[str, float]:
    if df_rekon is None or df_rekon.empty or col not in df_rekon.columns: return {}
    g = df_rekon.groupby("Pelabuhan")[col].sum()
    return {k: float(v) for k, v in g.items()}

def _build_summary_table_finnet(
    df_rekon_finnet: pd.DataFrame,
    df_finnet_telkom_raw: pd.DataFrame,
    year: int,
    month: int,
    espay_merged: Optional[Dict[str, Tuple[int, float]]] = None
) -> pd.DataFrame:
    """Summary FINNET:
       - Header kiri: 'Data Settlement FINNET'
       - Jumlah Transaksi & Nominal (exc fee): dari uploader FINNET Telkom (raw)
       - Nominal (inc fee): dari Total Settlement Report pada tabel rekon FINNET
       - Hanya baris: Bakauheni, Gilimanuk+Ketapang, Merak + baris Total
    """
    # sumber data kiri
    cnt_df = _count_tx_finnet(df_finnet_telkom_raw, year, month)
    exc_map = _amount_exc_finnet_telkom(df_finnet_telkom_raw, year, month)
    inc_map = _totals_from_rekon(df_rekon_finnet, "Total Settlement Report")

    # fungsi ambil nilai aman
    def gc(port: str) -> int:
        if cnt_df is None or cnt_df.empty: return 0
        s = cnt_df.loc[cnt_df["Pelabuhan"] == port, "Jumlah Transaksi"]
        return int(s.iloc[0]) if not s.empty else 0
    def ge(port: str) -> float: return float(exc_map.get(port, 0.0))
    def gi(port: str) -> float: return float(inc_map.get(port, 0.0))

    # espay merged (kanan)
    em = espay_merged or {}
    def emc(port: str) -> int: return int(em.get(port, (0, 0.0))[0])
    def ema(port: str) -> float: return float(em.get(port, (0, 0.0))[1])

    periode = _period_label(year, month)
    rows = []
    # Bakauheni
    rows.append({
        "Periode": periode, "Cabang": "ASDP Bakauheni",
        "FINNET - Jumlah Transaksi": gc("ASDP Bakauheni"),
        "FINNET - Nominal Transaksi (inc fee)": gi("ASDP Bakauheni"),
        "FINNET - Nominal Transaksi (exc fee)": ge("ASDP Bakauheni"),
        "ESPAY Merged - Jumlah Transaksi": emc("ASDP Bakauheni"),
        "ESPAY Merged - Nominal Transaksi (exc fee)": ema("ASDP Bakauheni"),
    })
    # Gilimanuk + Ketapang
    rows.append({
        "Periode": periode, "Cabang": "ASDP Gilimanuk + Ketapang",
        "FINNET - Jumlah Transaksi": gc("ASDP Gilimanuk") + gc("ASDP Ketapang"),
        "FINNET - Nominal Transaksi (inc fee)": gi("ASDP Gilimanuk") + gi("ASDP Ketapang"),
        "FINNET - Nominal Transaksi (exc fee)": ge("ASDP Gilimanuk") + ge("ASDP Ketapang"),
        "ESPAY Merged - Jumlah Transaksi": emc("ASDP Gilimanuk") + emc("ASDP Ketapang"),
        "ESPAY Merged - Nominal Transaksi (exc fee)": ema("ASDP Gilimanuk") + ema("ASDP Ketapang"),
    })
    # Merak
    rows.append({
        "Periode": periode, "Cabang": "ASDP Merak",
        "FINNET - Jumlah Transaksi": gc("ASDP Merak"),
        "FINNET - Nominal Transaksi (inc fee)": gi("ASDP Merak"),
        "FINNET - Nominal Transaksi (exc fee)": ge("ASDP Merak"),
        "ESPAY Merged - Jumlah Transaksi": emc("ASDP Merak"),
        "ESPAY Merged - Nominal Transaksi (exc fee)": ema("ASDP Merak"),
    })
    out = pd.DataFrame(rows)

    total_row = {
        "Periode": periode, "Cabang": "Total",
        "FINNET - Jumlah Transaksi": out["FINNET - Jumlah Transaksi"].sum(),
        "FINNET - Nominal Transaksi (inc fee)": out["FINNET - Nominal Transaksi (inc fee)"].sum(),
        "FINNET - Nominal Transaksi (exc fee)": out["FINNET - Nominal Transaksi (exc fee)"].sum(),
        "ESPAY Merged - Jumlah Transaksi": out["ESPAY Merged - Jumlah Transaksi"].sum(),
        "ESPAY Merged - Nominal Transaksi (exc fee)": out["ESPAY Merged - Nominal Transaksi (exc fee)"].sum(),
    }
    out = pd.concat([out, pd.DataFrame([total_row])], ignore_index=True)

    # set kolom dengan MultiIndex sesuai permintaan
    out = out[[
        "Periode","Cabang",
        "FINNET - Jumlah Transaksi",
        "FINNET - Nominal Transaksi (inc fee)",
        "FINNET - Nominal Transaksi (exc fee)",
        "ESPAY Merged - Jumlah Transaksi",
        "ESPAY Merged - Nominal Transaksi (exc fee)",
    ]]
    out.columns = pd.MultiIndex.from_tuples([
        ("","Periode"),("","Cabang"),
        ("Data Settlement FINNET","Jumlah Transaksi"),
        ("Data Settlement FINNET","Nominal Transaksi (inc fee)"),
        ("Data Settlement FINNET","Nominal Transaksi (exc fee)"),
        ("Data Settlement ESPAY (merged)","Jumlah Transaksi"),
        ("Data Settlement ESPAY (merged)","Nominal Transaksi (exc fee)"),
    ])
    return out

# ========= Render helper (tambahkan formatter untuk FINNET) =========
def _render_summary(df_sum: pd.DataFrame):
    if df_sum is None or df_sum.empty:
        st.info("Summary kosong."); return
    fmt = {}
    for key in [
        ("Data Settlement FINNET","Jumlah Transaksi"),
        ("Data Settlement FINNET","Nominal Transaksi (inc fee)"),
        ("Data Settlement FINNET","Nominal Transaksi (exc fee)"),
        ("Menu Payment Ferizy","Jumlah Transaksi"),
        ("Menu Payment Ferizy","Nominal Transaksi (inc fee)"),
        ("Menu Payment Ferizy","Nominal Transaksi (exc fee)"),
        ("Data Settlement ESPAY (merged)","Jumlah Transaksi"),
        ("Data Settlement ESPAY (merged)","Nominal Transaksi (exc fee)"),
    ]:
        if isinstance(df_sum.columns, pd.MultiIndex) and key in df_sum.columns:
            fmt[key] = "{:,.0f}"
    try:
        st.dataframe(df_sum.style.format(fmt), use_container_width=True)
    except Exception:
        st.dataframe(df_sum, use_container_width=True)

# ========= MAIN (hanya bagian Summary FINNET yang berubah) =========
def main() -> None:
    # ……… seluruh UI + proses step 1–4 sama seperti versi Anda sebelumnya ………
    # (pastikan variabel berikut tersedia saat masuk ke bagian summary):
    # - df_rekon_finnet
    # - results["finnet_telkom_raw"]
    # - espay_merged_map

    # ------- potongan akhir main (Summary & Download) -------
    # … (kode Anda sebelumnya membuat espay_merged_map) …

    st.divider(); st.subheader("TABEL SUMMARY REKONSILIASI")

    espay_merged_map = _espay_merged_stats(
        st.session_state.get("results", {}).get("espay_raw", pd.DataFrame()),
        st.session_state.get("results", {}).get("finnet_espay_raw", pd.DataFrame()),
        st.session_state.get("year", date.today().year) if "year" in st.session_state else date.today().year,
        st.session_state.get("month", date.today().month) if "month" in st.session_state else date.today().month,
    )

    with st.expander("Summary • FINNET", expanded=True):
        # >> DI SINI KITA PAKAI BUILDER BARU <<
        results = st.session_state.get("results", {})
        df_rekon_finnet = results.get("finnet_telkom", pd.DataFrame())
        df_finnet_telkom_raw = results.get("finnet_telkom_raw", pd.DataFrame())
        year = st.session_state.get("year", date.today().year) if "year" in st.session_state else date.today().year
        month = st.session_state.get("month", date.today().month) if "month" in st.session_state else date.today().month
        sum_finnet = _build_summary_table_finnet(
            df_rekon_finnet, df_finnet_telkom_raw, year, month, espay_merged=espay_merged_map
        )
        _render_summary(sum_finnet)

    # Summary ESPAY tetap pakai builder lama Anda (jika ada)
    # ……………………… lanjutkan kode Anda seperti sebelumnya ……………………

# ====== jalankan ======
if __name__ == "__main__":
    try:
        main()
    except BaseException as e:
        st.error("Aplikasi error saat render awal.")
        st.exception(e)

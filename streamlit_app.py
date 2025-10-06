# app.py
# -*- coding: utf-8 -*-
"""
Rekonsiliasi cepat: Tiket Detail vs Settlement Dana
Percepatan:
- Cache baca file (st.cache_data) berbasis hash
- Baca paralel (ThreadPoolExecutor) untuk banyak file/ZIP
- CSV cepat: sniff delimiter -> engine='c'
- Parser tanggal & Created->Action vektor (tanpa apply)
- Dukungan ZIP (.csv/.xls/.xlsx)
"""

from __future__ import annotations

import io
import re
import csv
import zipfile
import hashlib
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Tuple, Iterable

import numpy as np
import pandas as pd
import streamlit as st
from dateutil import parser as dtparser

# ---------------------- Fast helpers ----------------------

def _hash_file(f) -> str:
    f.seek(0)
    data = f.read()
    f.seek(0)
    return hashlib.md5(data).hexdigest()

def _parse_money(val) -> float:
    if val is None or (isinstance(val, float) and np.isnan(val)): return 0.0
    if isinstance(val, (int, float, np.number)): return float(val)
    s = str(val).strip()
    if not s: return 0.0
    neg = False
    if s.startswith("(") and s.endswith(")"): neg, s = True, s[1:-1].strip()
    if s.endswith("-"): neg, s = True, s[:-1].strip()
    s = re.sub(r"(idr|rp|cr|dr)", "", s, flags=re.IGNORECASE)
    s = re.sub(r"[^0-9\.,\-]", "", s).strip()
    if s.startswith("-"): neg, s = True, s[1:].strip()
    d, c = s.rfind("."), s.rfind(",")
    if d == -1 and c == -1: num_s = s
    elif d > c:              num_s = s.replace(",", "")
    else:                    num_s = s.replace(".", "").replace(",", ".")
    try:
        num = float(num_s)
    except Exception:
        num_s = s.replace(".", "").replace(",", "")
        num = float(num_s) if num_s else 0.0
    return -num if neg else num

def to_num_series(s: pd.Series) -> pd.Series:
    # cukup robust + cepat (masih apply, tapi setelah cleaning vector)
    if s.dtype != object:
        return s.astype(float, errors='ignore')
    return s.apply(_parse_money).astype(float)

def fast_to_date_series(sr: pd.Series) -> pd.Series:
    """Parse vektor: coba pandas langsung; jika NaT & numerik -> serial Excel."""
    # langkah 1: pandas langsung
    s = pd.to_datetime(sr, errors="coerce", dayfirst=True)
    # langkah 2: serial Excel untuk numerik yang masih NaT
    if isinstance(sr, pd.Series):
        nums = pd.to_numeric(sr, errors="coerce")
        mask = s.isna() & nums.notna()
        if mask.any():
            s.loc[mask] = pd.to_datetime(nums[mask], unit="D", origin="1899-12-30", errors="coerce")
    return s.dt.normalize()

def derive_action_fixed_fast(created_sr: pd.Series) -> pd.Series:
    """Ambil 10 char pertama 'YYYY-MM-DD'. Fallback: fast_to_date_series."""
    as_str = created_sr.astype(str, copy=False).str.slice(0, 10)
    mask_iso = as_str.str.match(r"\d{4}-\d{2}-\d{2}")
    out = pd.to_datetime(as_str.where(mask_iso), errors="coerce")
    # fallback untuk baris yang gagal
    miss = out.isna()
    if miss.any():
        out.loc[miss] = fast_to_date_series(created_sr[miss])
    return out.dt.normalize()

def _norm_label(s: str) -> str:
    if s is None or (isinstance(s, float) and np.isnan(s)): return ""
    return re.sub(r"\s+", " ", str(s).strip().lower())

def _find_col(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    if df.empty: return None
    cols = [c for c in df.columns if isinstance(c, str)]
    m = {c.lower().strip(): c for c in cols}
    for n in names:
        k = n.lower().strip()
        if k in m: return m[k]
    for n in names:
        key = n.lower().strip()
        for c in cols:
            if key in c.lower(): return c
    return None

def idr_fmt(n: float) -> str:
    if pd.isna(n): return "-"
    s = f"{abs(int(round(n))):,}".replace(",", ".")
    return f"({s})" if n < 0 else s

def month_selector() -> Tuple[int, int]:
    from datetime import date
    today = date.today()
    years = list(range(today.year - 5, today.year + 2))
    months = [("01","Januari"),("02","Februari"),("03","Maret"),("04","April"),
              ("05","Mei"),("06","Juni"),("07","Juli"),("08","Agustus"),
              ("09","September"),("10","Oktober"),("11","November"),("12","Desember")]
    c1, c2 = st.columns(2)
    with c1: year = st.selectbox("Tahun", years, index=years.index(today.year))
    with c2: sel = st.selectbox("Bulan", months, index=int(today.strftime("%m"))-1, format_func=lambda x: x[1]); month = int(sel[0])
    return year, month

# ---------------------- Readers (cached + parallel) ----------------------

def _guess_header_row(df_no_header: pd.DataFrame, targets: Iterable[str]) -> int:
    scan = min(20, len(df_no_header))
    best_row, best_score = 0, -1
    for i in range(scan):
        row = df_no_header.iloc[i].astype(str).str.lower().str.strip().fillna("")
        text = " ".join(row.tolist())
        score = sum(1 for t in targets if t in text)
        if score > best_score:
            best_row, best_score = i, score
            if score >= 3: break
    return best_row

def _sniff_delimiter(sample: bytes) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample.decode("utf-8", errors="ignore"))
        return dialect.delimiter
    except Exception:
        return ','

@st.cache_data(show_spinner=False)
def _read_excel_cached(content: bytes, ext: str, header=None) -> pd.DataFrame:
    buf = io.BytesIO(content)
    engine = "openpyxl" if ext == ".xlsx" else "xlrd"
    return pd.read_excel(buf, engine=engine, header=header, dtype=str)

@st.cache_data(show_spinner=False)
def _read_csv_cached(content: bytes, delimiter: str | None) -> pd.DataFrame:
    buf = io.BytesIO(content)
    if delimiter is None:
        sample = content[:65536]
        delim = _sniff_delimiter(sample)
    else:
        delim = delimiter
    try:
        return pd.read_csv(buf, sep=delim, engine="c", dtype=str, na_filter=False)
    except Exception:
        buf.seek(0)
        return pd.read_csv(buf, sep=None, engine="python", dtype=str, na_filter=False)

@st.cache_data(show_spinner=False)
def _extract_zip_cached(content: bytes) -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        for info in zf.infolist():
            if info.is_dir(): continue
            nm = info.filename
            if nm.lower().endswith((".csv", ".xls", ".xlsx")):
                with zf.open(info) as f:
                    out.append((nm, f.read()))
    return out

def _read_tiket_single(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name
    low = name.lower()
    uploaded_file.seek(0); content = uploaded_file.read(); uploaded_file.seek(0)
    frames = []
    if low.endswith(".zip"):
        for nm, data in _extract_zip_cached(content):
            l = nm.lower()
            if l.endswith(".csv"):
                df = _read_csv_cached(data, delimiter=None)
            else:
                raw = _read_excel_cached(data, ext=(".xlsx" if l.endswith(".xlsx") else ".xls"), header=None)
                hdr = _guess_header_row(raw, ["created","action","tarif","st bayar","status","bank","channel"])
                df = _read_excel_cached(data, ext=(".xlsx" if l.endswith(".xlsx") else ".xls"), header=hdr)
            df["__source__"] = f"{name}/{nm}"
            frames.append(df)
    else:
        if low.endswith(".csv"):
            df = _read_csv_cached(content, delimiter=None)
            df["__source__"] = name
            frames.append(df)
        else:
            raw = _read_excel_cached(content, ext=(".xlsx" if low.endswith(".xlsx") else ".xls"), header=None)
            hdr = _guess_header_row(raw, ["created","action","tarif","st bayar","status","bank","channel"])
            df = _read_excel_cached(content, ext=(".xlsx" if low.endswith(".xlsx") else ".xls"), header=hdr)
            df["__source__"] = name
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def _read_settle_single(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name
    low = name.lower()
    uploaded_file.seek(0); content = uploaded_file.read(); uploaded_file.seek(0)
    frames = []
    if low.endswith(".zip"):
        for nm, data in _extract_zip_cached(content):
            l = nm.lower()
            if l.endswith(".csv"):
                df = _read_csv_cached(data, delimiter=None)
            else:
                df = _read_excel_cached(data, ext=(".xlsx" if l.endswith(".xlsx") else ".xls"), header=0)
            df["__source__"] = f"{name}/{nm}"
            frames.append(df)
    else:
        if low.endswith(".csv"):
            df = _read_csv_cached(content, delimiter=None)
        else:
            df = _read_excel_cached(content, ext=(".xlsx" if low.endswith(".xlsx") else ".xls"), header=0)
        df["__source__"] = name
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def _concat_parallel(files, reader, max_workers: int = 4) -> pd.DataFrame:
    if not files: return pd.DataFrame()
    frames = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(reader, f): f.name for f in files}
        for fut in as_completed(futs):
            df = fut.result()
            if not df.empty: frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ---------------------- App ----------------------

st.set_page_config(page_title="Rekonsiliasi Tiket vs Settlement (Fast)", layout="wide")
st.title("Rekonsiliasi: Tiket Detail vs Settlement Dana")

with st.sidebar:
    st.header("1) Upload Sumber (multi-file)")
    tiket_files = st.file_uploader("Tiket Detail (.xls/.xlsx/.zip)", type=["xls", "xlsx", "zip"], accept_multiple_files=True)
    settle_files = st.file_uploader("Settlement Dana (.csv/.xls/.xlsx/.zip)", type=["csv", "xls", "xlsx", "zip"], accept_multiple_files=True)

    st.header("2) Parameter Bulan & Tahun (WAJIB)")
    y, m = month_selector()
    month_start = pd.Timestamp(y, m, 1)
    month_end   = pd.Timestamp(y, m, calendar.monthrange(y, m)[1])
    st.caption(f"Periode: {month_start.date()} s/d {month_end.date()}")

    go = st.button("Proses", type="primary", use_container_width=True)

# baca (cached + paralel)
tiket_df  = _concat_parallel(tiket_files, _read_tiket_single)
settle_df = _concat_parallel(settle_files, _read_settle_single)

if go:
    # Mapping fleksibel
    created_candidates = ["created","created date","create date","created (wib)","created time","action","action date","tanggal","tgl"]
    amount_candidates  = ["tarif","fare","amount","nominal","total","harga"]
    status_candidates  = ["st bayar","status bayar","status"]
    bank_candidates    = ["bank","payment channel","channel","payment method","bank/ewallet"]

    t_created = _find_col(tiket_df, created_candidates)
    t_amt     = _find_col(tiket_df, amount_candidates)
    t_stat    = _find_col(tiket_df, status_candidates)
    t_bank    = _find_col(tiket_df, bank_candidates)

    s_txn_date    = _find_col(settle_df, ["Transaction Date","Trans Date","Tanggal Transaksi"])
    s_settle_date = _find_col(settle_df, ["Settlement Date","SettlementDate","Tanggal Settlement"])
    s_amt         = _find_col(settle_df, ["Settlement Amount","Amount Settlement","Nominal Settlement","Amount"])
    s_prod        = _find_col(settle_df, ["Product Name","Product","ProductName","Nama Produk"])

    missing = []
    if t_created is None: missing.append("Tiket Detail: Created/Action")
    if t_amt is None:     missing.append("Tiket Detail: tarif/amount")
    if t_stat is None:    missing.append("Tiket Detail: St Bayar/Status")
    if t_bank is None:    missing.append("Tiket Detail: Bank/Channel")

    if missing:
        with st.expander("⚙️ Map kolom Tiket secara manual"):
            cols = list(tiket_df.columns)
            def pick(keys: List[str], label: str, cur: Optional[str]):
                auto = _find_col(tiket_df, keys) if cur is None else cur
                return st.selectbox(label, ["-- pilih --"] + cols, index=(cols.index(auto)+1) if (auto in cols) else 0)
            t_created = pick(created_candidates, "Kolom Created/Action", t_created)
            t_amt     = pick(amount_candidates,  "Kolom Tarif/Amount",  t_amt)
            t_stat    = pick(status_candidates,  "Kolom Status Bayar",  t_stat)
            t_bank    = pick(bank_candidates,    "Kolom Bank/Channel",  t_bank)
            if t_created == "-- pilih --": t_created = None
            if t_amt     == "-- pilih --": t_amt     = None
            if t_stat    == "-- pilih --": t_stat    = None
            if t_bank    == "-- pilih --": t_bank    = None

    missing = []
    if t_created is None: missing.append("Tiket Detail: Created/Action")
    if t_amt is None:     missing.append("Tiket Detail: tarif/amount")
    if t_stat is None:    missing.append("Tiket Detail: St Bayar/Status")
    if t_bank is None:    missing.append("Tiket Detail: Bank/Channel")
    if missing:
        st.error("Kolom wajib tidak ditemukan → " + "; ".join(missing))
        st.stop()

    if s_txn_date is None or s_amt is None:
        st.error("Settlement wajib: 'Transaction Date' & 'Settlement Amount' tidak ditemukan.")
        st.stop()
    if s_settle_date is None or s_prod is None:
        st.warning("Kolom 'Settlement Date' / 'Product Name' tidak ditemukan. Kolom BCA/Non-BCA akan 0.")

    # ---------- Tiket (Action dari Created: fixed 10, vektor) ----------
    td = tiket_df[[t_created, t_amt, t_stat, t_bank]].copy()
    td["__action_date"] = derive_action_fixed_fast(td[t_created])
    td = td[td["__action_date"].notna()]
    td_stat_v = td[t_stat].astype(str).str.strip().str.lower()
    td_bank_v = td[t_bank].astype(str).str.strip().str.lower()
    td = td[td_stat_v.eq("paid") & td_bank_v.str.contains("espay")]
    td = td[(td["__action_date"] >= month_start) & (td["__action_date"] <= month_end)]
    td[t_amt] = to_num_series(td[t_amt])
    tiket_by_date = td.groupby(td["__action_date"], sort=False)[t_amt].sum()
    tiket_by_date.index = pd.to_datetime(tiket_by_date.index).date

    # ---------- Settlement total (Transaction Date) ----------
    sd_txn = settle_df[[s_txn_date, s_amt]].copy()
    sd_txn[s_txn_date] = fast_to_date_series(sd_txn[s_txn_date])
    sd_txn = sd_txn[sd_txn[s_txn_date].notna()]
    sd_txn = sd_txn[(sd_txn[s_txn_date] >= month_start) & (sd_txn[s_txn_date] <= month_end)]
    sd_txn[s_amt] = to_num_series(sd_txn[s_amt])
    settle_total = sd_txn.groupby(sd_txn[s_txn_date], sort=False)[s_amt].sum()
    settle_total.index = pd.to_datetime(settle_total.index).date

    # ---------- BCA / Non-BCA (Settlement Date + Product Name) ----------
    if (s_settle_date is not None) and (s_prod is not None):
        sd_settle = settle_df[[s_settle_date, s_prod, s_amt]].copy()
        sd_settle[s_settle_date] = fast_to_date_series(sd_settle[s_settle_date])
        sd_settle = sd_settle[sd_settle[s_settle_date].notna()]
        sd_settle = sd_settle[(sd_settle[s_settle_date] >= month_start) & (sd_settle[s_settle_date] <= month_end)]
        sd_settle[s_amt] = to_num_series(sd_settle[s_amt])
        prod_norm = sd_settle[s_prod].map(_norm_label)
        bca_mask = prod_norm.eq(_norm_label("BCA VA Online"))
        settle_bca    = sd_settle[bca_mask].groupby(sd_settle[bca_mask][s_settle_date], sort=False)[s_amt].sum() if bca_mask.any() else pd.Series(dtype=float)
        settle_nonbca = sd_settle[~bca_mask].groupby(sd_settle[~bca_mask][s_settle_date], sort=False)[s_amt].sum() if (~bca_mask).any() else pd.Series(dtype=float)
    else:
        settle_bca = pd.Series(dtype=float)
        settle_nonbca = pd.Series(dtype=float)

    # ---------- Reindex hasil ke 1..akhir bulan ----------
    idx = pd.Index(pd.date_range(month_start, month_end, freq="D").date, name="Tanggal")

    def reidx(s: pd.Series) -> pd.Series:
        if not isinstance(s, pd.Series): s = pd.Series(dtype=float)
        if len(getattr(s, "index", [])): s.index = pd.to_datetime(s.index).date
        return s.reindex(idx, fill_value=0.0)

    final = pd.DataFrame(index=idx)
    final["Tiket Detail ESPAY"]      = reidx(tiket_by_date).values
    final["Settlement Dana ESPAY"]   = reidx(settle_total).values
    final["Selisih"]                 = final["Tiket Detail ESPAY"] - final["Settlement Dana ESPAY"]
    final["Settlement Dana BCA"]     = reidx(settle_bca).values
    final["Settlement Dana Non BCA"] = reidx(settle_nonbca).values

    view = final.reset_index()
    view.insert(0, "No", range(1, len(view) + 1))
    total_row = pd.DataFrame([{
        "No": "",
        "Tanggal": "TOTAL",
        "Tiket Detail ESPAY": final["Tiket Detail ESPAY"].sum(),
        "Settlement Dana ESPAY": final["Settlement Dana ESPAY"].sum(),
        "Selisih": final["Selisih"].sum(),
        "Settlement Dana BCA": final["Settlement Dana BCA"].sum(),
        "Settlement Dana Non BCA": final["Settlement Dana Non BCA"].sum(),
    }])
    view_total = pd.concat([view, total_row], ignore_index=True)

    fmt = view_total.copy()
    for c in ["Tiket Detail ESPAY","Settlement Dana ESPAY","Selisih","Settlement Dana BCA","Settlement Dana Non BCA"]:
        fmt[c] = fmt[c].apply(idr_fmt)

    st.subheader("Hasil Rekonsiliasi per Tanggal (mengikuti bulan parameter)")
    st.dataframe(fmt, use_container_width=True, hide_index=True)

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        view_total.to_excel(xw, index=False, sheet_name="Rekonsiliasi")
        fmt.to_excel(xw, index=False, sheet_name="Rekonsiliasi_View")
    st.download_button("Unduh Excel", out.getvalue(),
        file_name=f"rekonsiliasi_{y}-{m:02d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True)

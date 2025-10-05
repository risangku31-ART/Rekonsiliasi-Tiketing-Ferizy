# streamlit_app.py
# -*- coding: utf-8 -*-
"""
Rekonsiliasi otomatis (Smart Header + ZIP + Periode):
- Tiket Detail (Excel/CSV/ZIP): ambil 'Created' (tgl+jam) → ambil tanggalnya (date-only),
  jumlahkan 'Tarif/Nominal/Amount' per tanggal.
- Report Settlement (CSV/ZIP): ambil 'Transaction Date' + 'Settlement Amount/Ammount',
  jumlahkan per tanggal.
- Periode rekonsiliasi dapat dipilih (tgl mulai–tgl akhir).
- Parser angka robust (ID/EN, (), "-", dan scientific notation 'E+NN').
"""

from __future__ import annotations
import io, re, zipfile, calendar
from typing import List, Optional, Tuple
import numpy as np
import pandas as pd
import streamlit as st
from dateutil import parser as dtparser
from datetime import date, datetime

# ========== util parsing ==========

def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    if df is None or df.empty: return None
    cols = [c for c in df.columns if isinstance(c, str)]
    norm = {c.lower().strip(): c for c in cols}
    for n in candidates:
        k = n.lower().strip()
        if k in norm: return norm[k]
    for n in candidates:
        key = n.lower().strip()
        for c in cols:
            if key in c.lower(): return c
    return None

def _to_datetime_series(sr: pd.Series) -> pd.Series:
    """Parse tanggal-jam: dukung string ID/EN & Excel serial."""
    if sr.empty:
        return pd.to_datetime(pd.Series([], dtype=str), errors="coerce")
    s = sr.astype(str).str.strip()
    # buang label zona & 'T'
    s = s.str.replace(r"(?i)\b(wib|wita|wit|utc\+?7|utc\+?8|utc\+?9)\b", "", regex=True).str.replace("T", " ", regex=False)
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True, infer_datetime_format=True)
    mask = dt.isna()
    if mask.any():
        dt_fallback = s[mask].apply(lambda x: pd.Timestamp(dtparser.parse(x, dayfirst=True, fuzzy=True)) if x else pd.NaT)
        dt = dt.where(~mask, dt_fallback)
    # Excel serial
    num = pd.to_numeric(sr, errors="coerce")
    mask_serial = num.between(1, 100000)
    if mask_serial.any():
        base = pd.Timestamp("1899-12-30")
        dt_serial = base + pd.to_timedelta(num[mask_serial], unit="D")
        dt = dt.where(~mask_serial, dt_serial)
    return dt

def _to_money(sr: pd.Series) -> pd.Series:
    """Parser uang robust: 1.095.568.800 / 1,095,568,800.00 / (123) / 123- / 1.23E+08"""
    def p(x) -> float:
        if x is None: return 0.0
        s_raw = str(x).strip()
        if s_raw == "" or s_raw.lower() in ("nan","none"): return 0.0

        # Scientific notation (keep 'eE+-')
        s_no_space = s_raw.replace("\xa0","").replace(" ", "")
        if re.search(r"[eE][\+\-]?\d+$", s_no_space):
            try:
                return float(s_no_space.replace(",", ""))
            except Exception:
                pass

        s = s_raw.lower()
        neg = False
        if s.startswith("(") and s.endswith(")"):
            neg, s = True, s[1:-1].strip()
        if s.endswith("-"):
            neg, s = True, s[:-1].strip()
        s = re.sub(r"(idr|rp|cr|dr)", "", s)
        s = s.replace("\xa0","").replace(" ", "")
        s = re.sub(r"[^0-9\.,\-]", "", s)
        if not s or s == "-": return 0.0

        d, c = s.rfind("."), s.rfind(",")
        try:
            if d == -1 and c == -1:
                num = float(s)
            elif d != -1 and c != -1:
                # dua pemisah: tentukan mana desimal
                num = float(s.replace(",", "")) if d > c else float(s.replace(".", "").replace(",", "."))
            else:
                sep = "." if d != -1 else ","
                if s.count(sep) > 1:
                    num = float(s.replace(sep, ""))
                else:
                    frac_len = len(s) - (s.rfind(sep) + 1)
                    num = float(s.replace(sep, ".")) if 1 <= frac_len <= 2 else float(s.replace(sep, ""))
        except Exception:
            # fallback keras: buang semua non-digit
            just_digits = re.sub(r"[^0-9\-]", "", s)
            num = float(just_digits) if just_digits not in ("", "-") else 0.0

        return -num if neg else num
    return sr.apply(p).astype(float)

def _idr(n: float) -> str:
    if pd.isna(n): return "-"
    s = f"{abs(int(round(n))):,}".replace(",", ".")
    return f"({s})" if n < 0 else s

# ========== header guessing helpers ==========

def _guess_header_row(df_no_header: pd.DataFrame, targets: List[str], scan_rows: int = 25) -> int:
    scan = min(scan_rows, len(df_no_header))
    best_row, best = 0, -1
    tgt = [t.lower() for t in targets]
    for i in range(scan):
        row = df_no_header.iloc[i].astype(str).str.lower().str.strip().fillna("")
        text = " ".join(row.tolist())
        score = sum(1 for t in tgt if t in text)
        if score > best:
            best_row, best = i, score
            if score >= 3:
                break
    return best_row

# ========== readers (cached) ==========

@st.cache_data(show_spinner=False)
def _bytes_of(uploaded) -> bytes:
    uploaded.seek(0); data = uploaded.read(); uploaded.seek(0); return data

def _read_excel_by_name(data: bytes, name: str, header=None) -> pd.DataFrame:
    low = name.lower()
    if low.endswith(".xlsb"):
        return pd.read_excel(io.BytesIO(data), engine="pyxlsb", dtype=str, na_filter=False, header=header)
    if low.endswith(".xlsx"):
        return pd.read_excel(io.BytesIO(data), engine="openpyxl", dtype=str, na_filter=False, header=header)
    if low.endswith(".xls"):
        return pd.read_excel(io.BytesIO(data), engine="xlrd", dtype=str, na_filter=False, header=header)
    # fallback CSV when mislabeled
    return pd.read_csv(io.BytesIO(data), sep=None, engine="python", dtype=str, na_filter=False,
                       header=header if header in (None, 0, 1) else "infer", encoding="utf-8-sig")

def _read_csv_auto(data: bytes, header="infer") -> pd.DataFrame:
    try:
        return pd.read_csv(io.BytesIO(data), dtype=str, na_filter=False, header=header, encoding="utf-8-sig")
    except Exception:
        return pd.read_csv(io.BytesIO(data), sep=None, engine="python", dtype=str, na_filter=False,
                           header=header, encoding="utf-8-sig")

# --- SMART: Tiket (Excel/CSV) ---
@st.cache_data(show_spinner=False)
def read_tiket_any(data: bytes, name: str) -> Tuple[pd.DataFrame, List[str]]:
    read_files, frames = [], []
    low = name.lower()
    targets = ["created","created date","create date","created (wib)","created time","tanggal",
               "tarif","nominal","amount","total","harga"]
    def _has_required(df: pd.DataFrame) -> bool:
        return (_find_col(df, ["Created","Created Date","Create Date","Created (WIB)","Created Time","Tanggal","Tanggal Buat"]) is not None
                and _find_col(df, ["Tarif","Nominal","Amount","Total","Harga"]) is not None)

    def _read_one(b: bytes, nm: str):
        df = pd.DataFrame()
        nm_low = nm.lower()
        if nm_low.endswith((".xlsx",".xls",".xlsb")):
            for hdr in (0, 1):   # coba header=0 lalu header=1 (abaikan baris 1)
                df = _read_excel_by_name(b, nm, header=hdr)
                if _has_required(df): return df
            peek = _read_excel_by_name(b, nm, header=None)
            hdr_row = _guess_header_row(peek, targets)
            df = _read_excel_by_name(b, nm, header=hdr_row)
            return df
        if nm_low.endswith(".csv"):
            for hdr in ("infer", 0, 1):
                df = _read_csv_auto(b, header=hdr)
                if _has_required(df): return df
            peek = _read_csv_auto(b, header=None).head(25)
            hdr_row = _guess_header_row(peek, targets)
            df = _read_csv_auto(b, header=hdr_row)
            return df
        return df

    if low.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for info in zf.infolist():
                if info.is_dir(): continue
                nm = info.filename
                if not nm.lower().endswith((".xlsx",".xls",".xlsb",".csv")): continue
                with zf.open(info) as f: b = f.read()
                df = _read_one(b, nm)
                if not df.empty: frames.append(df); read_files.append(nm)
    else:
        df = _read_one(data, name)
        if not df.empty: frames.append(df); read_files.append(name)

    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return out, read_files

# --- SMART: Settlement (CSV only) ---
@st.cache_data(show_spinner=False)
def read_settle_any(data: bytes, name: str) -> Tuple[pd.DataFrame, List[str]]:
    read_files, frames = [], []
    low = name.lower()
    targets = ["transaction date","trans date","tanggal transaksi","tgl transaksi","transactiondate","transaction_date",
               "settlement ammount","settlement amount","amount settlement","nominal settlement",
               "settlementamount","settlement_amount","net settlement amount","net settlement","amount","jumlah"]
    def _has_required(df: pd.DataFrame) -> bool:
        return (_find_col(df, ["Transaction Date","Trans Date","Tanggal Transaksi","Tgl Transaksi","TransactionDate","Transaction_Date"]) is not None
                and _find_col(df, ["Settlement Amount","Settlement Ammount","Amount Settlement","Nominal Settlement",
                                   "SettlementAmount","Settlement_Amount","Net Settlement Amount","Net Settlement","Amount"]) is not None)

    def _read_one_csv(b: bytes, nm: str):
        for hdr in ("infer", 0, 1):
            df = _read_csv_auto(b, header=hdr)
            if _has_required(df): return df
        peek = _read_csv_auto(b, header=None).head(25)
        hdr_row = _guess_header_row(peek, targets)
        df = _read_csv_auto(b, header=hdr_row)
        return df

    if low.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for info in zf.infolist():
                if info.is_dir(): continue
                nm = info.filename
                if not nm.lower().endswith(".csv"): continue
                with zf.open(info) as f: b = f.read()
                df = _read_one_csv(b, nm)
                if not df.empty: frames.append(df); read_files.append(nm)
    else:
        if low.endswith(".csv"):
            df = _read_one_csv(data, name)
            if not df.empty: frames.append(df); read_files.append(name)

    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return out, read_files

# ========== app UI ==========

st.set_page_config(page_title="Rekonsiliasi Tiket vs Settlement (Periode + ZIP)", layout="wide")
st.title("Rekonsiliasi Otomatis: Tiket Detail vs Report Settlement")

with st.sidebar:
    st.header("1) Upload Sumber")
    tiket_file  = st.file_uploader("📄 Tiket Detail (Excel/CSV/ZIP)", type=["xlsx","xls","xlsb","csv","zip"])
    settle_file = st.file_uploader("🧾 Report Settlement (CSV/ZIP)", type=["csv","zip"])

    st.header("2) Periode Rekonsiliasi")
    today = date.today()
    start_default = date(today.year, today.month, 1)
    end_default = date(today.year, today.month, calendar.monthrange(today.year, today.month)[1])
    start_date = st.date_input("Tanggal mulai", value=start_default)
    end_date   = st.date_input("Tanggal akhir", value=end_default)
    if start_date > end_date:
        st.error("Tanggal mulai tidak boleh setelah tanggal akhir.")

    show_chart = st.checkbox("Tampilkan grafik ringkas", value=True)
    go = st.button("Proses", type="primary", use_container_width=True)

if go:
    if not tiket_file:
        st.error("Upload **Tiket Detail** terlebih dahulu."); st.stop()
    if not settle_file:
        st.error("Upload **Report Settlement** terlebih dahulu."); st.stop()
    if start_date > end_date:
        st.error("Periode tidak valid."); st.stop()

    # -------- Tiket --------
    tiket_df, tiket_read = read_tiket_any(_bytes_of(tiket_file), tiket_file.name)
    c_created = _find_col(tiket_df, ["Created","Created Date","Create Date","Created (WIB)","Created Time","Tanggal","Tanggal Buat"])
    c_amt     = _find_col(tiket_df, ["Tarif","Nominal","Amount","Total","Harga"])
    miss=[]
    if c_created is None: miss.append("Tiket: kolom 'Created'")
    if c_amt is None:     miss.append("Tiket: kolom 'Tarif/Nominal/Amount'")
    if miss:
        st.error("Kolom wajib tidak ditemukan → " + "; ".join(miss))
        st.write("Kolom tersedia di Tiket:", list(tiket_df.columns)); st.stop()

    t = tiket_df[[c_created, c_amt]].copy()
    t["__dt"]     = _to_datetime_series(t[c_created])
    t["Tanggal"]  = t["__dt"].dt.normalize()
    t[c_amt]      = _to_money(t[c_amt])
    t = t[~t["Tanggal"].isna()]
    # filter periode
    t = t[(t["Tanggal"] >= pd.Timestamp(start_date)) & (t["Tanggal"] <= pd.Timestamp(end_date))]
    tiket_per_tgl = t.groupby("Tanggal")[c_amt].sum().rename("Tiket Detail ESPAY")

    # -------- Settlement --------
    settle_df, settle_read = read_settle_any(_bytes_of(settle_file), settle_file.name)
    s_date = _find_col(settle_df, ["Transaction Date","Trans Date","Tanggal Transaksi","Tgl Transaksi","TransactionDate","Transaction_Date"])
    s_amt  = _find_col(settle_df, ["Settlement Amount","Settlement Ammount","Amount Settlement","Nominal Settlement",
                                   "SettlementAmount","Settlement_Amount","Net Settlement Amount","Net Settlement","Amount"])
    miss2=[]
    if s_date is None: miss2.append("Settlement: kolom 'Transaction Date'")
    if s_amt  is None: miss2.append("Settlement: kolom 'Settlement Amount/Ammount'")
    if miss2:
        st.error("Kolom wajib tidak ditemukan → " + "; ".join(miss2))
        st.write("Kolom tersedia di Settlement:", list(settle_df.columns)); st.stop()

    s = settle_df[[s_date, s_amt]].copy()
    s["Tanggal"] = _to_datetime_series(s[s_date]).dt.normalize()
    # PERBAIKAN: parser angka lebih kuat (E+xx, dsb.)
    s["__raw_amount"] = s[s_amt]
    s[s_amt]          = _to_money(s[s_amt])
    s = s[~s["Tanggal"].isna()]
    # filter periode
    s = s[(s["Tanggal"] >= pd.Timestamp(start_date)) & (s["Tanggal"] <= pd.Timestamp(end_date))]
    settle_per_tgl = s.groupby("Tanggal")[s_amt].sum().rename("Settlement Dana ESPAY")

    # -------- Rekonsiliasi (outer join by date range) --------
    idx = pd.date_range(start_date, end_date, freq="D").date
    idx = pd.Index(idx, name="Tanggal")

    df = pd.DataFrame(index=idx)
    df["Tiket Detail ESPAY"]    = pd.to_numeric(tiket_per_tgl.reindex(idx, fill_value=0.0).values)
    df["Settlement Dana ESPAY"] = pd.to_numeric(settle_per_tgl.reindex(idx, fill_value=0.0).values)
    df["Selisih"]               = df["Tiket Detail ESPAY"] - df["Settlement Dana ESPAY"]

    view = df.reset_index()
    view.insert(0, "No", range(1, len(view)+1))

    total_row = pd.DataFrame([{
        "No":"", "Tanggal":"TOTAL",
        "Tiket Detail ESPAY": df["Tiket Detail ESPAY"].sum(),
        "Settlement Dana ESPAY": df["Settlement Dana ESPAY"].sum(),
        "Selisih": df["Selisih"].sum(),
    }])
    view_total = pd.concat([view, total_row], ignore_index=True)

    fmt = view_total.copy()
    for c in ["Tiket Detail ESPAY","Settlement Dana ESPAY","Selisih"]:
        fmt[c] = fmt[c].apply(_idr)

    st.subheader("Hasil Rekonsiliasi per Tanggal")
    st.dataframe(fmt, use_container_width=True, hide_index=True)

    with st.expander("📦 File yang dibaca & deteksi header"):
        st.write("Tiket Detail:", tiket_read or "(tidak ada)")
        st.write("Settlement:", settle_read or "(tidak ada)")
        st.caption("Pembaca mencoba header=0, header=1 (abaikan baris 1), lalu menebak baris header jika masih gagal.")

    with st.expander("🧪 Diagnosa pembacaan Settlement Amount"):
        st.write("Contoh 10 nilai mentah → nilai parsed:")
        diag = pd.DataFrame({
            "raw": s["__raw_amount"].head(10).tolist(),
            "parsed": s[s_amt].head(10).tolist()
        })
        st.dataframe(diag, use_container_width=True, hide_index=True)
        st.write("Total Settlement (periode):", _idr(s[s_amt].sum()))

    if show_chart and not df.empty:
        st.subheader("Grafik Ringkas")
        st.bar_chart(df[["Tiket Detail ESPAY","Settlement Dana ESPAY"]])

    # Unduh Excel
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        view_total.to_excel(xw, index=False, sheet_name="Rekonsiliasi")
        fmt.to_excel(xw, index=False, sheet_name="Rekonsiliasi_View")
    st.download_button(
        "Unduh Excel",
        data=out.getvalue(),
        file_name=f"rekonsiliasi_{start_date}_{end_date}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

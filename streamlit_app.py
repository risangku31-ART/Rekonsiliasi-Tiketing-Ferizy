# streamlit_app.py
# -*- coding: utf-8 -*-
"""
Rekonsiliasi otomatis:
- Tiket Detail (Excel): pakai kolom 'Created' (tanggal+jam) → diambil tanggalnya (date-only),
  jumlahkan 'Tarif/Nominal/Amount' per tanggal.
- Report Settlement (CSV): pakai 'Transaction Date' + 'Settlement Amount/Ammount',
  jumlahkan per tanggal.
- Tabel hasil = join per tanggal (outer), lengkap dengan selisih & ekspor Excel.
"""

from __future__ import annotations
import io, re
from typing import List, Optional
import numpy as np
import pandas as pd
import streamlit as st
from dateutil import parser as dtparser

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
    """Parse tanggal jam: dukung string ID/EN & Excel serial."""
    if sr.empty:
        return pd.to_datetime(pd.Series([], dtype=str), errors="coerce")
    s = sr.astype(str).str.strip()
    # buang label zona kalau ada
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
    """Parser uang robust: 1.095.568.800 / 1,095,568,800.00 / (123) / 123-"""
    def p(x) -> float:
        if x is None: return 0.0
        s = str(x).strip().lower()
        if s in ("", "-", "nan", "none"): return 0.0
        neg = False
        if s.startswith("(") and s.endswith(")"): neg, s = True, s[1:-1].strip()
        if s.endswith("-"): neg, s = True, s[:-1].strip()
        s = re.sub(r"(idr|rp|cr|dr)", "", s)
        s = re.sub(r"[^0-9\.,\-]", "", s)
        if not s: return 0.0
        d, c = s.rfind("."), s.rfind(",")
        if d == -1 and c == -1:
            num = float(s)
        elif d != -1 and c != -1:
            num = float(s.replace(",", "")) if d > c else float(s.replace(".", "").replace(",", "."))
        else:
            sep = "." if d != -1 else ","
            if s.count(sep) > 1:
                num = float(s.replace(sep, ""))
            else:
                frac = len(s) - (s.rfind(sep) + 1)
                num = float(s.replace(sep, ".")) if 1 <= frac <= 2 else float(s.replace(sep, ""))
        return -num if neg else num
    return sr.apply(p).astype(float)

def _idr(n: float) -> str:
    if pd.isna(n): return "-"
    s = f"{abs(int(round(n))):,}".replace(",", ".")
    return f"({s})" if n < 0 else s

# ========== readers (cached) ==========

@st.cache_data(show_spinner=False)
def _bytes_of(uploaded) -> bytes:
    uploaded.seek(0); data = uploaded.read(); uploaded.seek(0); return data

@st.cache_data(show_spinner=False)
def read_tiket_excel(data: bytes, name: str) -> pd.DataFrame:
    """Baca Excel Tiket (sheet pertama)."""
    import pandas as pd
    low = name.lower()
    if low.endswith(".xlsb"):
        df = pd.read_excel(io.BytesIO(data), engine="pyxlsb", dtype=str, na_filter=False)
    elif low.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(data), engine="openpyxl", dtype=str, na_filter=False)
    elif low.endswith(".xls"):
        df = pd.read_excel(io.BytesIO(data), engine="xlrd", dtype=str, na_filter=False)
    else:
        # kalau user tak sengaja kirim CSV, tetap kita dukung
        df = pd.read_csv(io.BytesIO(data), sep=None, engine="python", dtype=str, na_filter=False, encoding="utf-8-sig")
    return df

@st.cache_data(show_spinner=False)
def read_settlement_csv(data: bytes) -> pd.DataFrame:
    """Baca CSV Settlement (deteksi delimiter otomatis)."""
    import pandas as pd
    try:
        return pd.read_csv(io.BytesIO(data), dtype=str, na_filter=False, encoding="utf-8-sig")
    except Exception:
        return pd.read_csv(io.BytesIO(data), sep=None, engine="python", dtype=str, na_filter=False, encoding="utf-8-sig")

# ========== app UI ==========

st.set_page_config(page_title="Rekonsiliasi Tiket vs Settlement (Sederhana)", layout="wide")
st.title("Rekonsiliasi Otomatis: Tiket Detail vs Report Settlement")

col1, col2 = st.columns(2)
with col1:
    tiket_file = st.file_uploader("📄 Tiket Detail (Excel: .xlsx/.xls/.xlsb)", type=["xlsx","xls","xlsb","csv"])
with col2:
    settle_file = st.file_uploader("🧾 Report Settlement (CSV)", type=["csv"])

show_chart = st.checkbox("Tampilkan grafik ringkas", value=True)
go = st.button("Proses", type="primary")

if go:
    # -------- guard --------
    if not tiket_file:
        st.error("Upload **Tiket Detail** terlebih dahulu."); st.stop()
    if not settle_file:
        st.error("Upload **Report Settlement** terlebih dahulu."); st.stop()

    # -------- Tiket --------
    tiket_df = read_tiket_excel(_bytes_of(tiket_file), tiket_file.name)
    c_created = _find_col(tiket_df, ["Created","Created Date","Create Date","Created (WIB)","Created Time","Tanggal","Tanggal Buat"])
    c_amt     = _find_col(tiket_df, ["Tarif","Nominal","Amount","Total","Harga"])
    miss=[]
    if c_created is None: miss.append("Tiket: kolom 'Created'")
    if c_amt is None:     miss.append("Tiket: kolom 'Tarif/Nominal/Amount'")
    if miss:
        st.error("Kolom wajib tidak ditemukan → " + "; ".join(miss))
        st.write("Kolom tersedia di Tiket:", list(tiket_df.columns)); st.stop()

    t = tiket_df[[c_created, c_amt]].copy()
    t["__dt"]   = _to_datetime_series(t[c_created])
    t["Tanggal"] = t["__dt"].dt.normalize()
    t[c_amt]    = _to_money(t[c_amt])
    t = t[~t["Tanggal"].isna()]
    tiket_per_tgl = t.groupby("Tanggal")[c_amt].sum().rename("Tiket Detail")

    # -------- Settlement --------
    settle_df = read_settlement_csv(_bytes_of(settle_file))
    s_date = _find_col(settle_df, ["Transaction Date","Trans Date","Tanggal Transaksi","Tgl Transaksi"])
    s_amt  = _find_col(settle_df, ["Settlement Amount","Settlement Ammount","Amount Settlement","Nominal Settlement","Amount"])
    miss2=[]
    if s_date is None: miss2.append("Settlement: kolom 'Transaction Date'")
    if s_amt  is None: miss2.append("Settlement: kolom 'Settlement Amount/Ammount'")
    if miss2:
        st.error("Kolom wajib tidak ditemukan → " + "; ".join(miss2))
        st.write("Kolom tersedia di Settlement:", list(settle_df.columns)); st.stop()

    s = settle_df[[s_date, s_amt]].copy()
    s["Tanggal"] = _to_datetime_series(s[s_date]).dt.normalize()
    s[s_amt]     = _to_money(s[s_amt])
    s = s[~s["Tanggal"].isna()]
    settle_per_tgl = s.groupby("Tanggal")[s_amt].sum().rename("Settlement Dana")

    # -------- Rekonsiliasi (outer join by date) --------
    idx = sorted(set(tiket_per_tgl.index.tolist()) | set(settle_per_tgl.index.tolist()))
    idx = pd.to_datetime(pd.Index(idx)).date
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
        file_name="rekonsiliasi_tiket_vs_settlement.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

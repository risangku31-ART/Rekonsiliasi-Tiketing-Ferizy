# app.py
# -*- coding: utf-8 -*-
"""
Rekonsiliasi: Tiket Detail vs Settlement Dana
- Bulan & Tahun → tanggal 1..akhir bulan
- Multi-file upload (Tiket Excel; Settlement CSV/Excel)
- Tiket: gunakan kolom 'Created' (bukan 'Action date')
  * Jika jam 00:00:00–00:59:59 → mundurkan hari: WIB=0, WITA=−1, WIT=−2
  * Filter: St Bayar='paid' & Bank='ESPAY'
- Settlement Dana ESPAY: pakai Transaction Date
- Settlement Dana BCA/Non BCA: pakai Settlement Date (Product Name='BCA VA Online')
- Pengambilan Tiket Detail: lewati baris-1 (merge header) saat baca Excel
"""

from __future__ import annotations

import io
import re
import calendar
from typing import Optional, List, Tuple

import numpy as np
import pandas as pd
import streamlit as st
from dateutil import parser as dtparser


# ---------------- Utilities ----------------

def _parse_money(val) -> float:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 0.0
    if isinstance(val, (int, float, np.number)):
        return float(val)
    s = str(val).strip()
    if not s:
        return 0.0
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg, s = True, s[1:-1].strip()
    if s.endswith("-"):
        neg, s = True, s[:-1].strip()
    s = re.sub(r"(idr|rp|cr|dr)", "", s, flags=re.IGNORECASE)
    s = re.sub(r"[^0-9\.,\-]", "", s).strip()
    if s.startswith("-"):
        neg, s = True, s[1:].strip()
    last_dot = s.rfind("."); last_com = s.rfind(",")
    if   last_dot == -1 and last_com == -1: num_s = s
    elif last_dot > last_com:               num_s = s.replace(",", "")
    else:                                   num_s = s.replace(".", "").replace(",", ".")
    try:
        num = float(num_s)
    except Exception:
        num_s = s.replace(".", "").replace(",", "")
        num = float(num_s) if num_s else 0.0
    return -num if neg else num


def _to_num(sr: pd.Series) -> pd.Series:
    return sr.apply(_parse_money).astype(float)


def _to_datetime(val) -> Optional[pd.Timestamp]:
    if pd.isna(val):
        return None
    if isinstance(val, (int, float, np.number)):
        if np.isfinite(val) and 1 <= float(val) <= 100000:
            base = pd.Timestamp("1899-12-30")
            try:
                return base + pd.to_timedelta(float(val), unit="D")
            except Exception:
                return None
        return None
    if isinstance(val, (pd.Timestamp, np.datetime64)):
        return pd.to_datetime(val)
    s = str(val).strip()
    if not s:
        return None
    for dayfirst in (True, False):
        try:
            return pd.Timestamp(dtparser.parse(s, dayfirst=dayfirst, fuzzy=True))
        except Exception:
            continue
    return None


def _to_date(val) -> Optional[pd.Timestamp]:
    dt = _to_datetime(val)
    return dt.normalize() if dt is not None else None


def _read_csv(uploaded_file) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "iso-8859-1"):
        try:
            uploaded_file.seek(0)
            return pd.read_csv(
                uploaded_file,
                encoding=enc,
                sep=None,
                engine="python",
                dtype=str,
                na_filter=False,
            )
        except Exception:
            continue
    st.error(f"CSV gagal dibaca: {uploaded_file.name}. Simpan ulang sebagai UTF-8.")
    return pd.DataFrame()


def _read_excel_generic(uploaded_file, *, skiprows: int | list[int] | None = None) -> pd.DataFrame:
    """Baca Excel via openpyxl; tampilkan pesan jelas jika openpyxl tidak ada."""
    try:
        uploaded_file.seek(0)
        return pd.read_excel(uploaded_file, engine="openpyxl", dtype=str, na_filter=False, skiprows=skiprows)
    except ImportError:
        st.error(
            "Gagal membaca Excel: paket **openpyxl** belum terpasang.\n"
            "Solusi: tambahkan ke `requirements.txt` → `openpyxl>=3.1.0`."
        )
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Gagal membaca {uploaded_file.name}: {e}")
        return pd.DataFrame()


def _read_tiket_excel(uploaded_file) -> pd.DataFrame:
    """Prioritas: lewati baris-1 (merge title). Fallback tanpa skiprows jika gagal."""
    df = _read_excel_generic(uploaded_file, skiprows=1)
    if not df.empty:
        return df
    # Fallback: coba tanpa skip
    return _read_excel_generic(uploaded_file, skiprows=None)


def _read_any(uploaded_file) -> pd.DataFrame:
    if not uploaded_file:
        return pd.DataFrame()
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        return _read_csv(uploaded_file)
    return _read_excel_generic(uploaded_file, skiprows=None)


def _find_col(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    if df.empty:
        return None
    cols = [c for c in df.columns if isinstance(c, str)]
    m = {c.lower().strip(): c for c in cols}
    for n in names:
        key = n.lower().strip()
        if key in m:
            return m[key]
    for n in names:
        key = n.lower().strip()
        for c in cols:
            if key in c.lower():
                return c
    return None


def _idr_fmt(n: float) -> str:
    if pd.isna(n):
        return "-"
    neg = n < 0
    s = f"{abs(int(round(n))):,}".replace(",", ".")
    return f"({s})" if neg else s


def _concat_tiket_files(files) -> pd.DataFrame:
    if not files:
        return pd.DataFrame()
    frames = []
    for f in files:
        df = _read_tiket_excel(f)
        if not df.empty:
            df["__source__"] = f.name
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _concat_settle_files(files) -> pd.DataFrame:
    if not files:
        return pd.DataFrame()
    frames = []
    for f in files:
        df = _read_any(f)
        if not df.empty:
            df["__source__"] = f.name
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _month_selector() -> Tuple[int, int]:
    from datetime import date
    today = date.today()
    years = list(range(today.year - 5, today.year + 2))
    months = [
        ("01", "Januari"), ("02", "Februari"), ("03", "Maret"), ("04", "April"),
        ("05", "Mei"), ("06", "Juni"), ("07", "Juli"), ("08", "Agustus"),
        ("09", "September"), ("10", "Oktober"), ("11", "November"), ("12", "Desember"),
    ]
    c1, c2 = st.columns(2)
    with c1:
        year = st.selectbox("Tahun", years, index=years.index(today.year))
    with c2:
        sel = st.selectbox("Bulan", months, index=int(today.strftime("%m")) - 1, format_func=lambda x: x[1])
        month = int(sel[0])
    return year, month


def _norm_label(s: str) -> str:
    if s is None or (isinstance(s, float) and np.isnan(s)):
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _derive_action_date_from_created(created_sr: pd.Series, zone: str) -> pd.Series:
    """Ambil tanggal dari 'Created'; jika jam==0 → mundur sesuai zona: WIB=0, WITA=−1, WIT=−2."""
    zone = zone.upper()
    minus_days = 0
    if "WITA" in zone:
        minus_days = 1
    elif "WIT" in zone:
        minus_days = 2

    def conv(val):
        if pd.isna(val):
            return None
        s = str(val).strip()
        if not s:
            return None

        # Fixed width "YYYY-MM-DD HH:MM:SS"
        date_part, hour = None, None
        if len(s) >= 19 and s[10] == " " and s[13] == ":" and s[16] == ":":
            date_part = s[:10]
            try:
                hour = int(s[11:13])
            except Exception:
                hour = None

        if date_part is None or hour is None:
            dt = _to_datetime(s)
            if dt is None:
                return None
            base_date = pd.Timestamp(dt.date())
            hour = int(dt.hour)
        else:
            base_date = _to_date(date_part)
            if base_date is None:
                dt = _to_datetime(s)
                if dt is None:
                    return None
                base_date = pd.Timestamp(dt.date())
                hour = int(dt.hour)

        if hour == 0 and minus_days > 0:
            base_date = base_date - pd.Timedelta(days=minus_days)
        return base_date

    return created_sr.apply(conv)


# ---------------- App ----------------

st.set_page_config(page_title="Rekonsiliasi Tiket vs Settlement", layout="wide")
st.title("Rekonsiliasi: Tiket Detail vs Settlement Dana")

with st.sidebar:
    st.header("1) Upload Sumber (multi-file)")
    tiket_files = st.file_uploader("Tiket Detail (Excel .xls/.xlsx)", type=["xls", "xlsx"], accept_multiple_files=True)
    settle_files = st.file_uploader("Settlement Dana (CSV/Excel)", type=["csv", "xls", "xlsx"], accept_multiple_files=True)

    st.header("2) Parameter Bulan & Tahun (WAJIB)")
    y, m = _month_selector()
    month_start = pd.Timestamp(y, m, 1)
    month_end = pd.Timestamp(y, m, calendar.monthrange(y, m)[1])
    st.caption(f"Periode dipakai: {month_start.date()} s/d {month_end.date()}")

    st.header("3) Zona Waktu Cabang")
    zone = st.selectbox("Zona waktu", ["WIB (UTC+7)", "WITA (UTC+8)", "WIT (UTC+9)"], index=0)

    go = st.button("Proses", type="primary", use_container_width=True)

tiket_df = _concat_tiket_files(tiket_files)
settle_df = _concat_settle_files(settle_files)

if go:
    # -------- Tiket Detail (pakai 'Created') --------
    t_created = _find_col(tiket_df, ["Created"])
    t_amt     = _find_col(tiket_df, ["tarif"])
    t_stat    = _find_col(tiket_df, ["St Bayar", "Status Bayar", "status"])
    t_bank    = _find_col(tiket_df, ["Bank", "Payment Channel", "channel"])

    missing = []
    if t_created is None: missing.append("Tiket Detail: Created")
    if t_amt is None:     missing.append("Tiket Detail: tarif")
    if t_stat is None:    missing.append("Tiket Detail: St Bayar")
    if t_bank is None:    missing.append("Tiket Detail: Bank")
    if missing:
        st.error("Kolom wajib tidak ditemukan → " + "; ".join(missing)); st.stop()

    td = tiket_df.copy()
    td["__action_date"] = _derive_action_date_from_created(td[t_created], zone)
    td = td[~td["__action_date"].isna()]

    td_stat = td[t_stat].astype(str).str.strip().str.lower()
    td_bank = td[t_bank].astype(str).str.strip().str.lower()
    td = td[td_stat.eq("paid") & td_bank.eq("espay")]
    td = td[(td["__action_date"] >= month_start) & (td["__action_date"] <= month_end)]
    td[t_amt] = _to_num(td[t_amt])

    tiket_by_date = td.groupby(td["__action_date"])[t_amt].sum()
    tiket_by_date.index = pd.to_datetime(tiket_by_date.index).date

    # -------- Settlement Dana --------
    s_txn_date    = _find_col(settle_df, ["Transaction Date"])
    s_settle_date = _find_col(settle_df, ["Settlement Date", "SettlementDate"])
    s_amt         = _find_col(settle_df, ["Settlement Amount"])
    s_prod        = _find_col(settle_df, ["Product Name", "Product"])

    miss2 = []
    if s_txn_date is None:    miss2.append("Settlement: Transaction Date")
    if s_amt is None:         miss2.append("Settlement: Settlement Amount")
    if miss2: st.error("Kolom wajib tidak ditemukan → " + "; ".join(miss2)); st.stop()

    # Total ESPAY (Transaction Date)
    sd_txn = settle_df.copy()
    sd_txn[s_txn_date] = sd_txn[s_txn_date].apply(_to_date)
    sd_txn = sd_txn[~sd_txn[s_txn_date].isna()]
    sd_txn = sd_txn[(sd_txn[s_txn_date] >= month_start) & (sd_txn[s_txn_date] <= month_end)]
    sd_txn[s_amt] = _to_num(sd_txn[s_amt])
    settle_total = sd_txn.groupby(sd_txn[s_txn_date])[s_amt].sum()
    settle_total.index = pd.to_datetime(settle_total.index).date

    # BCA/Non-BCA (Settlement Date + Product Name)
    if s_settle_date is None or s_prod is None:
        settle_bca = pd.Series(dtype=float)
        settle_nonbca = pd.Series(dtype=float)
        st.warning("Kolom 'Settlement Date' atau 'Product Name' tidak ditemukan. BCA/Non-BCA = 0.")
    else:
        sd_settle = settle_df.copy()
        sd_settle[s_settle_date] = sd_settle[s_settle_date].apply(_to_date)
        sd_settle = sd_settle[~sd_settle[s_settle_date].isna()]
        sd_settle = sd_settle[(sd_settle[s_settle_date] >= month_start) & (sd_settle[s_settle_date] <= month_end)]
        sd_settle[s_amt] = _to_num(sd_settle[s_amt])

        target = _norm_label("BCA VA Online")
        prod_norm = sd_settle[s_prod].apply(_norm_label)
        bca_mask = prod_norm.eq(target)

        settle_bca    = sd_settle[bca_mask].groupby(sd_settle[bca_mask][s_settle_date])[s_amt].sum() if bca_mask.any() else pd.Series(dtype=float)
        settle_nonbca = sd_settle[~bca_mask].groupby(sd_settle[~bca_mask][s_settle_date])[s_amt].sum() if (~bca_mask).any() else pd.Series(dtype=float)

    # -------- Reindex ke 1..akhir bulan --------
    idx = pd.Index(pd.date_range(month_start, month_end, freq="D").date, name="Tanggal")

    def _reidx(s: pd.Series) -> pd.Series:
        if not isinstance(s, pd.Series): s = pd.Series(dtype=float)
        if len(getattr(s, "index", [])): s.index = pd.to_datetime(s.index).date
        return s.reindex(idx, fill_value=0.0)

    tiket_series  = _reidx(tiket_by_date)
    total_series  = _reidx(settle_total)
    bca_series    = _reidx(settle_bca)
    nonbca_series = _reidx(settle_nonbca)

    # -------- Tabel utama --------
    final = pd.DataFrame(index=idx)
    final["Tiket Detail ESPAY"]      = tiket_series.values
    final["Settlement Dana ESPAY"]   = total_series.values
    final["Selisih"]                 = final["Tiket Detail ESPAY"] - final["Settlement Dana ESPAY"]
    final["Settlement Dana BCA"]     = bca_series.values
    final["Settlement Dana Non BCA"] = nonbca_series.values

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
        fmt[c] = fmt[c].apply(_idr_fmt)

    st.subheader("Hasil Rekonsiliasi per Tanggal (mengikuti bulan parameter)")
    st.dataframe(fmt, use_container_width=True, hide_index=True)

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        view_total.to_excel(xw, index=False, sheet_name="Rekonsiliasi")
        fmt.to_excel(xw, index=False, sheet_name="Rekonsiliasi_View")
    st.download_button(
        "Unduh Excel",
        data=out.getvalue(),
        file_name=f"rekonsiliasi_{y}-{m:02d}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

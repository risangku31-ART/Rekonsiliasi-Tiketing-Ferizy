# streamlit_app.py
# -*- coding: utf-8 -*-
"""Rekonsiliasi: Tiket Detail vs Settlement Dana (fuzzy bank + grafik + diagnosa + toggle koreksi jam 00)
   Versi cepat: CSV C-engine, Excel peek 25 baris + usecols, parser vektorisasi, cache parsing per file.
"""

from __future__ import annotations

import io
import re
import zipfile
import calendar
from typing import Optional, List, Tuple, Iterable

import numpy as np
import pandas as pd
import streamlit as st
from dateutil import parser as dtparser


# ========== Parsers & helpers ==========

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
    dot, com = s.rfind("."), s.rfind(",")
    if dot == -1 and com == -1:
        num_s = s
    elif dot > com:
        num_s = s.replace(",", "")
    else:
        num_s = s.replace(".", "").replace(",", ".")
    try:
        num = float(num_s)
    except Exception:
        num_s = s.replace(".", "").replace(",", "")
        num = float(num_s) if num_s else 0.0
    return -num if neg else num


def _to_num(sr: pd.Series) -> pd.Series:
    """Vektorisasi parsing uang."""
    if sr.empty:
        return sr.astype(float)
    s = sr.astype(str).str.strip().str.lower()

    # (123) atau 123- -> -123
    s = s.str.replace(r"\(([^)]*)\)", r"-\1", regex=True)
    s = s.str.replace(r"\-$", "", regex=True)

    # buang label/teks & karakter non-digit dasar
    s = s.str.replace(r"(idr|rp|cr|dr)", "", regex=True)
    s = s.str.replace(r"[^0-9,\.\-]", "", regex=True)

    # jika ada titik & koma sekaligus: anggap koma = thousands, hapus koma
    both = s.str.contains(",") & s.str.contains("\.")
    s = s.where(~both, s.str.replace(",", "", regex=False))

    # jika hanya koma sebagai decimal, jadikan titik
    only_comma = s.str.contains(",") & ~s.str.contains("\.")
    s = s.where(~only_comma, s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))

    # hilangkan sisa thousands separator koma, lalu ke numeric
    out = pd.to_numeric(s.str.replace(",", "", regex=False), errors="coerce").fillna(0.0)
    return out.astype(float)


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


def _to_datetime_series(sr: pd.Series) -> pd.Series:
    """Vektorisasi parsing datetime + dukung Excel serial number."""
    if sr.empty:
        return pd.to_datetime(pd.Series([], dtype=str), errors="coerce")
    dt = pd.to_datetime(sr, errors="coerce", dayfirst=True, infer_datetime_format=True)
    mask_na = dt.isna()
    if mask_na.any():
        dt2 = pd.to_datetime(sr[mask_na], errors="coerce", dayfirst=False, infer_datetime_format=True)
        dt = dt.where(~mask_na, dt2)

    # Excel serial
    num = pd.to_numeric(sr, errors="coerce")
    mask_serial = num.between(1, 100000)
    if mask_serial.any():
        base = pd.Timestamp("1899-12-30")
        dt_serial = base + pd.to_timedelta(num[mask_serial], unit="D")
        dt = dt.where(~mask_serial, dt_serial)
    return dt


def _to_date(val) -> Optional[pd.Timestamp]:
    dt = _to_datetime(val)
    return dt.normalize() if dt is not None else None


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    if df.empty:
        return None
    cols = [c for c in df.columns if isinstance(c, str)]
    norm = {c.lower().strip(): c for c in cols}
    for n in candidates:
        k = n.lower().strip()
        if k in norm:
            return norm[k]
    for n in candidates:
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


# ========== Readers (cache, zip, xlsb, header guess) ==========

SUPPORTED_EXTS = (".xlsx", ".xls", ".xlsb", ".csv", ".zip")

@st.cache_data(show_spinner=False)
def _bytes_of(uploaded_file) -> bytes:
    uploaded_file.seek(0)
    data = uploaded_file.read()
    uploaded_file.seek(0)
    return data


def _read_csv_fast(buf: io.BytesIO) -> pd.DataFrame:
    """C-engine secepat mungkin, fallback bila perlu."""
    # 1) default: C-engine (delimiter koma)
    try:
        buf.seek(0)
        return pd.read_csv(buf, dtype=str, na_filter=False)
    except Exception:
        pass
    # 2) coba delimiter ';' (tetap C-engine)
    try:
        buf.seek(0)
        return pd.read_csv(buf, sep=";", dtype=str, na_filter=False)
    except Exception:
        pass
    # 3) terakhir: sniffing engine="python"
    buf.seek(0)
    return pd.read_csv(buf, engine="python", sep=None, dtype=str, na_filter=False)


def _read_excel_by_ext(buf: io.BytesIO, name: str, *, header=None, skiprows=None, nrows=None, usecols=None) -> pd.DataFrame:
    low = name.lower()
    common = dict(dtype=str, na_filter=False, header=header, skiprows=skiprows, nrows=nrows, usecols=usecols)
    if low.endswith(".xlsb"):
        return pd.read_excel(buf, engine="pyxlsb", **common)
    if low.endswith(".xlsx"):
        return pd.read_excel(buf, engine="openpyxl", **common)
    if low.endswith(".xls"):
        return pd.read_excel(buf, engine="xlrd", **common)
    raise ValueError(f"Ekstensi tidak didukung: {name}")


def _extract_zip_bytes(data: bytes) -> list[tuple[str, io.BytesIO]]:
    out: list[tuple[str, io.BytesIO]] = []
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            nm = info.filename
            if not nm.lower().endswith((".xlsx", ".xls", ".xlsb", ".csv")):
                continue
            with zf.open(info) as f:
                out.append((nm, io.BytesIO(f.read())))
    return out


def _guess_header_row(df_no_header: pd.DataFrame, targets: Iterable[str]) -> int:
    scan = min(20, len(df_no_header))
    best_row, best_score = 0, -1
    for i in range(scan):
        row = df_no_header.iloc[i].astype(str).str.lower().str.strip()
        text = " ".join(row.tolist())
        score = sum(1 for t in targets if t in text)
        if score > best_score:
            best_row, best_score = i, score
            if score >= 4:
                break
    return best_row


def _read_tiket_from_bytes(buf: io.BytesIO, name: str) -> pd.DataFrame:
    low = name.lower()
    if low.endswith(".csv"):
        df = _read_csv_fast(buf)
        df["__source__"] = name
        return df

    # Peek kecil untuk tebak header (hemat waktu)
    peek_targets = ["created", "tarif", "st bayar", "status", "bank", "channel", "payment"]
    buf.seek(0)
    peek = _read_excel_by_ext(buf, name, header=None, nrows=25)
    if peek.empty:
        return pd.DataFrame()
    header_row = _guess_header_row(peek, peek_targets)

    # Baca penuh tapi hanya kolom yang (kemungkinan) dipakai
    need_keys = [
        "created", "create date", "created date", "created (wib)", "created time", "tanggal",
        "tarif", "nominal", "amount", "total", "harga",
        "st bayar", "status bayar", "status",
        "bank", "payment channel", "channel", "payment method", "bank/ewallet"
    ]
    usecols_fn = lambda c: any(k in str(c).lower() for k in need_keys)

    buf.seek(0)
    df = _read_excel_by_ext(buf, name, header=header_row, usecols=usecols_fn)
    df["__source__"] = name
    return df


def _read_settle_from_bytes(buf: io.BytesIO, name: str) -> pd.DataFrame:
    low = name.lower()
    if low.endswith(".csv"):
        df = _read_csv_fast(buf)
    else:
        usecols_fn = lambda c: any(k in str(c).lower() for k in
                                   ["transaction date", "trans date", "tanggal transaksi",
                                    "settlement date", "settlementdate", "tanggal settlement",
                                    "settlement amount", "amount settlement", "nominal settlement", "amount",
                                    "product name", "product", "productname", "nama produk"])
        df = _read_excel_by_ext(buf, name, header=0, usecols=usecols_fn)
    df["__source__"] = name
    return df


@st.cache_data(show_spinner=False)
def _parse_tiket_any_cached(data: bytes, name: str) -> pd.DataFrame:
    low = name.lower()
    if low.endswith(".zip"):
        frames = [_read_tiket_from_bytes(buf, nm) for nm, buf in _extract_zip_bytes(data)]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return _read_tiket_from_bytes(io.BytesIO(data), name)


@st.cache_data(show_spinner=False)
def _parse_settle_any_cached(data: bytes, name: str) -> pd.DataFrame:
    low = name.lower()
    if low.endswith(".zip"):
        frames = [_read_settle_from_bytes(buf, nm) for nm, buf in _extract_zip_bytes(data)]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return _read_settle_from_bytes(io.BytesIO(data), name)


def _concat_tiket_files(files) -> pd.DataFrame:
    frames = []
    for f in (files or []):
        data = _bytes_of(f)
        df = _parse_tiket_any_cached(data, f.name)
        if not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _concat_settle_files(files) -> pd.DataFrame:
    frames = []
    for f in (files or []):
        data = _bytes_of(f)
        df = _parse_settle_any_cached(data, f.name)
        if not df.empty:
            frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ========== Business helpers ==========

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


def _derive_action_date_from_created(created_sr: pd.Series, zone: str, *, adjust_midnight: bool = True) -> pd.Series:
    dt = _to_datetime_series(created_sr)
    base = dt.dt.normalize()
    if not adjust_midnight:
        return base

    z = zone.upper()
    minus_days = 1 if "WITA" in z else (2 if "WIT" in z else 0)
    if minus_days == 0:
        return base

    mask_midnight = dt.dt.hour.eq(0)
    shift = pd.to_timedelta(mask_midnight.astype(int) * minus_days, unit="D")
    return (base - shift)


# ========== App ==========

st.set_page_config(page_title="Rekonsiliasi Tiket vs Settlement", layout="wide")
st.title("Rekonsiliasi: Tiket Detail vs Settlement Dana")

with st.sidebar:
    st.header("1) Upload Sumber (multi-file)")
    tiket_files = st.file_uploader(
        "Tiket Detail (.csv/.xls/.xlsx/.xlsb/.zip)",
        type=["csv", "xls", "xlsx", "xlsb", "zip"],
        accept_multiple_files=True,
    )
    settle_files = st.file_uploader(
        "Settlement Dana (.csv/.xls/.xlsx/.xlsb/.zip)",
        type=["csv", "xls", "xlsx", "xlsb", "zip"],
        accept_multiple_files=True,
    )

    st.header("2) Parameter Bulan & Tahun (WAJIB)")
    y, m = _month_selector()
    month_start = pd.Timestamp(y, m, 1)
    month_end = pd.Timestamp(y, m, calendar.monthrange(y, m)[1])
    st.caption(f"Periode: {month_start.date()} s/d {month_end.date()}")

    st.header("3) Zona Waktu Cabang")
    zone = st.selectbox("Zona waktu", ["WIB (UTC+7)", "WITA (UTC+8)", "WIT (UTC+9)"], index=0)
    adjust_midnight = st.checkbox("Koreksi jam 00 (WITA −1 hari, WIT −2 hari)", value=False)

    st.header("4) Opsi")
    fuzzy_bank = st.checkbox("Pencocokan Bank mengandung 'espay' (bukan exact)", value=True)
    show_charts = st.checkbox("Tampilkan grafik ringkas", value=True)

    go = st.button("Proses", type="primary", use_container_width=True)

# gabung file (dengan cache parsing per file)
tiket_df = _concat_tiket_files(tiket_files)
settle_df = _concat_settle_files(settle_files)

if go:
    if not tiket_files:
        st.error("Harap upload **Tiket Detail** minimal 1 file.")
        st.stop()

    # -------- Tiket (pakai Created) --------
    t_created = _find_col(tiket_df, ["Created", "Created Date", "Create Date", "Tanggal Buat", "Created (WIB)", "Created Time"])
    t_amt  = _find_col(tiket_df, ["tarif", "nominal", "amount", "total", "harga"])
    t_stat = _find_col(tiket_df, ["St Bayar", "Status Bayar", "status bayar", "status"])
    t_bank = _find_col(tiket_df, ["Bank", "Payment Channel", "channel", "payment method", "bank/ewallet"])

    missing = []
    if t_created is None: missing.append("Tiket Detail: Created")
    if t_amt is None:     missing.append("Tiket Detail: tarif/nominal")
    if t_stat is None:    missing.append("Tiket Detail: St Bayar/Status")
    if t_bank is None:    missing.append("Tiket Detail: Bank/Channel")
    if missing:
        st.error("Kolom wajib tidak ditemukan → " + "; ".join(missing))
        st.write("Kolom Tiket tersedia:", list(tiket_df.columns))
        st.stop()

    td = tiket_df.copy()
    td["__action_date"] = _derive_action_date_from_created(td[t_created], zone, adjust_midnight=adjust_midnight)
    td = td[~td["__action_date"].isna()]

    # === Diagnosa (membantu cek tgl 1–7 kosong) ===
    with st.expander("🔎 Diagnosa data Tiket (klik untuk lihat)"):
        try:
            td["_raw_dt"] = _to_datetime_series(td[t_created])
            td["_raw_date"] = td["_raw_dt"].dt.normalize()
        except Exception:
            td["_raw_dt"] = pd.NaT
            td["_raw_date"] = pd.NaT

        coverage = (
            td.groupby("__source__")
              .agg(raw_min=("_raw_date", "min"), raw_max=("_raw_date", "max"),
                   drv_min=("__action_date", "min"), drv_max=("__action_date", "max"),
                   rows=("__action_date", "size"))
              .reset_index()
        )
        st.markdown("**Cakupan tanggal per file Tiket** (raw `Created` vs tanggal terderivasi):")
        st.dataframe(coverage, use_container_width=True, hide_index=True)

        if td["_raw_dt"].notna().any():
            td["_hour"] = td["_raw_dt"].dt.hour
            hour_stat = td["_hour"].value_counts(dropna=True).sort_index()
            st.markdown("**Sebaran jam `Created`** (cek dominasi jam 00):")
            st.write(hour_stat.to_frame("rows"))

        total0 = len(tiket_df)
        miss_created = int(tiket_df[t_created].isna().sum() if t_created in tiket_df else 0)
        after_created = len(td)
        td_stat_all = tiket_df[t_stat].astype(str).str.strip().str.lower() if t_stat in tiket_df else pd.Series(dtype=str)
        td_bank_all = tiket_df[t_bank].astype(str).str.strip().str.lower() if t_bank in tiket_df else pd.Series(dtype=str)
        drop_status = int((td_stat_all != "paid").sum()) if len(td_stat_all) else 0
        drop_bank_exact = int((td_bank_all != "espay").sum()) if len(td_bank_all) else 0

        in_month_rows = int(((td["__action_date"] >= month_start) & (td["__action_date"] <= month_end)).sum())
        out_month_rows = len(td) - in_month_rows

        st.markdown("**Ringkasan baris terbuang**")
        st.write({
            "Total baris awal": total0,
            "Tanpa/invalid Created": miss_created,
            "Setelah derivasi Created": after_created,
            "Status ≠ paid": drop_status,
            "Bank ≠ espay (exact check)": drop_bank_exact,
            "Keluar bulan parameter (setelah koreksi)": out_month_rows,
            "Fuzzy Bank aktif?": True,  # hanya info
        })

        first7 = pd.date_range(month_start, month_start + pd.Timedelta(days=6), freq="D").date
        mask_1_7 = td["__action_date"].isin(first7)
        bank_1_7 = (
            td.loc[mask_1_7, t_bank]
              .astype(str).str.strip().str.lower()
              .value_counts()
              .rename("rows")
              .to_frame()
        )
        st.markdown("**Distribusi nilai `Bank` untuk tanggal 1–7 (setelah koreksi)**")
        st.dataframe(bank_1_7, use_container_width=True)

    # Filter paid + espay (exact / fuzzy) + bulan parameter
    td_stat_v = td[t_stat].astype(str).str.strip().str.lower()
    td_bank_v = td[t_bank].astype(str).str.strip().str.lower()
    bank_mask = td_bank_v.str.contains("espay") if fuzzy_bank else td_bank_v.eq("espay")
    td = td[td_stat_v.eq("paid") & bank_mask]
    td = td[(td["__action_date"] >= month_start) & (td["__action_date"] <= month_end)]
    td[t_amt] = _to_num(td[t_amt])

    tiket_by_date = td.groupby(td["__action_date"])[t_amt].sum()
    tiket_by_date.index = pd.to_datetime(tiket_by_date.index).date

    # -------- Settlement --------
    s_txn_date    = _find_col(settle_df, ["Transaction Date", "Trans Date", "Tanggal Transaksi"])
    s_settle_date = _find_col(settle_df, ["Settlement Date", "SettlementDate", "Tanggal Settlement"])
    s_amt         = _find_col(settle_df, ["Settlement Amount", "Amount Settlement", "Nominal Settlement", "Amount"])
    s_prod        = _find_col(settle_df, ["Product Name", "Product", "ProductName", "Nama Produk"])

    miss2 = []
    if s_txn_date is None: miss2.append("Settlement: Transaction Date")
    if s_amt is None:      miss2.append("Settlement: Settlement Amount")
    if miss2:
        st.error("Kolom wajib tidak ditemukan → " + "; ".join(miss2))
        st.write("Kolom Settlement tersedia:", list(settle_df.columns))
        st.stop()

    # Total ESPAY → Transaction Date
    sd_txn = settle_df.copy()
    sd_txn[s_txn_date] = _to_datetime_series(sd_txn[s_txn_date]).dt.normalize()
    sd_txn = sd_txn[~sd_txn[s_txn_date].isna()]
    sd_txn = sd_txn[(sd_txn[s_txn_date] >= month_start) & (sd_txn[s_txn_date] <= month_end)]
    sd_txn[s_amt] = _to_num(sd_txn[s_amt])
    settle_total = sd_txn.groupby(sd_txn[s_txn_date])[s_amt].sum()
    settle_total.index = pd.to_datetime(settle_total.index).date

    # BCA/Non-BCA → Settlement Date + Product Name == "BCA VA Online"
    if s_settle_date is not None and s_prod is not None:
        sd_settle = settle_df.copy()
        sd_settle[s_settle_date] = _to_datetime_series(sd_settle[s_settle_date]).dt.normalize()
        sd_settle = sd_settle[~sd_settle[s_settle_date].isna()]
        sd_settle = sd_settle[(sd_settle[s_settle_date] >= month_start) & (sd_settle[s_settle_date] <= month_end)]
        sd_settle[s_amt] = _to_num(sd_settle[s_amt])

        target = _norm_label("BCA VA Online")
        prod_norm = sd_settle[s_prod].apply(_norm_label)
        bca_mask = prod_norm.eq(target)

        settle_bca    = sd_settle[bca_mask].groupby(sd_settle[bca_mask][s_settle_date])[s_amt].sum() if bca_mask.any() else pd.Series(dtype=float)
        settle_nonbca = sd_settle[~bca_mask].groupby(sd_settle[~bca_mask][s_settle_date])[s_amt].sum() if (~bca_mask).any() else pd.Series(dtype=float)
    else:
        st.warning("Kolom 'Settlement Date' atau 'Product Name' tidak ditemukan. Kolom BCA/Non-BCA diisi 0.")
        settle_bca = pd.Series(dtype=float)
        settle_nonbca = pd.Series(dtype=float)

    # -------- Reindex ke 1..akhir bulan --------
    idx = pd.Index(pd.date_range(month_start, month_end, freq="D").date, name="Tanggal")

    def _reidx(s: pd.Series) -> pd.Series:
        if not isinstance(s, pd.Series):
            s = pd.Series(dtype=float)
        if len(getattr(s, "index", [])):
            s.index = pd.to_datetime(s.index).date
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
    for c in ["Tiket Detail ESPAY", "Settlement Dana ESPAY", "Selisih", "Settlement Dana BCA", "Settlement Dana Non BCA"]:
        fmt[c] = fmt[c].apply(_idr_fmt)

    st.subheader("Hasil Rekonsiliasi per Tanggal (mengikuti bulan parameter)")
    st.dataframe(fmt, use_container_width=True, hide_index=True)

    # -------- Grafik ringkas (opsional) --------
    if show_charts:
        st.subheader("Grafik Ringkas")
        chart_data = view[view["Tanggal"] != "TOTAL"].set_index("Tanggal")[
            ["Tiket Detail ESPAY", "Settlement Dana ESPAY", "Settlement Dana BCA", "Settlement Dana Non BCA"]
        ]
        st.bar_chart(chart_data)  # tanpa set warna

    # -------- Unduh Excel --------
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

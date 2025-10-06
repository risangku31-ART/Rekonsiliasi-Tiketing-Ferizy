# app.py
# -*- coding: utf-8 -*-
"""
Rekonsiliasi: Tiket Detail vs Settlement Dana (Action dari Created fixed-width 10)
- Bulan & Tahun → tanggal 1..akhir bulan
- Multi-file upload: Tiket (.xls/.xlsx/.zip), Settlement (.csv/.xls/.xlsx/.zip)
- Tiket: Action = 10 karakter pertama dari kolom 'Created' (contoh '2025-09-30 23:25:26' → '2025-09-30')
         Filter St Bayar='paid' & Bank contains 'espay'
- Settlement total: Transaction Date
- Settlement BCA/Non-BCA: Settlement Date + Product Name == 'BCA VA Online'
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

# ---------- Parsers / utils ----------

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

def _to_num(sr: pd.Series) -> pd.Series:
    return sr.apply(_parse_money).astype(float)

def _to_datetime(val) -> Optional[pd.Timestamp]:
    if pd.isna(val): return None
    if isinstance(val, (int, float, np.number)):
        if np.isfinite(val) and 1 <= float(val) <= 100000:
            base = pd.Timestamp("1899-12-30")
            try:
                return base + pd.to_timedelta(float(val), unit="D")
            except Exception:
                return None
        return None
    if isinstance(val, (pd.Timestamp, np.datetime64)): return pd.to_datetime(val)
    s = str(val).strip()
    if not s: return None
    for dayfirst in (True, False):
        try:
            return pd.Timestamp(dtparser.parse(s, dayfirst=dayfirst, fuzzy=True))
        except Exception:
            continue
    return None

def _to_date(val) -> Optional[pd.Timestamp]:
    dt = _to_datetime(val)
    return dt.normalize() if dt is not None else None

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

def _idr_fmt(n: float) -> str:
    if pd.isna(n): return "-"
    neg = n < 0
    s = f"{abs(int(round(n))):,}".replace(",", ".")
    return f"({s})" if neg else s

def _month_selector() -> Tuple[int, int]:
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

# ---------- Header guessing for Tiket Excel ----------

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

# ---------- ZIP-aware readers ----------

def _read_excel_by_name(name: str, buf: io.BytesIO, *, header=None, dtype=str) -> pd.DataFrame:
    low = name.lower()
    if low.endswith(".xlsx"):
        return pd.read_excel(buf, engine="openpyxl", header=header, dtype=dtype)
    if low.endswith(".xls"):
        return pd.read_excel(buf, engine="xlrd", header=header, dtype=dtype)
    raise ValueError(f"Ekstensi Excel tidak didukung: {name}")

def _read_csv_buf(buf: io.BytesIO) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "iso-8859-1"):
        try:
            buf.seek(0)
            return pd.read_csv(buf, encoding=enc, sep=None, engine="python", dtype=str, na_filter=False)
        except Exception:
            continue
    raise ValueError("Gagal membaca CSV (encoding).")

def _extract_zip(file) -> list[tuple[str, io.BytesIO]]:
    file.seek(0); data = file.read(); file.seek(0)
    arr: list[tuple[str, io.BytesIO]] = []
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for info in zf.infolist():
            if info.is_dir(): continue
            nm = info.filename
            if nm.lower().endswith((".csv", ".xls", ".xlsx")):
                with zf.open(info) as f:
                    arr.append((nm, io.BytesIO(f.read())))
    return arr

def _read_tiket_file(file) -> pd.DataFrame:
    name = file.name.lower()
    frames = []
    if name.endswith(".zip"):
        for nm, buf in _extract_zip(file):
            low = nm.lower()
            try:
                if low.endswith(".csv"):
                    df = _read_csv_buf(buf)
                else:
                    raw = _read_excel_by_name(nm, buf, header=None)
                    hdr = _guess_header_row(raw, ["created","action","tarif","st bayar","status","bank","channel"])
                    buf.seek(0)
                    df = _read_excel_by_name(nm, buf, header=hdr)
                df["__source__"] = f"{file.name}/{nm}"
                frames.append(df)
            except Exception as e:
                st.error(f"Gagal baca Tiket di ZIP {nm}: {e}")
    else:
        try:
            file.seek(0)
            raw = pd.read_excel(file, engine="openpyxl", header=None, dtype=str)
            hdr = _guess_header_row(raw, ["created","action","tarif","st bayar","status","bank","channel"])
            file.seek(0)
            df = pd.read_excel(file, engine="openpyxl", header=hdr, dtype=str)
            df["__source__"] = file.name
            frames.append(df)
        except Exception as e:
            st.error(f"Gagal membaca Tiket: {file.name} → {e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def _read_settle_file(file) -> pd.DataFrame:
    name = file.name.lower()
    frames = []
    if name.endswith(".zip"):
        for nm, buf in _extract_zip(file):
            try:
                df = _read_csv_buf(buf) if nm.lower().endswith(".csv") else _read_excel_by_name(nm, buf, header=0)
                df["__source__"] = f"{file.name}/{nm}"
                frames.append(df)
            except Exception as e:
                st.error(f"Gagal membaca Settlement di ZIP {nm}: {e}")
    else:
        try:
            if name.endswith(".csv"):
                file.seek(0); df = _read_csv_buf(io.BytesIO(file.read())); file.seek(0)
            else:
                file.seek(0); df = pd.read_excel(file, engine="openpyxl", dtype=str)
            df["__source__"] = file.name
            frames.append(df)
        except Exception as e:
            st.error(f"Gagal membaca Settlement: {file.name} → {e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def _concat(files, reader) -> pd.DataFrame:
    if not files: return pd.DataFrame()
    frames = []
    for f in files:
        df = reader(f)
        if not df.empty: frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# ---------- Action derivation (FIXED WIDTH 10) ----------

def _derive_action_fixed(created_sr: pd.Series) -> pd.Series:
    """Ambil 10 karakter pertama dari kolom 'Created' → 'YYYY-MM-DD'.
    Jika tidak cocok, fallback ke parser datetime dan format '%Y-%m-%d'."""
    def conv(val):
        if pd.isna(val):
            return None
        # numeric serial / timestamp → tanggal
        if isinstance(val, (int, float, np.number, pd.Timestamp, np.datetime64)):
            dt = _to_datetime(val)
            return pd.Timestamp(dt.date()) if dt is not None else None
        s = str(val).strip()
        if len(s) >= 10:
            s10 = s[:10]
            # validasi cepat yyyy-mm-dd
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s10):
                try:
                    return pd.Timestamp(s10)
                except Exception:
                    pass
        # fallback parser
        d = _to_date(s)
        return d
    return created_sr.apply(conv)

# ---------- App ----------

st.set_page_config(page_title="Rekonsiliasi Tiket vs Settlement", layout="wide")
st.title("Rekonsiliasi: Tiket Detail vs Settlement Dana")

with st.sidebar:
    st.header("1) Upload Sumber (multi-file)")
    tiket_files = st.file_uploader("Tiket Detail (.xls/.xlsx/.zip)", type=["xls", "xlsx", "zip"], accept_multiple_files=True)
    settle_files = st.file_uploader("Settlement Dana (.csv/.xls/.xlsx/.zip)", type=["csv", "xls", "xlsx", "zip"], accept_multiple_files=True)

    st.header("2) Parameter Bulan & Tahun (WAJIB)")
    y, m = _month_selector()
    month_start = pd.Timestamp(y, m, 1)
    month_end   = pd.Timestamp(y, m, calendar.monthrange(y, m)[1])
    st.caption(f"Periode dipakai: {month_start.date()} s/d {month_end.date()}")

    go = st.button("Proses", type="primary", use_container_width=True)

tiket_df  = _concat(tiket_files, _read_tiket_file)
settle_df = _concat(settle_files, _read_settle_file)

if go:
    # --- Mapping kolom (fleksibel) ---
    created_candidates = ["created", "created date", "create date", "created (wib)", "created time",
                          "action", "action date", "tanggal", "tgl"]
    amount_candidates  = ["tarif", "fare", "amount", "nominal", "total", "harga"]
    status_candidates  = ["st bayar", "status bayar", "status"]
    bank_candidates    = ["bank", "payment channel", "channel", "payment method", "bank/ewallet"]

    t_created = _find_col(tiket_df, created_candidates)
    t_amt     = _find_col(tiket_df, amount_candidates)
    t_stat    = _find_col(tiket_df, status_candidates)
    t_bank    = _find_col(tiket_df, bank_candidates)

    s_txn_date    = _find_col(settle_df, ["Transaction Date", "Trans Date", "Tanggal Transaksi"])
    s_settle_date = _find_col(settle_df, ["Settlement Date", "SettlementDate", "Tanggal Settlement"])
    s_amt         = _find_col(settle_df, ["Settlement Amount", "Amount Settlement", "Nominal Settlement", "Amount"])
    s_prod        = _find_col(settle_df, ["Product Name", "Product", "ProductName", "Nama Produk"])

    missing = []
    if t_created is None: missing.append("Tiket Detail: Created/Action")
    if t_amt is None:     missing.append("Tiket Detail: tarif/amount")
    if t_stat is None:    missing.append("Tiket Detail: St Bayar/Status")
    if t_bank is None:    missing.append("Tiket Detail: Bank/Channel")

    # Mapping manual jika auto gagal
    if missing:
        with st.expander("⚙️ Map kolom Tiket secara manual (auto tidak menemukan)"):
            st.caption("Pilih kolom yang benar jika deteksi otomatis salah.")
            cols = list(tiket_df.columns)
            def pick(default_keys: List[str], label: str) -> Optional[str]:
                auto = _find_col(tiket_df, default_keys)
                return st.selectbox(label, ["-- pilih --"] + cols,
                                    index=(cols.index(auto) + 1) if (auto in cols) else 0)
            c_created = pick(created_candidates, "Kolom Created/Action")
            c_amt     = pick(amount_candidates,  "Kolom Tarif/Amount")
            c_stat    = pick(status_candidates,  "Kolom Status Bayar")
            c_bank    = pick(bank_candidates,    "Kolom Bank/Channel")

        if c_created and c_created != "-- pilih --": t_created = c_created
        if c_amt and c_amt != "-- pilih --":         t_amt     = c_amt
        if c_stat and c_stat != "-- pilih --":       t_stat    = c_stat
        if c_bank and c_bank != "-- pilih --":       t_bank    = c_bank

    # Validasi final
    missing = []
    if t_created is None: missing.append("Tiket Detail: Created/Action")
    if t_amt is None:     missing.append("Tiket Detail: tarif/amount")
    if t_stat is None:    missing.append("Tiket Detail: St Bayar/Status")
    if t_bank is None:    missing.append("Tiket Detail: Bank/Channel")
    if missing:
        st.error("Kolom wajib tidak ditemukan → " + "; ".join(missing))
        if not tiket_df.empty:  st.write("Kolom Tiket tersedia:", list(tiket_df.columns))
        if not settle_df.empty: st.write("Kolom Settlement tersedia:", list(settle_df.columns))
        st.stop()

    if s_txn_date is None or s_amt is None:
        st.error("Settlement wajib: 'Transaction Date' & 'Settlement Amount' tidak ditemukan.")
        st.write("Kolom Settlement tersedia:", list(settle_df.columns))
        st.stop()
    if s_settle_date is None or s_prod is None:
        st.warning("Kolom 'Settlement Date' atau 'Product Name' tidak ditemukan. BCA/Non-BCA akan 0.")

    # --- Tiket (Action dari Created: fixed-width 10) ---
    td = tiket_df.copy()
    td["__action_date"] = _derive_action_fixed(td[t_created])
    td = td[~td["__action_date"].isna()]
    td_stat_v = td[t_stat].astype(str).str.strip().str.lower()
    td_bank_v = td[t_bank].astype(str).str.strip().str.lower()
    td = td[td_stat_v.eq("paid") & td_bank_v.str.contains("espay")]
    td = td[(td["__action_date"] >= month_start) & (td["__action_date"] <= month_end)]
    td[t_amt] = _to_num(td[t_amt])

    tiket_by_date = td.groupby(td["__action_date"])[t_amt].sum()
    tiket_by_date.index = pd.to_datetime(tiket_by_date.index).date

    # --- Settlement total (Transaction Date) ---
    sd_txn = settle_df.copy()
    sd_txn[s_txn_date] = sd_txn[s_txn_date].apply(_to_date)
    sd_txn = sd_txn[~sd_txn[s_txn_date].isna()]
    sd_txn = sd_txn[(sd_txn[s_txn_date] >= month_start) & (sd_txn[s_txn_date] <= month_end)]
    sd_txn[s_amt] = _to_num(sd_txn[s_amt])
    settle_total = sd_txn.groupby(sd_txn[s_txn_date])[s_amt].sum()
    settle_total.index = pd.to_datetime(settle_total.index).date

    # --- BCA / Non-BCA (Settlement Date + Product Name) ---
    if (s_settle_date is not None) and (s_prod is not None):
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
    else:
        settle_bca = pd.Series(dtype=float)
        settle_nonbca = pd.Series(dtype=float)

    # --- Reindex 1..akhir bulan ---
    idx = pd.Index(pd.date_range(month_start, month_end, freq="D").date, name="Tanggal")

    def _reidx(s: pd.Series) -> pd.Series:
        if not isinstance(s, pd.Series): s = pd.Series(dtype=float)
        if len(getattr(s, "index", [])): s.index = pd.to_datetime(s.index).date
        return s.reindex(idx, fill_value=0.0)

    tiket_series  = _reidx(tiket_by_date)
    total_series  = _reidx(settle_total)
    bca_series    = _reidx(settle_bca)
    nonbca_series = _reidx(settle_nonbca)

    # --- Tabel output ---
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

    # Unduh Excel
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

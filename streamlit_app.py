# path: streamlit_app.py
# --- (potongan atas file Anda tetap sama) ---

import io
import csv
import re
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from calendar import monthrange
from collections import defaultdict, OrderedDict
from typing import List, Optional, Tuple, Dict, Iterable, Union

import pandas as pd
import streamlit as st
from openpyxl import load_workbook

st.set_page_config(page_title="Rekonsiliasi Payment Report", layout="wide")

# =========================== KONST ===========================
COL_H = "TIPE PEMBAYARAN"
COL_B = "TANGGAL PEMBAYARAN"
COL_AA = "REF NO"
COL_K = "TOTAL TARIF TANPA BIAYA ADMIN (Rp.)"
COL_X = "SOF ID"
COL_ASAL = "ASAL"
REQUIRED_COLS = [COL_H, COL_B, COL_AA, COL_K, COL_X, COL_ASAL]

CAT_COLS = [
    "Cash", "Prepaid BRI", "Prepaid BNI", "Prepaid Mandiri", "Prepaid BCA",
    "SKPT", "IFCS", "Reedem", "ESPAY", "Finnet",
]
NON_COMPONENTS = ["Cash", "Prepaid BRI", "Prepaid BNI", "Prepaid Mandiri", "Prepaid BCA", "SKPT", "IFCS", "Reedem"]

DEFAULT_CSV_CHUNK_ROWS = 200_000
XLSX_BATCH_ROWS = 50_000
NONBCA_CREDIT_COL_INDEX = 9

_HAS_PYARROW = False
try:
    import pyarrow  # noqa
    _HAS_PYARROW = True
except Exception:
    pass

# =========================== Utils ===========================
def ss_get_set(key: str, default):
    if key not in st.session_state:
        st.session_state[key] = default
    return st.session_state[key]

def _style_table(df_display: pd.DataFrame, highlight: bool):
    numeric_cols = df_display.select_dtypes(include="number").columns.tolist()
    styler = df_display.style.format("{:,.0f}", subset=numeric_cols)
    if highlight:
        for col in [
            "Selisih Tiket Detail vs Settlement Report",
            "Selisih Dana Masuk vs Settlement Report",
            "Selisih",
        ]:
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
    if name is None:
        return "Tidak diketahui"
    s = str(name).strip()
    up = s.upper()
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
        if not val:
            return 0.0
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
        try:
            return float(val)
        except Exception:
            return 0.0

    vals = x.apply(_to_float)
    vals = vals.where(~neg, -vals.abs())
    return vals.round(0).astype("Int64")

def _mask_remark_contains(remark: pd.Series, keywords: List[str]) -> pd.Series:
    if not keywords:
        return pd.Series([True] * len(remark), index=remark.index)
    norm = remark.astype(str).str.upper()
    mask = pd.Series(False, index=remark.index)
    for k in keywords:
        mask = mask | norm.str.contains(str(k).upper(), na=False)
    return mask

def _sniff_delimiter(sample: bytes) -> str:
    try:
        return csv.Sniffer().sniff(sample.decode("utf-8", "ignore")).delimiter
    except Exception:
        return ","

def _to_date_minus1(v) -> pd.Series:
    t = pd.to_datetime(v, errors="coerce", dayfirst=True)
    return t - pd.Timedelta(days=1)

def _port_from_filename(fname: str) -> str:
    up = str(fname).upper()
    if "MERAK" in up:      return "ASDP Merak"
    if "BAKAUHENI" in up:  return "ASDP Bakauheni"
    if "GILIMANUK" in up:  return "ASDP Gilimanuk"
    if "KETAPANG" in up:   return "ASDP Ketapang"
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

# =========================== Pembacaan CSV helper ===========================
def _iter_csv_chunks(file_like: Union[io.BytesIO, io.BufferedReader], usecols: List[str], year: int, month: int,
                     chunksize: int = DEFAULT_CSV_CHUNK_ROWS):
    head = file_like.read(2048); file_like.seek(0)
    delim = _sniff_delimiter(head)
    if _HAS_PYARROW:
        try:
            df = pd.read_csv(file_like, usecols=usecols, engine="pyarrow", sep=delim)
            t = pd.to_datetime(df[COL_B], errors="coerce")
            mask = (t.dt.year == year) & (t.dt.month == month)
            if mask.any():
                sub = df.loc[mask].copy()
                sub["Tanggal"] = t.loc[mask].dt.date
                yield sub
            return
        except Exception:
            file_like.seek(0)
    for chunk in pd.read_csv(
        file_like, usecols=usecols, chunksize=chunksize, sep=delim, encoding="utf-8-sig",
        dtype={COL_H: "string", COL_AA: "string", COL_X: "string", COL_ASAL: "string"},
        on_bad_lines="skip", engine="python",
    ):
        t = pd.to_datetime(chunk[COL_B], errors="coerce")
        mask = (t.dt.year == year) & (t.dt.month == month)
        if mask.any():
            sub = chunk.loc[mask].copy()
            sub["Tanggal"] = t.loc[mask].dt.date
            yield sub

# =========================== Payment (ringkas) ===========================
def _empty_agg():
    return defaultdict(lambda: defaultdict(float))

def _update_agg_series(agg, ser: pd.Series, colname: str) -> None:
    if ser.empty:
        return
    for (dt, asal), val in ser.items():
        agg[(dt, asal)][colname] += float(val)

def _apply_rules_and_update(df_chunk: pd.DataFrame, agg) -> None:
    H = df_chunk[COL_H].fillna("").astype(str).str.lower()
    AA = df_chunk[COL_AA].fillna("").astype(str).str.lower()
    X  = df_chunk[COL_X].fillna("").astype(str).str.lower()
    ASAL = df_chunk[COL_ASAL].fillna("Tidak diketahui").astype(str).str.strip()

    amt = pd.to_numeric(df_chunk[COL_K], errors="coerce").fillna(0)
    tgl = df_chunk["Tanggal"]

    def sum_by_key(mask: pd.Series) -> pd.Series:
        if mask.any():
            return amt[mask].groupby([tgl[mask], ASAL[mask]], dropna=False).sum(min_count=1)
        mi = pd.MultiIndex.from_arrays([[], []], names=["Tanggal", "Pelabuhan"])
        return pd.Series(index=mi, dtype="float64")

    rules = OrderedDict([
        ("Cash", H.str.contains("cash", na=False)),
        ("Prepaid BRI", H.str.contains("prepaid-bri", na=False)),
        ("Prepaid BNI", H.str.contains("prepaid-bni", na=False)),
        ("Prepaid Mandiri", H.str.contains("prepaid-mandiri", na=False)),
        ("Prepaid BCA", H.str.contains("prepaid-bca", na=False)),
        ("SKPT", H.str.contains("skpt", na=False)),
        ("IFCS", H.str.contains("ifcs", na=False)),
        ("Reedem", H.str.contains("reedem", na=False) | H.str.contains("redeem", na=False)),
        ("ESPAY", H.str.contains("finpay", na=False) & AA.str.startswith("esp", na=False)),
        ("Finnet", H.str.contains("finpay", na=False) & (~AA.str.startswith("esp", na=False))),
    ])
    for name, m in rules.items():
        _update_agg_series(agg, sum_by_key(m), name)

    is_finpay = H.str.contains("finpay", na=False)
    is_bca_tag = X.str.contains("vabcaespay", na=False) | X.str.contains("bluespay", na=False)
    _update_agg_series(agg, sum_by_key(is_finpay & is_bca_tag), "BCA")
    _update_agg_series(agg, sum_by_key(is_finpay & (~is_bca_tag)), "NON BCA")

    is_not_spay = ~X.str.contains("spay", na=False)
    is_bca = X.str.contains("bca", na=False)
    _update_agg_series(agg, sum_by_key(is_finpay & is_not_spay & is_bca), "FINNET_TIKET_BCA")
    _update_agg_series(agg, sum_by_key(is_finpay & is_not_spay & (~is_bca)), "FINNET_TIKET_NON_BCA")

    is_spay = X.str.contains("spay", na=False)
    _update_agg_series(agg, sum_by_key(is_spay & is_bca_tag), "ESPAY_TIKET_BCA")
    _update_agg_series(agg, sum_by_key(is_spay & (~is_bca_tag)), "ESPAY_TIKET_NON_BCA")

def _build_result_from_agg(agg) -> pd.DataFrame:
    if not agg:
        return pd.DataFrame()
    rows: List[dict] = []
    for (dt, asal), bucket in agg.items():
        row = {"Tanggal": dt, "Pelabuhan": asal}
        for c in CAT_COLS:
            row[c] = bucket.get(c, 0.0)
        row["Total"] = sum(row[c] for c in CAT_COLS)
        bca = bucket.get("BCA", 0.0); nonbca = bucket.get("NON BCA", 0.0)
        row["BCA"] = bca; row["NON BCA"] = nonbca
        row["NON"] = sum(row[c] for c in NON_COMPONENTS)
        row["TOTAL"] = bca + nonbca + row["NON"]
        row["Selisih"] = row["TOTAL"] - row["Total"]
        rows.append(row)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df[["Tanggal", "Pelabuhan"] + CAT_COLS + ["Total", "BCA", "NON BCA", "NON", "TOTAL", "Selisih"]]
    return df.sort_values(["Pelabuhan", "Tanggal"]).reset_index(drop=True)

def _process_csv_fast(data: bytes, year: int, month: int, agg, chunk_rows: int) -> None:
    buf = io.BytesIO(data)
    for sub in _iter_csv_chunks(buf, REQUIRED_COLS, year, month, chunksize=chunk_rows):
        _apply_rules_and_update(sub, agg)

def _flush_xlsx_batch(buf: List[List], year: int, month: int, agg) -> None:
    df = pd.DataFrame(buf, columns=[COL_H, COL_B, COL_AA, COL_K, COL_X, COL_ASAL])
    t = pd.to_datetime(df[COL_B], errors="coerce")
    mask = (t.dt.year == year) & (t.dt.month == month)
    if not mask.any():
        return
    sub = df.loc[mask].copy()
    sub["Tanggal"] = t.loc[mask].dt.date
    _apply_rules_and_update(sub, agg)

def _process_xlsx_streaming(data: bytes, year: int, month: int, agg) -> None:
    try:
        wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]
        rows = ws.iter_rows(values_only=True)
        header = next(rows, None)
        if header is None:
            wb.close(); return
        name_to_idx = {str(h).strip(): i for i, h in enumerate(header) if h is not None}
        if not all(c in name_to_idx for c in REQUIRED_COLS):
            wb.close(); return
        buf = []
        for r in rows:
            try:
                buf.append([r[name_to_idx[COL_H]], r[name_to_idx[COL_B]], r[name_to_idx[COL_AA]],
                            r[name_to_idx[COL_K]], r[name_to_idx[COL_X]], r[name_to_idx[COL_ASAL]]])
            except Exception:
                continue
            if len(buf) >= XLSX_BATCH_ROWS:
                _flush_xlsx_batch(buf, year, month, agg); buf.clear()
        if buf:
            _flush_xlsx_batch(buf, year, month, agg); buf.clear()
        wb.close()
    except Exception:
        try:
            df = pd.read_excel(io.BytesIO(data), sheet_name=0, usecols=REQUIRED_COLS)
        except Exception:
            return
        t = pd.to_datetime(df[COL_B], errors="coerce")
        mask = (t.dt.year == year) & (t.dt.month == month)
        if not mask.any(): return
        sub = df.loc[mask].copy(); sub["Tanggal"] = t.loc[mask].dt.date
        _apply_rules_and_update(sub, agg)

def _process_xlsb(data: bytes, year: int, month: int, agg) -> None:
    try:
        df = pd.read_excel(io.BytesIO(data), sheet_name=0, usecols=REQUIRED_COLS, engine="pyxlsb")
    except Exception:
        return
    t = pd.to_datetime(df[COL_B], errors="coerce")
    mask = (t.dt.year == year) & (t.dt.month == month)
    if not mask.any(): return
    sub = df.loc[mask].copy(); sub["Tanggal"] = t.loc[mask].dt.date
    _apply_rules_and_update(sub, agg)

def _process_payment_file(name: str, data: bytes, year: int, month: int, chunk_rows: int) -> dict:
    agg = _empty_agg()
    low = str(name).lower()
    try:
        if low.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                for info in zf.infolist():
                    if info.is_dir(): continue
                    fn = info.filename.lower()
                    if not fn.endswith((".xlsx", ".xls", ".xlsb", ".csv")): continue
                    content = zf.read(info)
                    if fn.endswith((".xlsx", ".xls")): _process_xlsx_streaming(content, year, month, agg)
                    elif fn.endswith(".xlsb"): _process_xlsb(content, year, month, agg)
                    else: _process_csv_fast(content, year, month, agg, chunk_rows)
        elif low.endswith((".xlsx", ".xls")): _process_xlsx_streaming(data, year, month, agg)
        elif low.endswith(".xlsb"): _process_xlsb(data, year, month, agg)
        elif low.endswith(".csv"): _process_csv_fast(data, year, month, agg, chunk_rows)
    except Exception:
        pass
    out = {}
    for (dt, asal), bucket in agg.items():
        key_dt = pd.to_datetime(dt).date() if not isinstance(dt, date) else dt
        out[(key_dt, str(asal))] = dict(bucket)
    return out

def _merge_plain_aggs(target: dict, source: dict) -> None:
    for key, bucket in source.items():
        if key not in target:
            target[key] = dict(bucket)
        else:
            for k2, v2 in bucket.items():
                target[key][k2] = float(target[key].get(k2, 0.0)) + float(v2)

def load_and_aggregate_fast(files, year: int, month: int, max_workers: int, csv_chunk_rows: int) -> dict:
    merged_plain = {}
    if not files: return defaultdict(lambda: defaultdict(float))
    def submit_one(executor, f):
        try: data = f.getvalue() if hasattr(f, "getvalue") else f.read()
        except Exception: data = b""
        name = getattr(f, "name", "file")
        return executor.submit(_process_payment_file, name, data, year, month, csv_chunk_rows)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [submit_one(ex, f) for f in files]
        for fut in as_completed(futs):
            try: _merge_plain_aggs(merged_plain, fut.result() or {})
            except Exception: continue
    agg = defaultdict(lambda: defaultdict(float))
    for key, bucket in merged_plain.items():
        agg[key].update(bucket)
    return agg

# =========================== ESPAY (EDIT: alias kolom) ===========================
def _pick_col(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    """Return real column name matching any alias (normalized)."""
    if df is None or df.empty:
        return None
    norm_map = {_norm_colname(c): c for c in df.columns}
    alias_norms = [_norm_colname(a) for a in aliases]
    # exact
    for a in alias_norms:
        if a in norm_map: return norm_map[a]
    # startswith
    for a in alias_norms:
        for n, real in norm_map.items():
            if n.startswith(a) or a.startswith(n):
                return real
    # contains
    for a in alias_norms:
        for n, real in norm_map.items():
            if a in n or n in a:
                return real
    return None

def _read_settlement_single_table(content: bytes, filename: str) -> Optional[pd.DataFrame]:
    low = str(filename).lower()
    try:
        if low.endswith(".csv"):
            buf = io.BytesIO(content)
            head = buf.read(2048); buf.seek(0)
            delim = _sniff_delimiter(head)
            if _HAS_PYARROW:
                df = pd.read_csv(buf, sep=delim, engine="pyarrow")
            else:
                try:
                    df = pd.read_csv(buf, sep=delim, encoding="utf-8-sig", engine="python", on_bad_lines="skip")
                except UnicodeDecodeError:
                    buf.seek(0)
                    df = pd.read_csv(buf, sep=delim, encoding="latin1", engine="python", on_bad_lines="skip")
        elif low.endswith(".xlsx"):
            df = pd.read_excel(io.BytesIO(content), engine="openpyxl")
        else:
            return None
    except Exception:
        return None

    if df is None or df.empty:
        return None
    df.rename(columns={c: str(c).strip() for c in df.columns}, inplace=True)

    # Aliases as requested
    col_product = _pick_col(df, ["Product Name", "Channel", "Product", "Payment Channel", "Payment Method", "Method"])
    col_date    = _pick_col(df, ["Settlement Date", "settlement_date", "Tanggal", "Date"])
    col_amount  = _pick_col(df, ["Settlement Amount", "Amount - Tx Fee", "AmountTxFee", "Amount_Tx_Fee", "Net Amount"])
    col_va      = _pick_col(df, ["VA NAME", "VA Name", "va_name", "Merchant_name", "Merchant name", "Merchant Name"])

    needed = [col_product, col_date, col_amount, col_va]
    if any(c is None for c in needed):
        return None

    rename_map = {
        col_product: "Product Name",
        col_date: "Settlement Date",
        col_amount: "Settlement Amount",
        col_va: "VA NAME",
    }
    df = df.rename(columns=rename_map)
    return df[["Product Name", "Settlement Amount", "Settlement Date", "VA NAME"]].copy()

def _load_settlement_espay(files) -> pd.DataFrame:
    all_dfs: List[pd.DataFrame] = []
    for f in files or []:
        try: data = f.getvalue() if hasattr(f, "getvalue") else f.read()
        except Exception: continue
        name = f.name
        try:
            df_part = _read_settlement_single_table(data, name)
            if df_part is not None and not df_part.empty:
                all_dfs.append(df_part)
        except Exception:
            continue
    if not all_dfs:
        return pd.DataFrame()
    return pd.concat(all_dfs, ignore_index=True)

def _build_espay_settlement_table(df_settlement: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    if df_settlement is None or df_settlement.empty: return pd.DataFrame()
    df = df_settlement.copy()
    t = pd.to_datetime(df["Settlement Date"], errors="coerce")
    df["Tanggal"] = t.dt.date
    df = df.loc[(t.dt.year == year) & (t.dt.month == month)].copy()
    if df.empty: return pd.DataFrame()

    va_name = df["VA NAME"].fillna("").astype(str).str.upper()
    def map_pelabuhan(name: str) -> Optional[str]:
        if "BAKAUHENI" in name: return "ASDP Bakauheni"
        if "GILIMANUK" in name: return "ASDP Gilimanuk"
        if "KETAPANG" in name:  return "ASDP Ketapang"
        if "MERAK" in name:     return "ASDP Merak"
        return None
    df["Pelabuhan"] = va_name.apply(map_pelabuhan)
    df = df[df["Pelabuhan"].notna()].copy()
    if df.empty: return pd.DataFrame()

    amt_raw = df["Settlement Amount"].astype(str).str.strip()
    amt = pd.to_numeric(amt_raw.str.replace(r"[^\d\-]", "", regex=True), errors="coerce").fillna(0.0) / 100.0  # penting: file ESPAY biasanya cent

    pn = df["Product Name"].fillna("").astype(str).str.lower()
    is_va = pn.str.contains("va", na=False)
    is_bca = pn.str.contains("bca", na=False) | pn.str.contains("blu", na=False)

    df["VIRTUAL ACCOUNT"] = amt.where(is_va, 0.0)
    df["E-MONEY"] = amt.where(~is_va, 0.0)
    df["BCA"] = amt.where(is_bca, 0.0)
    df["NON BCA"] = amt.where(~is_bca, 0.0)

    grouped = df.groupby(["Tanggal", "Pelabuhan"], dropna=False)[["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]].sum().reset_index()
    for c in ["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]:
        grouped[c] = grouped[c].fillna(0.0)

    unique_ports = grouped["Pelabuhan"].dropna().unique()
    if len(unique_ports) == 0: return pd.DataFrame()
    days_in_month = monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days_in_month + 1)]

    full_idx = pd.MultiIndex.from_product([all_dates, unique_ports], names=["Tanggal", "Pelabuhan"])
    out = pd.DataFrame(index=full_idx).reset_index().merge(grouped, on=["Tanggal", "Pelabuhan"], how="left")
    for c in ["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]:
        out[c] = out[c].fillna(0.0)
    out["TOTAL VA + E-MONEY"] = out["VIRTUAL ACCOUNT"] + out["E-MONEY"]
    out["TOTAL BCA + NON BCA"] = out["BCA"] + out["NON BCA"]
    desired = ["Tanggal", "Pelabuhan", "VIRTUAL ACCOUNT", "E-MONEY", "TOTAL VA + E-MONEY", "BCA", "NON BCA", "TOTAL BCA + NON BCA"]
    return out.sort_values(["Pelabuhan", "Tanggal"]).reset_index(drop=True)[desired]

# --- (SELURUH BAGIAN LAIN file Anda: FINNET, RK loaders, summary, UI, export) ---
# Tidak diubah; pastikan fungsi di atas menggantikan versi lama.

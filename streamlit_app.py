# path: streamlit_app.py
# Rekonsiliasi Payment/Settlement/RK – cepat, hemat RAM, UI stabil

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

# ---------------- Streamlit config & safety ----------------
st.set_page_config(page_title="Rekonsiliasi Payment Report", layout="wide")
st.set_option("client.showErrorDetails", True)

# ---------------- Konstanta ----------------
COL_H   = "TIPE PEMBAYARAN"
COL_B   = "TANGGAL PEMBAYARAN"
COL_AA  = "REF NO"
COL_K   = "TOTAL TARIF TANPA BIAYA ADMIN (Rp.)"
COL_X   = "SOF ID"
COL_ASAL= "ASAL"
REQUIRED_COLS = [COL_H, COL_B, COL_AA, COL_K, COL_X, COL_ASAL]

CAT_COLS = [
    "Cash","Prepaid BRI","Prepaid BNI","Prepaid Mandiri","Prepaid BCA",
    "SKPT","IFCS","Reedem","ESPAY","Finnet",
]
NON_COMPONENTS = ["Cash","Prepaid BRI","Prepaid BNI","Prepaid Mandiri","Prepaid BCA","SKPT","IFCS","Reedem"]

DEFAULT_CSV_CHUNK_ROWS = 200_000
XLSX_BATCH_ROWS = 50_000
NONBCA_CREDIT_COL_INDEX = 9

_HAS_PYARROW = False
try:
    import pyarrow  # noqa
    _HAS_PYARROW = True
except Exception:
    pass

# ---------------- Utilities ----------------
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

def _to_numeric_rupiah(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.replace(r"[^\d\-]", "", regex=True)
    out = pd.to_numeric(x, errors="coerce").fillna(0.0)
    return out

# ---------------- CSV chunk iterator ----------------
def _iter_csv_chunks(file_like: Union[io.BytesIO, io.BufferedReader], usecols: List[str], year: int, month: int,
                     chunksize: int = DEFAULT_CSV_CHUNK_ROWS) -> Iterable[pd.DataFrame]:
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

# ---------------- Payment loaders ----------------
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

def _process_csv_fast(data: bytes, year: int, month: int, agg, chunk_rows: int) -> None:
    buf = io.BytesIO(data)
    for sub in _iter_csv_chunks(buf, REQUIRED_COLS, year, month, chunksize=chunk_rows):
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

# ---------------- Settlement ESPAY (xlsx: Amount - Tx Fee) ----------------
def _pick_col(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    if df is None or df.empty:
        return None
    norm_map = {_norm_colname(c): c for c in df.columns}
    alias_norms = [_norm_colname(a) for a in aliases]
    for a in alias_norms:
        if a in norm_map: return norm_map[a]
    for a in alias_norms:
        for n, real in norm_map.items():
            if n.startswith(a) or a.startswith(n):
                return real
    for a in alias_norms:
        for n, real in norm_map.items():
            if a in n or n in a:
                return real
    return None

def _read_settlement_single_table(content: bytes, filename: str) -> Optional[pd.DataFrame]:
    low = str(filename).lower()
    is_xlsx = low.endswith(".xlsx")
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
        elif is_xlsx:
            df = pd.read_excel(io.BytesIO(content), engine="openpyxl")
        else:
            return None
    except Exception:
        return None

    if df is None or df.empty:
        return None
    df.rename(columns={c: str(c).strip() for c in df.columns}, inplace=True)

    # Map kolom wajib (kanonik)
    col_product = _pick_col(df, ["Product Name","Channel","Product","Payment Channel","Payment Method","Method"])
    col_date    = _pick_col(df, ["Settlement Date","settlement_date","Tanggal","Date"])
    col_va      = _pick_col(df, ["VA NAME","VA Name","va_name","Merchant_name","Merchant name","Merchant Name"])

    if col_product is None or col_date is None or col_va is None:
        return None

    # Settlement Amount (rupiah):
    amt_series = None
    if is_xlsx:
        c_amt = _pick_col(df, ["Amount","Amt","Total Amount"])
        c_fee = _pick_col(df, ["Tx Fee","Fee","Tx_Fee","Tx-Fee","Transaction Fee"])
        if c_amt and c_fee:
            amt_series = _to_numeric_rupiah(df[c_amt]) - _to_numeric_rupiah(df[c_fee])  # rupiah
        else:
            # fallback ke kolom existing
            c_net = _pick_col(df, ["Settlement Amount","Amount - Tx Fee","Net Amount"])
            if c_net:
                amt_series = _to_numeric_rupiah(df[c_net])
    else:
        c_net = _pick_col(df, ["Amount - Tx Fee","Settlement Amount","Net Amount"])
        if c_net is not None:
            raw = _to_numeric_rupiah(df[c_net])
            # heuristik: jika sebagian besar > 0 dan banyak kelipatan 100 → cent → konversi ke rupiah
            if (raw.abs() > 0).mean() > 0 and (raw % 100 == 0).mean() > 0.7:
                amt_series = (raw / 100.0)
            else:
                amt_series = raw

    if amt_series is None:
        return None

    out = pd.DataFrame({
        "Product Name": df[col_product],
        "Settlement Date": df[col_date],
        "VA NAME": df[col_va],
        "Settlement Amount": amt_series,  # sudah rupiah
    })
    return out

def _load_settlement_espay(files) -> pd.DataFrame:
    all_dfs: List[pd.DataFrame] = []
    for f in files or []:
        try: data = f.getvalue() if hasattr(f, "getvalue") else f.read()
        except Exception: continue
        try:
            df_part = _read_settlement_single_table(data, f.name)
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

    # Settlement Amount sudah rupiah dari loader
    amt = pd.to_numeric(df["Settlement Amount"], errors="coerce").fillna(0.0)

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

# ---------------- Settlement FINNET ----------------
def _read_finnet_single_csv(content: bytes) -> Optional[pd.DataFrame]:
    buf = io.BytesIO(content)
    head = buf.read(2048); buf.seek(0)
    delim = _sniff_delimiter(head)
    try:
        if _HAS_PYARROW:
            return pd.read_csv(buf, sep=delim, engine="pyarrow")
        try:
            return pd.read_csv(buf, sep=delim, encoding="utf-8-sig", engine="python", on_bad_lines="skip")
        except UnicodeDecodeError:
            buf.seek(0)
            return pd.read_csv(buf, sep=delim, encoding="latin1", engine="python", on_bad_lines="skip")
    except Exception:
        return None

def _load_settlement_finnet(files) -> pd.DataFrame:
    all_dfs: List[pd.DataFrame] = []
    for f in files or []:
        try:
            data = f.getvalue() if hasattr(f, "getvalue") else f.read()
        except Exception:
            continue
        name = f.name.lower()
        try:
            if name.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for info in zf.infolist():
                        if info.is_dir(): continue
                        if not info.filename.lower().endswith(".csv"): continue
                        try:
                            with zf.open(info, "r") as zh:
                                part = _read_finnet_single_csv(zh.read())
                        except Exception:
                            part = None
                        if part is not None:
                            all_dfs.append(part)
            elif name.endswith(".csv"):
                part = _read_finnet_single_csv(data)
                if part is not None:
                    all_dfs.append(part)
        except Exception:
            continue
    if not all_dfs:
        return pd.DataFrame()
    df = pd.concat(all_dfs, ignore_index=True)
    df.rename(columns={c: str(c).strip() for c in df.columns}, inplace=True)
    norm_cols = {c: _norm_colname(c) for c in df.columns}
    rename_map = {}
    for req in ["Payment Method","Merchant Amount","Payment Date Time","Merchant Name"]:
        req_norm = _norm_colname(req)
        for real, nm in norm_cols.items():
            if nm == req_norm or nm.startswith(req_norm) or req_norm.startswith(nm):
                rename_map[real] = req
                break
    df.rename(columns=rename_map, inplace=True)
    return df

def _build_finnet_settlement_table(df_finnet: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    if df_finnet is None or df_finnet.empty: return pd.DataFrame()
    df = df_finnet.copy()
    needed = ["Payment Date Time","Merchant Amount","Merchant Name","Payment Method"]
    if any(c not in df.columns for c in needed): return pd.DataFrame()

    raw_dt = df["Payment Date Time"].astype(str).str.strip()
    t = pd.to_datetime(raw_dt.str.slice(0, 10), errors="coerce")
    df["Tanggal"] = t.dt.date
    df = df.loc[(t.dt.year == year) & (t.dt.month == month)].copy()
    if df.empty: return pd.DataFrame()

    mn = df["Merchant Name"].fillna("").astype(str).str.upper()
    def map_pelabuhan(name: str) -> str:
        if "BAKAUHENI" in name: return "ASDP Bakauheni"
        if "GILIMANUK" in name: return "ASDP Gilimanuk"
        if "KETAPANG" in name:  return "ASDP Ketapang"
        if "MERAK" in name:     return "ASDP Merak"
        return "ASDP Lainnya"
    df["Pelabuhan"] = mn.apply(map_pelabuhan)

    amt = pd.to_numeric(df["Merchant Amount"].astype(str).str.replace(r"[^\d\-]", "", regex=True), errors="coerce").fillna(0.0)
    pm = df["Payment Method"].fillna("").astype(str).str.lower()
    is_va = pm.str.contains("va", na=False)
    is_bca = pm.str.contains("bca", na=False) | pm.str.contains("blu", na=False)
    is_non_bca = ~(pm.str.contains("bca", na=False) | pm.str.contains("blu", na=False))

    df["VIRTUAL ACCOUNT"] = amt.where(is_va, 0.0)
    df["E-MONEY"] = amt.where(~is_va, 0.0)
    df["BCA"] = amt.where(is_bca, 0.0)
    df["NON BCA"] = amt.where(is_non_bca, 0.0)

    grouped = df.groupby(["Tanggal","Pelabuhan"], dropna=False)[["VIRTUAL ACCOUNT","E-MONEY","BCA","NON BCA"]].sum().reset_index()
    for c in ["VIRTUAL ACCOUNT","E-MONEY","BCA","NON BCA"]:
        grouped[c] = grouped[c].fillna(0.0)

    unique_ports = grouped["Pelabuhan"].dropna().unique()
    if len(unique_ports) == 0: return pd.DataFrame()
    days = monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days+1)]
    full_idx = pd.MultiIndex.from_product([all_dates, unique_ports], names=["Tanggal","Pelabuhan"])
    out = pd.DataFrame(index=full_idx).reset_index().merge(grouped, on=["Tanggal","Pelabuhan"], how="left")
    for c in ["VIRTUAL ACCOUNT","E-MONEY","BCA","NON BCA"]:
        out[c] = out[c].fillna(0.0)
    out["TOTAL VA + E-MONEY"] = out["VIRTUAL ACCOUNT"] + out["E-MONEY"]
    out["TOTAL BCA + NON BCA"] = out["BCA"] + out["NON BCA"]
    desired = ["Tanggal","Pelabuhan","VIRTUAL ACCOUNT","E-MONEY","TOTAL VA + E-MONEY","BCA","NON BCA","TOTAL BCA + NON BCA"]
    return out.sort_values(["Pelabuhan","Tanggal"]).reset_index(drop=True)[desired]

# ---------------- RK Loaders ----------------
def _read_any_table_with_header(content: bytes, filename: str, header_row: int) -> Optional[pd.DataFrame]:
    skiprows = range(0, max(header_row - 1, 0))
    low = str(filename).lower()
    ext = low.rsplit(".", 1)[-1] if "." in low else ""
    try:
        if ext in {"xlsx","xlsm"}:
            return pd.read_excel(io.BytesIO(content), engine="openpyxl", skiprows=skiprows, header=0)
        if ext == "xls":
            return pd.read_excel(io.BytesIO(content), engine="xlrd", skiprows=skiprows, header=0)
        if ext == "xlsb":
            return pd.read_excel(io.BytesIO(content), engine="pyxlsb", skiprows=skiprows, header=0)
        buf = io.BytesIO(content)
        head = buf.read(2048); buf.seek(0)
        delim = _sniff_delimiter(head)
        if _HAS_PYARROW:
            return pd.read_csv(buf, skiprows=skiprows, header=0, engine="pyarrow", sep=delim)
        try:
            return pd.read_csv(buf, skiprows=skiprows, header=0, encoding="utf-8-sig", sep=delim, engine="python", on_bad_lines="skip")
        except UnicodeDecodeError:
            buf.seek(0)
            return pd.read_csv(buf, skiprows=skiprows, header=0, encoding="latin1", sep=delim, engine="python", on_bad_lines="skip")
    except Exception:
        return None

def _read_bca_table_row2(content: bytes) -> Optional[pd.DataFrame]:
    df = None
    for eng in ("openpyxl","xlrd","pyxlsb",None):
        try:
            df = pd.read_excel(io.BytesIO(content), engine=eng, header=0) if eng else pd.read_excel(io.BytesIO(content), header=0)
            break
        except Exception:
            df = None
    if df is None:
        try:
            if _HAS_PYARROW:
                df = pd.read_csv(io.BytesIO(content), header=0, engine="pyarrow")
            else:
                text = content.decode("utf-8-sig", "ignore")
                df = pd.read_csv(io.StringIO(text), header=0)
        except Exception:
            return None
    return df if (df is not None and not df.empty) else None

def _load_rk_bca_generic(files, keys: List[str], port_from_name) -> Dict[Tuple[date, str], float]:
    totals: Dict[Tuple[date, str], float] = defaultdict(float)
    if not files: return {}
    def extract(df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if df is None or df.empty: return None
        cols = list(df.columns); norm = {c: _norm_colname(c) for c in cols}
        c_tgl = next((c for c in cols if norm[c] in {"tanggal","date","transactiondate","tgl"} or "tanggal" in norm[c] or "date" in norm[c]), None)
        c_ket = next((c for c in cols if any(k in norm[c] for k in ["keterangan","remark","description","deskripsi"])), None)
        c_amt = next((c for c in cols if norm[c] in {"mutasi","credit","kredit","amount","nominal"} or norm[c]=="mutasi"), None)
        if not (c_tgl and c_ket and c_amt): return None
        t = pd.to_datetime(df[c_tgl], errors="coerce", dayfirst=True)
        sub = pd.DataFrame({"Tanggal": t.dt.date, "Keterangan": df[c_ket].astype(str), "Amount": _parse_amount_credit_series(df[c_amt]).astype("float64")})
        sub = sub[sub["Tanggal"].notna()]
        sub = sub[_mask_remark_contains(sub["Keterangan"], keys)]
        return sub[["Tanggal","Amount"]] if not sub.empty else None
    def handle_one(content: bytes, fname: str):
        port = port_from_name(fname)
        df = _read_bca_table_row2(content)
        if df is None or df.empty: return
        part = extract(df)
        if part is None or part.empty: return
        for dt_val, amt in part.groupby("Tanggal")["Amount"].sum().items():
            totals[(dt_val, port)] += float(amt)
    for f in files or []:
        try: data = f.getvalue() if hasattr(f, "getvalue") else f.read()
        except Exception: continue
        fname = f.name
        try:
            if fname.lower().endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for info in zf.infolist():
                        if info.is_dir(): continue
                        if not info.filename.lower().endswith((".xlsx",".xls",".xlsb",".csv")): continue
                        try: handle_one(zf.read(info), info.filename)
                        except Exception: continue
            else:
                handle_one(data, fname)
        except Exception:
            continue
    return dict(totals)

def _load_rk_bca_sgw_by_dt_port(files):   return _load_rk_bca_generic(files, ["SGW"], _port_from_bca_filename)
def _load_rk_bca_finif_by_dt_port(files): return _load_rk_bca_generic(files, ["FINIF","FINON"], _port_from_bca_filename)

def _load_rk_nonbca_generic(files, header_row: int, keys: List[str]) -> Dict[Tuple[date, str], float]:
    if not files: return {}
    totals: Dict[Tuple[date, str], float] = defaultdict(float)
    def handle_one(content: bytes, fname: str):
        port = _port_from_filename(fname)
        df = _read_any_table_with_header(content, fname, header_row)
        if df is None or df.empty or df.shape[1] <= NONBCA_CREDIT_COL_INDEX: return
        c_date = next((c for c in df.columns if _norm_colname(c) in {"date","tanggal","transactiondate","tgl"}), None)
        c_remark = next((c for c in df.columns if any(k in _norm_colname(c) for k in ["remark","keterangan","description","deskripsi"])), None)
        if not c_date or not c_remark: return
        sub = pd.DataFrame({
            "Tanggal": _to_date_minus1(df[c_date]).dt.date,
            "Remark": df[c_remark].astype(str),
            "Amount": _parse_amount_credit_series(df.iloc[:, NONBCA_CREDIT_COL_INDEX]).astype("float64"),
        })
        sub = sub[sub["Tanggal"].notna()]
        sub = sub[_mask_remark_contains(sub["Remark"], keys)]
        if sub.empty: return
        for dt_val, amt in sub.groupby("Tanggal")["Amount"].sum().items():
            totals[(dt_val, _canonical_port_name(port))] += float(amt)
    for f in files or []:
        try: data = f.getvalue() if hasattr(f, "getvalue") else f.read()
        except Exception: continue
        name = f.name
        if str(name).lower().endswith(".zip"):
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for info in zf.infolist():
                        if info.is_dir(): continue
                        if not info.filename.lower().endswith((".csv",".xls",".xlsx",".xlsb")): continue
                        try: handle_one(zf.read(info), info.filename)
                        except Exception: continue
            except Exception:
                continue
        else:
            handle_one(data, name)
    return dict(totals)

def _load_rk_nonbca_inflow_by_dt_port_from_files(files, header_row: int):     return _load_rk_nonbca_generic(files, header_row, ["FINIF","FINON"])
def _load_rk_nonbca_inflow_by_dt_port_from_files_sgw(files, header_row: int): return _load_rk_nonbca_generic(files, header_row, ["SGW"])

# ---------------- Gabungan KTP+GLM helper ----------------
def _append_ketapang_gilimanuk_combined(df: pd.DataFrame, final_cols: list) -> pd.DataFrame:
    if df is None or df.empty or "Pelabuhan" not in df.columns or "Tanggal" not in df.columns:
        return df
    ports_src = ["ASDP Ketapang","ASDP Gilimanuk"]
    mask = df["Pelabuhan"].isin(ports_src)
    if not mask.any():
        return df
    numeric_cols = [c for c in final_cols if c not in ("Tanggal","Pelabuhan") and c in df.columns]
    grouped = df.loc[mask].groupby("Tanggal", as_index=False)[numeric_cols].sum()
    if grouped.empty:
        return df
    grouped.insert(1, "Pelabuhan", "ASDP Gilimanuk + ASDP Ketapang")
    out = pd.concat([df, grouped[final_cols]], ignore_index=True)
    return out.sort_values(["Pelabuhan","Tanggal"]).reset_index(drop=True)

# ---------------- Tabel Rekonsiliasi ----------------
def _build_finnet_rekon_table(
    agg,
    df_finnet_settlement: Optional[pd.DataFrame],
    year: int,
    month: int,
    bca_inflow_by_dt_port: Optional[Dict[Tuple[date, str], float]] = None,
    nonbca_inflow_by_dt_port: Optional[Dict[Tuple[date, str], float]] = None,
) -> pd.DataFrame:
    ports_from_payment = {_canonical_port_name(asal) for (_, asal) in agg.keys() if asal is not None}
    ports_from_settle = set()
    if df_finnet_settlement is not None and not df_finnet_settlement.empty and "Pelabuhan" in df_finnet_settlement.columns:
        ports_from_settle = set(df_finnet_settlement["Pelabuhan"].dropna().apply(_canonical_port_name).unique())
    unique_ports = sorted(ports_from_payment.union(ports_from_settle))
    if not unique_ports: return pd.DataFrame()

    days_in_month = monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days_in_month+1)]
    base_idx = pd.MultiIndex.from_product([all_dates, unique_ports], names=["Tanggal","Pelabuhan"])
    base_df = pd.DataFrame(index=base_idx).reset_index()

    rows = []
    for (dt, asal), bucket in agg.items():
        asal_norm = _canonical_port_name(asal)
        if asal_norm not in unique_ports: continue
        dt_val = dt if isinstance(dt, date) else pd.to_datetime(dt, errors="coerce").date()
        if dt_val is None or dt_val.year != year or dt_val.month != month: continue
        bca_val = float(bucket.get("FINNET_TIKET_BCA", 0.0))
        non_bca_val = float(bucket.get("FINNET_TIKET_NON_BCA", 0.0))
        if bca_val == 0.0 and non_bca_val == 0.0: continue
        rows.append({"Tanggal": dt_val, "Pelabuhan": asal_norm, "Tiket_BCA": bca_val, "Tiket_NON_BCA": non_bca_val})

    ticket_df = (pd.DataFrame(rows).groupby(["Tanggal","Pelabuhan"], as_index=False)[["Tiket_BCA","Tiket_NON_BCA"]].sum()
                 if rows else pd.DataFrame(columns=["Tanggal","Pelabuhan","Tiket_BCA","Tiket_NON_BCA"]))

    if df_finnet_settlement is not None and not df_finnet_settlement.empty:
        need = [c for c in ["Tanggal","Pelabuhan","BCA","NON BCA"] if c in df_finnet_settlement.columns]
        settle_df = (df_finnet_settlement[need].copy() if len(need)==4 else pd.DataFrame(columns=["Tanggal","Pelabuhan","BCA","NON BCA"]))
        if not settle_df.empty:
            settle_df["Pelabuhan"] = settle_df["Pelabuhan"].apply(_canonical_port_name)
            settle_df = settle_df.groupby(["Tanggal","Pelabuhan"], as_index=False)[["BCA","NON BCA"]].sum()
    else:
        settle_df = pd.DataFrame(columns=["Tanggal","Pelabuhan","BCA","NON BCA"])

    out = base_df.copy()
    if not ticket_df.empty: out = out.merge(ticket_df, on=["Tanggal","Pelabuhan"], how="left")
    if not settle_df.empty: out = out.merge(settle_df, on=["Tanggal","Pelabuhan"], how="left")

    for c in ["Tiket_BCA","Tiket_NON_BCA","BCA","NON BCA"]:
        if c not in out.columns: out[c] = 0.0
        else: out[c] = out[c].fillna(0.0)

    out["Tiket Detail - BCA"] = out["Tiket_BCA"]
    out["Tiket Detail - Non BCA"] = out["Tiket_NON_BCA"]
    out["Settlement Report - BCA"] = out["BCA"]
    out["Settlement Report - Non BCA"] = out["NON BCA"]

    bca_map = bca_inflow_by_dt_port or {}
    nonbca_map = nonbca_inflow_by_dt_port or {}
    out["Dana Masuk - BCA"] = out.apply(lambda r: float(bca_map.get((r["Tanggal"], _canonical_port_name(r["Pelabuhan"])), 0.0)), axis=1)
    out["Dana Masuk - Non BCA"] = out.apply(lambda r: float(nonbca_map.get((r["Tanggal"], _canonical_port_name(r["Pelabuhan"])), 0.0)), axis=1)

    out["Total Tiket Detail"] = out["Tiket Detail - BCA"] + out["Tiket Detail - Non BCA"]
    out["Total Settlement Report"] = out["Settlement Report - BCA"] + out["Settlement Report - Non BCA"]
    out["Total Dana Masuk"] = out["Dana Masuk - BCA"] + out["Dana Masuk - Non BCA"]
    out["Selisih Tiket Detail vs Settlement Report"] = out["Total Tiket Detail"] - out["Total Settlement Report"]
    out["Selisih Dana Masuk vs Settlement Report"] = out["Total Dana Masuk"] - out["Total Settlement Report"]

    final_cols = [
        "Tanggal","Pelabuhan",
        "Tiket Detail - BCA","Tiket Detail - Non BCA","Total Tiket Detail",
        "Settlement Report - BCA","Settlement Report - Non BCA","Total Settlement Report",
        "Dana Masuk - BCA","Dana Masuk - Non BCA","Total Dana Masuk",
        "Selisih Tiket Detail vs Settlement Report","Selisih Dana Masuk vs Settlement Report",
    ]
    out = out.sort_values(["Pelabuhan","Tanggal"]).reset_index(drop=True)[final_cols]
    out = _append_ketapang_gilimanuk_combined(out, final_cols)
    return out

def _build_espay_rekon_table(
    agg,
    df_espay_settlement: Optional[pd.DataFrame],
    year: int,
    month: int,
    bca_inflow_by_dt_port_sgw: Optional[Dict[Tuple[date, str], float]] = None,
    nonbca_inflow_by_dt_port_sgw: Optional[Dict[Tuple[date, str], float]] = None,
) -> pd.DataFrame:
    ports_from_payment = {_canonical_port_name(asal) for (_, asal) in agg.keys() if asal is not None}
    ports_from_settle = set()
    if df_espay_settlement is not None and not df_espay_settlement.empty and "Pelabuhan" in df_espay_settlement.columns:
        ports_from_settle = set(df_espay_settlement["Pelabuhan"].dropna().apply(_canonical_port_name).unique())
    unique_ports = sorted(ports_from_payment.union(ports_from_settle))
    if not unique_ports: return pd.DataFrame()

    days_in_month = monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days_in_month+1)]
    base_idx = pd.MultiIndex.from_product([all_dates, unique_ports], names=["Tanggal","Pelabuhan"])
    base_df = pd.DataFrame(index=base_idx).reset_index()

    rows = []
    for (dt, asal), bucket in agg.items():
        asal_norm = _canonical_port_name(asal)
        if asal_norm not in unique_ports: continue
        dt_val = dt if isinstance(dt, date) else pd.to_datetime(dt, errors="coerce").date()
        if dt_val is None or dt_val.year != year or dt_val.month != month: continue
        bca_val = float(bucket.get("ESPAY_TIKET_BCA", 0.0))
        non_bca_val = float(bucket.get("ESPAY_TIKET_NON_BCA", 0.0))
        if bca_val == 0.0 and non_bca_val == 0.0: continue
        rows.append({"Tanggal": dt_val, "Pelabuhan": asal_norm, "Tiket_BCA": bca_val, "Tiket_NON_BCA": non_bca_val})

    ticket_df = (pd.DataFrame(rows).groupby(["Tanggal","Pelabuhan"], as_index=False)[["Tiket_BCA","Tiket_NON_BCA"]].sum()
                 if rows else pd.DataFrame(columns=["Tanggal","Pelabuhan","Tiket_BCA","Tiket_NON_BCA"]))

    if df_espay_settlement is not None and not df_espay_settlement.empty:
        need = [c for c in ["Tanggal","Pelabuhan","BCA","NON BCA"] if c in df_espay_settlement.columns]
        settle_df = (df_espay_settlement[need].copy() if len(need)==4 else pd.DataFrame(columns=["Tanggal","Pelabuhan","BCA","NON BCA"]))
        if not settle_df.empty:
            settle_df["Pelabuhan"] = settle_df["Pelabuhan"].apply(_canonical_port_name)
            settle_df = settle_df.groupby(["Tanggal","Pelabuhan"], as_index=False)[["BCA","NON BCA"]].sum()
    else:
        settle_df = pd.DataFrame(columns=["Tanggal","Pelabuhan","BCA","NON BCA"])

    out = base_df.copy()
    if not ticket_df.empty: out = out.merge(ticket_df, on=["Tanggal","Pelabuhan"], how="left")
    if not settle_df.empty: out = out.merge(settle_df, on=["Tanggal","Pelabuhan"], how="left")

    for c in ["Tiket_BCA","Tiket_NON_BCA","BCA","NON BCA"]:
        if c not in out.columns: out[c] = 0.0
        else: out[c] = out[c].fillna(0.0)

    out["Tiket Detail - BCA"] = out["Tiket_BCA"]
    out["Tiket Detail - Non BCA"] = out["Tiket_NON_BCA"]
    out["Settlement Report - BCA"] = out["BCA"]
    out["Settlement Report - Non BCA"] = out["NON BCA"]

    bca_map = bca_inflow_by_dt_port_sgw or {}
    nonbca_map = nonbca_inflow_by_dt_port_sgw or {}
    out["Dana Masuk - BCA"] = out.apply(
        lambda r: float(bca_map.get((r["Tanggal"], _canonical_port_name(r["Pelabuhan"])), 0.0)), axis=1
    )
    out["Dana Masuk - Non BCA"] = out.apply(
        lambda r: float(nonbca_map.get((r["Tanggal"], _canonical_port_name(r["Pelabuhan"])), 0.0)), axis=1
    )

    out["Total Tiket Detail"] = out["Tiket Detail - BCA"] + out["Tiket Detail - Non BCA"]
    out["Total Settlement Report"] = out["Settlement Report - BCA"] + out["Settlement Report - Non BCA"]
    out["Total Dana Masuk"] = out["Dana Masuk - BCA"] + out["Dana Masuk - Non BCA"]
    out["Selisih Tiket Detail vs Settlement Report"] = out["Total Tiket Detail"] - out["Total Settlement Report"]
    out["Selisih Dana Masuk vs Settlement Report"] = out["Total Dana Masuk"] - out["Total Settlement Report"]

    final_cols = [
        "Tanggal","Pelabuhan",
        "Tiket Detail - BCA","Tiket Detail - Non BCA","Total Tiket Detail",
        "Settlement Report - BCA","Settlement Report - Non BCA","Total Settlement Report",
        "Dana Masuk - BCA","Dana Masuk - Non BCA","Total Dana Masuk",
        "Selisih Tiket Detail vs Settlement Report","Selisih Dana Masuk vs Settlement Report",
    ]
    out = out.sort_values(["Pelabuhan","Tanggal"]).reset_index(drop=True)[final_cols]
    out = _append_ketapang_gilimanuk_combined(out, final_cols)
    return out

# ---------------- Summary (3 baris) ----------------
def _count_tx_finnet(df_raw: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=["Pelabuhan","Jumlah Transaksi"])
    need = ["Payment Date Time","Merchant Name"]
    if any(c not in df_raw.columns for c in need):
        return pd.DataFrame(columns=["Pelabuhan","Jumlah Transaksi"])
    raw_dt = df_raw["Payment Date Time"].astype(str).str.strip()
    t = pd.to_datetime(raw_dt.str.slice(0, 10), errors="coerce")
    df = pd.DataFrame({"Tanggal": t.dt.date, "Merchant Name": df_raw["Merchant Name"].astype(str)})
    df = df.loc[(t.dt.year == year) & (t.dt.month == month)]
    if df.empty:
        return pd.DataFrame(columns=["Pelabuhan","Jumlah Transaksi"])
    mn = df["Merchant Name"].fillna("").str.upper()
    def map_port(name: str) -> str:
        if "BAKAUHENI" in name: return "ASDP Bakauheni"
        if "GILIMANUK" in name: return "ASDP Gilimanuk"
        if "KETAPANG" in name:  return "ASDP Ketapang"
        if "MERAK" in name:     return "ASDP Merak"
        return "ASDP Lainnya"
    ports = mn.apply(map_port)
    return ports.value_counts().rename_axis("Pelabuhan").rename("Jumlah Transaksi").reset_index()

def _count_tx_espay(df_raw: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=["Pelabuhan","Jumlah Transaksi"])
    need = ["Settlement Date","VA NAME"]
    if any(c not in df_raw.columns for c in need):
        return pd.DataFrame(columns=["Pelabuhan","Jumlah Transaksi"])
    t = pd.to_datetime(df_raw["Settlement Date"], errors="coerce")
    df = pd.DataFrame({"Tanggal": t.dt.date, "VA NAME": df_raw["VA NAME"].astype(str)})
    df = df.loc[(t.dt.year == year) & (t.dt.month == month)]
    if df.empty:
        return pd.DataFrame(columns=["Pelabuhan","Jumlah Transaksi"])
    def map_port(name: str) -> Optional[str]:
        n = str(name).upper()
        if "BAKAUHENI" in n: return "ASDP Bakauheni"
        if "GILIMANUK" in n: return "ASDP Gilimanuk"
        if "KETAPANG" in n:  return "ASDP Ketapang"
        if "MERAK" in n:     return "ASDP Merak"
        return None
    ports = df["VA NAME"].map(map_port)
    return ports.dropna().value_counts().rename_axis("Pelabuhan").rename("Jumlah Transaksi").reset_index()

def _build_summary_table_filtered(df_rekon: pd.DataFrame, df_counts: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    if df_rekon is None or df_rekon.empty:
        return pd.DataFrame()
    totals = df_rekon.groupby("Pelabuhan", as_index=False)[["Total Settlement Report","Total Tiket Detail"]].sum()

    def get_total(port: str) -> Tuple[float, float]:
        sub = totals[totals["Pelabuhan"] == port]
        inc = float(sub["Total Settlement Report"].sum()) if not sub.empty else 0.0
        exc = float(sub["Total Tiket Detail"].sum()) if not sub.empty else 0.0
        return inc, exc

    counts_map: Dict[str, int] = {}
    if df_counts is not None and not df_counts.empty and "Pelabuhan" in df_counts.columns:
        for _, r in df_counts.iterrows():
            counts_map[str(r["Pelabuhan"])] = int(r.get("Jumlah Transaksi", 0))
    def get_count(port: str) -> int:
        return int(counts_map.get(port, 0))

    periode = _period_label(year, month)
    rows = []

    inc_bak, exc_bak = get_total("ASDP Bakauheni")
    rows.append({"Periode": periode, "Cabang": "ASDP Bakauheni", "Jumlah Transaksi": get_count("ASDP Bakauheni"),
                 "Nominal Transaksi (inc fee)": inc_bak, "Nominal Transaksi (exc fee)": exc_bak})

    inc_g, exc_g = get_total("ASDP Gilimanuk")
    inc_k, exc_k = get_total("ASDP Ketapang")
    rows.append({"Periode": periode, "Cabang": "ASDP Gilimanuk + Ketapang",
                 "Jumlah Transaksi": get_count("ASDP Gilimanuk") + get_count("ASDP Ketapang"),
                 "Nominal Transaksi (inc fee)": inc_g + inc_k,
                 "Nominal Transaksi (exc fee)": exc_g + exc_k})

    inc_m, exc_m = get_total("ASDP Merak")
    rows.append({"Periode": periode, "Cabang": "ASDP Merak", "Jumlah Transaksi": get_count("ASDP Merak"),
                 "Nominal Transaksi (inc fee)": inc_m, "Nominal Transaksi (exc fee)": exc_m})

    out = pd.DataFrame(rows)
    subtotal = {
        "Periode": periode,
        "Cabang": "Total",
        "Jumlah Transaksi": out["Jumlah Transaksi"].sum(),
        "Nominal Transaksi (inc fee)": out["Nominal Transaksi (inc fee)"].sum(),
        "Nominal Transaksi (exc fee)": out["Nominal Transaksi (exc fee)"].sum(),
    }
    out = pd.concat([out, pd.DataFrame([subtotal])], ignore_index=True)

    out.columns = pd.MultiIndex.from_tuples([
        ("", "Periode"),
        ("", "Cabang"),
        ("Menu Payment Ferizy", "Jumlah Transaksi"),
        ("Menu Payment Ferizy", "Nominal Transaksi (inc fee)"),
        ("Menu Payment Ferizy", "Nominal Transaksi (exc fee)"),
    ])
    return out

# ---------------- UI helpers & Excel export ----------------
def _safe_sheetname(name: str) -> str:
    s = re.sub(r"[:\\/?*\[\]]", "-", str(name))
    return s[:31] if len(s) > 31 else s

def _short_port(port: str) -> str:
    p = re.sub(r"^ASDP\s+", "", str(port).strip(), flags=re.I)
    return p.upper()

def _prep_numeric_dates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Tanggal" in out.columns:
        out["Tanggal"] = pd.to_datetime(out["Tanggal"], errors="coerce")
    num_cols = out.select_dtypes(include="number").columns
    out[num_cols] = out[num_cols].fillna(0).round(0).astype("Int64")
    return out

def _to_excel_workbook_bytes(df_payment: pd.DataFrame, df_rekon_finnet: pd.DataFrame, df_rekon_espay: pd.DataFrame,
                             df_summary_finnet: pd.DataFrame, df_summary_espay: pd.DataFrame,
                             year: int, month: int):
    wrote = False
    for engine in ("xlsxwriter", "openpyxl"):
        try:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine=engine) as writer:
                if df_summary_finnet is not None and not df_summary_finnet.empty:
                    df_summary_finnet.to_excel(writer, sheet_name=_safe_sheetname("Summary - Finnet"), index=False, merge_cells=True)
                    wrote = True
                if df_summary_espay is not None and not df_summary_espay.empty:
                    df_summary_espay.to_excel(writer, sheet_name=_safe_sheetname("Summary - Espay"), index=False, merge_cells=True)
                    wrote = True

                if df_payment is not None and not df_payment.empty:
                    for p in sorted([x for x in df_payment["Pelabuhan"].dropna().unique() if "+" not in str(x)]):
                        dfp = _prep_numeric_dates(df_payment[df_payment["Pelabuhan"] == p])
                        sheet = _safe_sheetname(f"Payment - {_short_port(p)}")
                        dfp.to_excel(writer, sheet_name=sheet, index=False)
                        try: writer.sheets[sheet].freeze_panes(1, 0)
                        except Exception: pass
                        wrote = True

                if df_rekon_finnet is not None and not df_rekon_finnet.empty:
                    for p in sorted([x for x in df_rekon_finnet["Pelabuhan"].dropna().unique() if "+" not in str(x)]):
                        dff = _prep_numeric_dates(df_rekon_finnet[df_rekon_finnet["Pelabuhan"] == p])
                        sheet = _safe_sheetname(f"Finnet - {_short_port(p)}")
                        dff.to_excel(writer, sheet_name=sheet, index=False)
                        try: writer.sheets[sheet].freeze_panes(1, 0)
                        except Exception: pass
                        wrote = True

                if df_rekon_espay is not None and not df_rekon_espay.empty:
                    for p in sorted([x for x in df_rekon_espay["Pelabuhan"].dropna().unique() if "+" not in str(x)]):
                        dfe = _prep_numeric_dates(df_rekon_espay[df_rekon_espay["Pelabuhan"] == p])
                        sheet = _safe_sheetname(f"Espay - {_short_port(p)}")
                        dfe.to_excel(writer, sheet_name=sheet, index=False)
                        try: writer.sheets[sheet].freeze_panes(1, 0)
                        except Exception: pass
                        wrote = True

                if not wrote:
                    pd.DataFrame({"Info": ["Tidak ada data untuk diekspor"]}).to_excel(
                        writer, sheet_name="Empty", index=False
                    )
            return buf.getvalue(), engine, None
        except ImportError:
            continue
        except Exception as e:
            return None, None, f"Gagal menulis Excel ({engine}): {e}"
    return None, None, "Tidak ada engine Excel (xlsxwriter/openpyxl)."

def _render_df(df_show: pd.DataFrame, highlight: bool, max_rows_style: int = 1500) -> None:
    df_show = df_show.copy()
    df_show["Tanggal"] = pd.to_datetime(df_show["Tanggal"]).dt.strftime("%d/%m/%Y")
    df_show = _add_subtotal_row(df_show, label="Subtotal", date_col="Tanggal")
    numeric_cols = df_show.select_dtypes(include="number").columns
    df_show[numeric_cols] = df_show[numeric_cols].fillna(0).round(0).astype("Int64")
    if len(df_show) <= max_rows_style and highlight:
        try:
            st.dataframe(_style_table(df_show, True), use_container_width=True, height=520)
            return
        except Exception:
            pass
    st.dataframe(df_show, use_container_width=True, height=520)

def _render_summary(df_sum: pd.DataFrame):
    if df_sum is None or df_sum.empty:
        st.info("Summary kosong.")
        return
    try:
        sty = df_sum.style.format({
            ("Menu Payment Ferizy","Jumlah Transaksi"): "{:,.0f}",
            ("Menu Payment Ferizy","Nominal Transaksi (inc fee)"): "{:,.0f}",
            ("Menu Payment Ferizy","Nominal Transaksi (exc fee)"): "{:,.0f}",
        })
        st.dataframe(sty, use_container_width=True)
    except Exception:
        st.dataframe(df_sum, use_container_width=True)

# ---------------- MAIN ----------------
def main() -> None:
    st.title("Rekonsiliasi Payment Report")
    st.sidebar.success("Upload file lalu klik ▶️ Mulai Proses. Tiap uploader diproses mandiri.")

    today = date.today()
    years_options = list(range(today.year - 5, today.year + 6))
    year = st.sidebar.selectbox("Tahun", options=years_options, index=years_options.index(today.year))
    month_names = {
        1:"01 - Januari",2:"02 - Februari",3:"03 - Maret",4:"04 - April",
        5:"05 - Mei",6:"06 - Juni",7:"07 - Juli",8:"08 - Agustus",
        9:"09 - September",10:"10 - Oktober",11:"11 - November",12:"12 - Desember",
    }
    month = st.sidebar.selectbox("Bulan", options=list(range(1,13)), index=today.month-1,
                                 format_func=lambda m: month_names[m])

    st.sidebar.markdown("### ⚙️ Kinerja")
    max_workers = st.sidebar.slider("Parallel workers (antar file)", 1, 4, 2)
    chunk_rows = st.sidebar.number_input("CSV chunk rows", min_value=50_000, step=50_000, value=DEFAULT_CSV_CHUNK_ROWS)
    st.sidebar.caption(("pyarrow ✔️" if _HAS_PYARROW else "pyarrow ❌"))

    ss_get_set("upload_rev", 0)
    if st.sidebar.button("🔄 Reset semua upload"):
        st.session_state.upload_rev += 1

    up_files = st.sidebar.file_uploader(
        "Upload Payment Report: ZIP / Excel (.xlsx/.xls/.xlsb) / CSV",
        type=["zip","xlsx","xls","xlsb","csv"], accept_multiple_files=True,
        key=f"payment_{st.session_state.upload_rev}",
    )
    settlement_files = st.sidebar.file_uploader(
        "Upload Settlement ESPAY (.xlsx / .csv)",
        type=["xlsx","csv"], accept_multiple_files=True,
        key=f"settlement_espay_{st.session_state.upload_rev}",
    )
    finnet_files = st.sidebar.file_uploader(
        "Upload Settlement Finnet by Telkom (ZIP / .csv)", type=["zip","csv"], accept_multiple_files=True,
        key=f"settlement_finnet_{st.session_state.upload_rev}",
    )
    finnet_espay_files = st.sidebar.file_uploader(
        "Upload Settlement Finnet (ESPAY) (ZIP / .csv)", type=["zip","csv"], accept_multiple_files=True,
        key=f"settlement_finnet_espay_{st.session_state.upload_rev}",
    )
    rek_bca_files = st.sidebar.file_uploader(
        "Upload Rekening Koran BCA", type=["zip","xlsx","xls","xlsb","csv"], accept_multiple_files=True,
        key=f"rek_bca_{st.session_state.upload_rev}",
    )
    rek_nonbca_files = st.sidebar.file_uploader(
        "Upload Rekening Koran Non BCA", type=["zip","xlsx","xls","xlsb","csv"], accept_multiple_files=True,
        key=f"rek_nonbca_{st.session_state.upload_rev}",
    )

    with st.sidebar.expander("🔧 Diagnostic"):
        st.session_state["diag_mode"] = st.checkbox("Aktifkan Diagnostic Mode", value=st.session_state.get("diag_mode", False))

    highlight = st.sidebar.checkbox("Highlight Selisih ≠ 0", value=True)

    ss_get_set("run_started", False)
    ss_get_set("results", {})

    col_btn = st.columns([1, 1, 1, 6])
    if col_btn[0].button("▶️ Mulai Proses", type="primary"):
        st.session_state.run_started = True
        st.session_state.results = {}
    if col_btn[1].button("🔁 Proses Ulang"):
        st.session_state.run_started = True
        st.session_state.results = {}
    if col_btn[2].button("🧹 Bersihkan Hasil"):
        st.session_state.run_started = False
        st.session_state.results = {}

    if not st.session_state.run_started:
        st.info("Unggah berkas apa saja, lalu klik **▶️ Mulai Proses**. Tiap step tetap bekerja meski step sebelumnya kosong.")
        return

    progress = st.progress(0)
    results = st.session_state.results

    # --- 1) Payment (opsional)
    progress.progress(5)
    if "payment" not in results:
        with st.spinner("1/5 • Memproses Payment Report (jika ada)…"):
            if up_files:
                agg = load_and_aggregate_fast(up_files, year, month, max_workers, chunk_rows)
                results["agg"] = agg
                results["payment"] = _build_result_from_agg(agg)
            else:
                results["agg"] = _empty_agg()
                results["payment"] = pd.DataFrame()
    df_payment = results["payment"]

    st.subheader(f"1) Hasil Rekonsiliasi Payment • {month_names[month]} {year}")
    if df_payment.empty:
        st.info("Payment Report tidak diupload / kosong. Lanjut ke settlement.")
    else:
        ports = sorted(df_payment["Pelabuhan"].dropna().unique())
        if len(ports) <= 6:
            tabs = st.tabs(ports)
            for tab, port in zip(tabs, ports):
                with tab:
                    st.markdown(f"**Pelabuhan: {port}**")
                    _render_df(df_payment[df_payment["Pelabuhan"] == port], highlight=False)
        else:
            chosen = st.selectbox("Pilih Pelabuhan (Payment)", ports, key="pay_sel")
            _render_df(df_payment[df_payment["Pelabuhan"] == chosen], highlight=False)
    progress.progress(25)

    # --- 2) Settlement ESPAY
    st.divider(); st.subheader("2) DETAIL SETTLEMENT ESPAY")
    if "espay" not in results:
        with st.spinner("2/5 • Memproses Settlement ESPAY (jika ada)…"):
            if settlement_files:
                raw = _load_settlement_espay(settlement_files)
                results["espay_raw"] = raw
                results["espay"] = _build_espay_settlement_table(raw, year, month)
            else:
                results["espay_raw"] = pd.DataFrame()
                results["espay"] = pd.DataFrame()
    df_espay = results["espay"]
    if df_espay.empty:
        st.info("Settlement ESPAY kosong / tidak diupload / periode tidak cocok.")
    else:
        ports_espay = sorted(df_espay["Pelabuhan"].dropna().unique())
        if len(ports_espay) <= 6:
            tabs_espay = st.tabs(ports_espay)
            for tab, port in zip(tabs_espay, ports_espay):
                with tab:
                    st.markdown(f"**Pelabuhan: {port}**")
                    _render_df(df_espay[df_espay["Pelabuhan"] == port], highlight=False)
        else:
            chosen = st.selectbox("Pilih Pelabuhan (ESPAY)", ports_espay, key="espay_sel")
            _render_df(df_espay[df_espay["Pelabuhan"] == chosen], highlight=False)
    progress.progress(45)

    # --- 3) FINNET by Telkom
    st.divider(); st.subheader("3) DETAIL SETTLEMENT FINNET BY TELKOM")
    if "finnet_telkom" not in results:
        with st.spinner("3/5 • Memproses Settlement FINNET (Telkom) (jika ada)…"):
            if finnet_files:
                raw = _load_settlement_finnet(finnet_files)
                results["finnet_telkom_raw"] = raw
                results["finnet_telkom"] = _build_finnet_settlement_table(raw, year, month)
            else:
                results["finnet_telkom_raw"] = pd.DataFrame()
                results["finnet_telkom"] = pd.DataFrame()
    df_finnet = results["finnet_telkom"]
    if df_finnet.empty:
        st.info("Settlement FINNET (Telkom) kosong / tidak diupload / periode tidak cocok.")
    else:
        ports_finnet = sorted(df_finnet["Pelabuhan"].dropna().unique())
        if len(ports_finnet) <= 6:
            tabs_finnet = st.tabs(ports_finnet)
            for tab, port in zip(tabs_finnet, ports_finnet):
                with tab:
                    st.markdown(f"**Pelabuhan: {port}**")
                    _render_df(df_finnet[df_finnet["Pelabuhan"] == port], highlight=False)
        else:
            chosen = st.selectbox("Pilih Pelabuhan (Finnet Telkom)", ports_finnet, key="finnet_telkom_sel")
            _render_df(df_finnet[df_finnet["Pelabuhan"] == chosen], highlight=False)
    progress.progress(65)

    # --- 4) FINNET (ESPAY)
    st.divider(); st.subheader("4) DETAIL SETTLEMENT FINNET (ESPAY)")
    if "finnet_espay" not in results:
        with st.spinner("4/5 • Memproses Settlement FINNET (ESPAY) (jika ada)…"):
            if finnet_espay_files:
                raw = _load_settlement_finnet(finnet_espay_files)
                results["finnet_espay_raw"] = raw
                results["finnet_espay"] = _build_finnet_settlement_table(raw, year, month)
            else:
                results["finnet_espay_raw"] = pd.DataFrame()
                results["finnet_espay"] = pd.DataFrame()
    df_finnet_espay = results["finnet_espay"]
    if df_finnet_espay.empty:
        st.info("Settlement FINNET (ESPAY) kosong / tidak diupload / periode tidak cocok.")
    else:
        ports_fe = sorted(df_finnet_espay["Pelabuhan"].dropna().unique())
        if len(ports_fe) <= 6:
            tabs_fe = st.tabs(ports_fe)
            for tab, port in zip(tabs_fe, ports_fe):
                with tab:
                    st.markdown(f"**Pelabuhan: {port}**")
                    _render_df(df_finnet_espay[df_finnet_espay["Pelabuhan"] == port], highlight=False)
        else:
            chosen = st.selectbox("Pilih Pelabuhan (Finnet Espay)", ports_fe, key="finnet_espay_sel")
            _render_df(df_finnet_espay[df_finnet_espay["Pelabuhan"] == chosen], highlight=False)
    progress.progress(80)

    # --- 5) Rekonsiliasi Gabungan
    st.divider()
    st.subheader("5) TABEL REKONSILIASI GABUNGAN PAYMENT - SETTLEMENT DANA - REKENING KORAN")
    agg = results.get("agg", _empty_agg())

    st.markdown("**1. Rekonsiliasi Finnet**")
    bca_finif = _load_rk_bca_finif_by_dt_port(rek_bca_files) if rek_bca_files else {}
    nonbca_finif = _load_rk_nonbca_inflow_by_dt_port_from_files(rek_nonbca_files, header_row=13) if rek_nonbca_files else {}
    df_rekon_finnet = _build_finnet_rekon_table(
        agg, results.get("finnet_telkom", pd.DataFrame()), year, month,
        bca_inflow_by_dt_port=bca_finif, nonbca_inflow_by_dt_port=nonbca_finif,
    )
    if df_rekon_finnet.empty:
        st.info("Tabel Rekonsiliasi Finnet belum dapat dibentuk.")
    else:
        ports_rekon = sorted(df_rekon_finnet["Pelabuhan"].dropna().unique())
        if len(ports_rekon) <= 6:
            tabs_rekon = st.tabs(ports_rekon)
            for tab, label in zip(tabs_rekon, ports_rekon):
                with tab:
                    st.markdown(f"**Pelabuhan: {label}**")
                    _render_df(df_rekon_finnet[df_rekon_finnet["Pelabuhan"] == label], highlight=highlight)
        else:
            chosen = st.selectbox("Pilih Pelabuhan (Rekon Finnet)", ports_rekon, key="rekon_finnet_sel")
            _render_df(df_rekon_finnet[df_rekon_finnet["Pelabuhan"] == chosen], highlight=highlight)

    st.markdown("**2. Rekonsiliasi ESPAY**")
    bca_sgw = _load_rk_bca_sgw_by_dt_port(rek_bca_files) if rek_bca_files else {}
    nonbca_sgw = _load_rk_nonbca_inflow_by_dt_port_from_files_sgw(rek_nonbca_files, header_row=13) if rek_nonbca_files else {}
    df_rekon_espay = _build_espay_rekon_table(
        agg, results.get("espay", pd.DataFrame()), year, month,
        bca_inflow_by_dt_port_sgw=bca_sgw, nonbca_inflow_by_dt_port_sgw=nonbca_sgw,
    )
    if df_rekon_espay.empty:
        st.info("Tabel Rekonsiliasi ESPAY belum dapat dibentuk.")
    else:
        ports_rekon_espay = sorted(df_rekon_espay["Pelabuhan"].dropna().unique())
        if len(ports_rekon_espay) <= 6:
            tabs_rekon_espay = st.tabs(ports_rekon_espay)
            for tab, label in zip(tabs_rekon_espay, ports_rekon_espay):
                with tab:
                    st.markdown(f"**Pelabuhan: {label}**")
                    _render_df(df_rekon_espay[df_rekon_espay["Pelabuhan"] == label], highlight=highlight)
        else:
            chosen = st.selectbox("Pilih Pelabuhan (Rekon ESPAY)", ports_rekon_espay, key="rekon_espay_sel")
            _render_df(df_rekon_espay[df_rekon_espay["Pelabuhan"] == chosen], highlight=highlight)

    # --- Summary
    st.divider(); st.subheader("TABEL SUMMARY REKONSILIASI")
    with st.expander("Summary • FINNET", expanded=True):
        finnet_counts = _count_tx_finnet(results.get("finnet_telkom_raw", pd.DataFrame()), year, month)
        sum_finnet = _build_summary_table_filtered(df_rekon_finnet, finnet_counts, year, month)
        _render_summary(sum_finnet)
    with st.expander("Summary • ESPAY", expanded=True):
        espay_counts = _count_tx_espay(results.get("espay_raw", pd.DataFrame()), year, month)
        sum_espay = _build_summary_table_filtered(df_rekon_espay, espay_counts, year, month)
        _render_summary(sum_espay)

    progress.progress(100)

    # --- Unduh (Excel saja)
    st.divider(); st.subheader("Unduh Hasil (Excel per Pelabuhan / per Sheet + Summary)")
    excel_bytes, engine_used, err_msg = _to_excel_workbook_bytes(
        results.get("payment", pd.DataFrame()),
        df_rekon_finnet, df_rekon_espay,
        sum_finnet if 'sum_finnet' in locals() else pd.DataFrame(),
        sum_espay if 'sum_espay' in locals() else pd.DataFrame(),
        year, month
    )
    if excel_bytes:
        st.download_button(
            f"Unduh Excel (.xlsx) • engine: {engine_used}",
            data=excel_bytes,
            file_name=f"rekap_rekonsiliasi_{year}_{month:02d}_per_pelabuhan.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.warning("Ekspor Excel gagal. Tambahkan `xlsxwriter` atau `openpyxl`."
                   + (f"\nDetail: {err_msg}" if err_msg else ""))

# --------- Run with hard guard ---------
if __name__ == "__main__":
    try:
        main()
    except BaseException as e:
        st.error("Aplikasi error saat render awal. Rincian di bawah:")
        st.exception(e)

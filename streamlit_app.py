# path: streamlit_app.py
import io, re, csv, zipfile
from datetime import date
from calendar import monthrange
from collections import defaultdict, OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple, Dict, Iterable

import pandas as pd
import streamlit as st
from openpyxl import load_workbook

# ---------- Streamlit ----------
st.set_page_config(page_title="Rekonsiliasi Payment Report", layout="wide")
st.set_option("client.showErrorDetails", True)

# ---------- Konstanta ----------
COL_H = "TIPE PEMBAYARAN"
COL_B = "TANGGAL PEMBAYARAN"
COL_AA = "REF NO"
COL_K = "TOTAL TARIF TANPA BIAYA ADMIN (Rp.)"
COL_X = "SOF ID"
COL_ASAL = "ASAL"
REQUIRED_COLS = [COL_H, COL_B, COL_AA, COL_K, COL_X, COL_ASAL]

CAT_COLS = ["Cash","Prepaid BRI","Prepaid BNI","Prepaid Mandiri","Prepaid BCA","SKPT","IFCS","Reedem","ESPAY","Finnet"]
NON_COMPONENTS = ["Cash","Prepaid BRI","Prepaid BNI","Prepaid Mandiri","Prepaid BCA","SKPT","IFCS","Reedem"]

DEFAULT_CSV_CHUNK_ROWS = 200_000
XLSX_BATCH_ROWS = 50_000
NONBCA_CREDIT_COL_INDEX = 9

_HAS_PYARROW = False
try:
    import pyarrow  # noqa
    _HAS_PYARROW = True
except Exception:
    _HAS_PYARROW = False

# ---------- Utils ----------
def ss_get_set(key: str, default):
    if key not in st.session_state:
        st.session_state[key] = default
    return st.session_state[key]

def _style_table(df_display: pd.DataFrame, highlight: bool):
    numeric_cols = df_display.select_dtypes(include="number").columns.tolist()
    styler = df_display.style.format("{:,.0f}", subset=numeric_cols)
    if highlight:
        for col in ["Selisih Tiket Detail vs Settlement Report","Selisih Dana Masuk vs Settlement Report","Selisih"]:
            if col in df_display.columns:
                styler = styler.apply(
                    lambda s: ["background-color:#fdecea; color:#b71c1c; font-weight:600;" if (pd.notna(v) and float(v) != 0) else "" for v in s],
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
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch.lower())
    return "".join(out)

def _canonical_port_name(name: Optional[str]) -> str:
    if name is None: return "Tidak diketahui"
    s = str(name).strip(); up = s.upper()
    if "BAKAUHENI" in up: return "ASDP Bakauheni"
    if "GILIMANUK" in up: return "ASDP Gilimanuk"
    if "KETAPANG"  in up: return "ASDP Ketapang"
    if "MERAK"     in up: return "ASDP Merak"
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
    for k in keywords:
        kk = str(k).upper()
        mask = mask | norm.str.contains(kk, na=False)
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
    if "MERAK" in up: return "ASDP Merak"
    if "BAKAUHENI" in up: return "ASDP Bakauheni"
    if "GILIMANUK" in up: return "ASDP Gilimanuk"
    if "KETAPANG" in up: return "ASDP Ketapang"
    return "ASDP Lainnya"

def _port_from_bca_filename(fname: str) -> str:
    up = str(fname).upper()
    if "MERAK" in up: return "ASDP Merak"
    if ("BEKAUHENI" in up) or ("BAKAUHENI" in up): return "ASDP Bakauheni"
    if "KETAPANG" in up: return "ASDP Bakauheni"  # sesuai instruksi sebelumnya
    if "GILIMANUK" in up: return "ASDP Gilimanuk"
    return "ASDP Lainnya"

def _period_label(y: int, m: int) -> str:
    return pd.Timestamp(y, m, 1).strftime("%b-%y")

# ---------- Payment: header mapping fleksibel ----------
def _find_inc_colname(columns: List[str]) -> Optional[str]:
    if not columns: return None
    targets = {"totaltarif","totaltarifrp","total_tarif","total tarif"}
    for c in columns:
        n = _norm_colname(c)
        if (n in targets) or ("totaltarif" in n):
            return c
    return None

def _resolve_payment_headers(columns: Iterable[str]) -> Dict[str, str]:
    if not columns: return {}
    aliases = {
        COL_H: {"tipepembayaran","tipe_pembayaran","jenis pembayaran","jenispembayaran","paymenttype","payment_type"},
        COL_B: {"tanggalpembayaran","tanggal_pembayaran","paymentdate","payment_date","tglpembayaran","tgl_pembayaran","tanggalbayar","tglbayar"},
        COL_AA: {"refno","ref_no","referenceno","reference_no","nomorreferensi","noreferensi","reference"},
        COL_K: {"totaltariftanpabiayaadminrp","totaltariftanpabiayaadmin","total_tarif_tanpa_biaya_admin","nominaltransaksiexcfee","nominal_transaksi_exc_fee","excfee","exc_fee","amount_exc"},
        COL_X: {"sofid","sof_id","sourceoffund","source_of_fund","sof"},
        COL_ASAL: {"asal","pelabuhan","cabang","origin","lokasi","lokasi_asal","asalpelabuhan"},
    }
    real_by_norm = {}
    for c in columns:
        n = _norm_colname(c)
        if n and n not in real_by_norm:
            real_by_norm[n] = str(c)
    mapping = {}
    for need, alias_set in aliases.items():
        found = None
        for a in alias_set:
            if a in real_by_norm:
                found = real_by_norm[a]; break
        if not found:
            for n, real in real_by_norm.items():
                for a in alias_set:
                    if (n == a) or n.startswith(a) or (a in n):
                        found = real; break
                if found: break
        if found:
            mapping[found] = need
    return mapping

def _apply_header_mapping_df(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    if not mapping: return df
    return df.rename(columns=mapping)

# ---------- Payment: aggregator ----------
def _empty_agg(): return defaultdict(lambda: defaultdict(float))

def _update_agg_series(agg, ser: pd.Series, colname: str) -> None:
    if ser is None or ser.empty: return
    for (dt, asal), val in ser.items():
        agg[(dt, asal)][colname] += float(val)

def _apply_rules_and_update(df_chunk: pd.DataFrame, agg) -> None:
    H = df_chunk[COL_H].fillna("").astype(str).str.lower()
    AA = df_chunk[COL_AA].fillna("").astype(str).str.lower()
    X  = df_chunk[COL_X].fillna("").astype(str).str.lower()
    ASAL = df_chunk[COL_ASAL].fillna("Tidak diketahui").astype(str).str.strip()

    amt_exc = pd.to_numeric(df_chunk[COL_K], errors="coerce").fillna(0)
    amt_inc = pd.to_numeric(df_chunk.get("AMT_INC", 0), errors="coerce").fillna(0)
    tgl = df_chunk["Tanggal"]

    def sum_by(series: pd.Series, mask: pd.Series) -> pd.Series:
        if mask.any():
            return series[mask].groupby([tgl[mask], ASAL[mask]], dropna=False).sum(min_count=1)
        return pd.Series(dtype="float64", index=pd.MultiIndex.from_arrays([[], []], names=["Tanggal","Pelabuhan"]))

    def cnt_by(mask: pd.Series) -> pd.Series:
        if mask.any():
            k = pd.DataFrame({"tgl": tgl[mask], "asal": ASAL[mask], "one": 1})
            return k.groupby(["tgl","asal"], dropna=False)["one"].sum()
        return pd.Series(dtype="float64", index=pd.MultiIndex.from_arrays([[], []], names=["Tanggal","Pelabuhan"]))

    rules = OrderedDict([
        ("Cash", H.str.contains("cash", na=False)),
        ("Prepaid BRI", H.str.contains("prepaid-bri", na=False)),
        ("Prepaid BNI", H.str.contains("prepaid-bni", na=False)),
        ("Prepaid Mandiri", H.str.contains("prepaid-mandiri", na=False)),
        ("Prepaid BCA", H.str.contains("prepaid-bca", na=False)),
        ("SKPT", H.str.contains("skpt", na=False)),
        ("IFCS", H.str.contains("ifcs", na=False)),
        ("Reedem", H.str.contains("reedem|redeem", na=False)),
        ("ESPAY", H.str.contains("finpay", na=False) & AA.str.startswith("esp", na=False)),
        ("Finnet", H.str.contains("finpay", na=False) & (~AA.str.startswith("esp", na=False))),
    ])
    for name, m in rules.items():
        _update_agg_series(agg, sum_by(amt_exc, m), name)

    is_finpay = H.str.contains("finpay", na=False)
    is_bca_tag = X.str.contains("vabcaespay|bluespay", na=False)
    _update_agg_series(agg, sum_by(amt_exc, is_finpay & is_bca_tag), "BCA")
    _update_agg_series(agg, sum_by(amt_exc, is_finpay & (~is_bca_tag)), "NON BCA")

    is_not_spay = ~X.str.contains("spay", na=False)
    is_bca = X.str.contains("bca", na=False)
    m1 = (is_finpay & is_not_spay & is_bca)
    m2 = (is_finpay & is_not_spay & (~is_bca))
    _update_agg_series(agg, sum_by(amt_exc, m1), "FINNET_TIKET_BCA")
    _update_agg_series(agg, sum_by(amt_exc, m2), "FINNET_TIKET_NON_BCA")
    _update_agg_series(agg, cnt_by(m1), "FINNET_TIKET_BCA_CNT")
    _update_agg_series(agg, cnt_by(m2), "FINNET_TIKET_NON_BCA_CNT")
    _update_agg_series(agg, sum_by(amt_inc, (is_finpay & is_not_spay)), "FINNET_INC")

    is_spay = X.str.contains("spay", na=False)
    m3 = (is_spay & is_bca_tag)
    m4 = (is_spay & (~is_bca_tag))
    _update_agg_series(agg, sum_by(amt_exc, m3), "ESPAY_TIKET_BCA")
    _update_agg_series(agg, sum_by(amt_exc, m4), "ESPAY_TIKET_NON_BCA")
    _update_agg_series(agg, cnt_by(m3), "ESPAY_TIKET_BCA_CNT")
    _update_agg_series(agg, cnt_by(m4), "ESPAY_TIKET_NON_BCA_CNT")
    _update_agg_series(agg, sum_by(amt_inc, is_spay), "ESPAY_INC")

def _build_result_from_agg(agg) -> pd.DataFrame:
    if not agg: return pd.DataFrame()
    rows = []
    for (dt, asal), bucket in agg.items():
        row = {"Tanggal": dt, "Pelabuhan": asal}
        for c in CAT_COLS: row[c] = bucket.get(c, 0.0)
        row["Total"] = sum(row[c] for c in CAT_COLS)
        bca = bucket.get("BCA", 0.0); nonbca = bucket.get("NON BCA", 0.0)
        row["BCA"] = bca; row["NON BCA"] = nonbca
        row["NON"] = sum(row[c] for c in NON_COMPONENTS)
        row["TOTAL"] = bca + nonbca + row["NON"]
        row["Selisih"] = row["TOTAL"] - row["Total"]
        rows.append(row)
    df = pd.DataFrame(rows)
    if df.empty: return df
    df = df[["Tanggal","Pelabuhan"] + CAT_COLS + ["Total","BCA","NON BCA","NON","TOTAL","Selisih"]]
    return df.sort_values(["Pelabuhan","Tanggal"]).reset_index(drop=True)

# ---------- Payment: readers ----------
def _process_csv_fast(data: bytes, year: int, month: int, agg, chunk_rows: int) -> None:
    buf = io.BytesIO(data)
    head = buf.read(4096); buf.seek(0)
    delim = _sniff_delimiter(head)

    try:
        if _HAS_PYARROW:
            cols_df = pd.read_csv(buf, nrows=0, sep=delim, engine="pyarrow")
        else:
            cols_df = pd.read_csv(buf, nrows=0, sep=delim, engine="python")
    except Exception:
        buf.seek(0)
        cols_df = pd.read_csv(buf, nrows=0, sep=delim, engine="python")
    buf.seek(0)

    header_map = _resolve_payment_headers(list(cols_df.columns))
    have_all = header_map and all(v in header_map.values() for v in REQUIRED_COLS)
    if not have_all: return

    inc_col_real = _find_inc_colname(list(cols_df.columns))
    real_usecols = list(header_map.keys()) + ([inc_col_real] if inc_col_real else [])

    kwargs = dict(
        sep=delim, usecols=real_usecols, chunksize=chunk_rows, encoding="utf-8-sig",
        on_bad_lines="skip", engine="python"
    )
    for chunk in pd.read_csv(buf, **kwargs):
        chunk = _apply_header_mapping_df(chunk, header_map)
        if inc_col_real and inc_col_real in chunk.columns:
            chunk["AMT_INC"] = pd.to_numeric(chunk[inc_col_real], errors="coerce").fillna(0)
        t = pd.to_datetime(chunk[COL_B], errors="coerce", dayfirst=True)
        mask = (t.dt.year == year) & (t.dt.month == month)
        if not mask.any(): continue
        keep_cols = [c for c in REQUIRED_COLS if c in chunk.columns]
        if "AMT_INC" in chunk.columns: keep_cols.append("AMT_INC")
        sub = chunk.loc[mask, keep_cols].copy()
        sub["Tanggal"] = t.loc[mask].dt.date
        _apply_rules_and_update(sub, agg)

def _flush_xlsx_batch(buf_rows: List[List], year: int, month: int, agg, has_inc: bool) -> None:
    cols = [COL_H, COL_B, COL_AA, COL_K, COL_X, COL_ASAL] + (["AMT_INC"] if has_inc else [])
    df = pd.DataFrame(buf_rows, columns=cols)
    if has_inc:
        df["AMT_INC"] = pd.to_numeric(df["AMT_INC"], errors="coerce").fillna(0)
    t = pd.to_datetime(df[COL_B], errors="coerce", dayfirst=True)
    mask = (t.dt.year == year) & (t.dt.month == month)
    if not mask.any(): return
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

        header_map = _resolve_payment_headers([str(h).strip() if h is not None else "" for h in header])
        have_all = header_map and all(v in header_map.values() for v in REQUIRED_COLS)
        if not have_all:
            wb.close(); return

        name_to_idx = {str(h).strip(): i for i, h in enumerate(header) if h is not None}
        inc_col_real = _find_inc_colname([str(h) for h in header if h is not None])
        inc_idx = name_to_idx[inc_col_real] if (inc_col_real and inc_col_real in name_to_idx) else None
        has_inc = inc_idx is not None

        idx_H   = name_to_idx[[k for k, v in header_map.items() if v == COL_H][0]]
        idx_B   = name_to_idx[[k for k, v in header_map.items() if v == COL_B][0]]
        idx_AA  = name_to_idx[[k for k, v in header_map.items() if v == COL_AA][0]]
        idx_K   = name_to_idx[[k for k, v in header_map.items() if v == COL_K][0]]
        idx_X   = name_to_idx[[k for k, v in header_map.items() if v == COL_X][0]]
        idx_ASL = name_to_idx[[k for k, v in header_map.items() if v == COL_ASAL][0]]

        buf_rows = []
        for r in rows:
            try:
                row = [r[idx_H], r[idx_B], r[idx_AA], r[idx_K], r[idx_X], r[idx_ASL]]
                if has_inc: row.append(r[inc_idx])
                buf_rows.append(row)
            except Exception:
                continue
            if len(buf_rows) >= XLSX_BATCH_ROWS:
                _flush_xlsx_batch(buf_rows, year, month, agg, has_inc); buf_rows.clear()

        if buf_rows:
            _flush_xlsx_batch(buf_rows, year, month, agg, has_inc); buf_rows.clear()
        wb.close()
    except Exception:
        try:
            df = pd.read_excel(io.BytesIO(data), sheet_name=0)
        except Exception:
            return
        if df is None or df.empty: return
        header_map = _resolve_payment_headers(list(df.columns))
        have_all = header_map and all(v in header_map.values() for v in REQUIRED_COLS)
        if not have_all: return
        df = _apply_header_mapping_df(df, header_map)
        inc_col_real = _find_inc_colname(list(df.columns))
        if inc_col_real:
            df["AMT_INC"] = pd.to_numeric(df[inc_col_real], errors="coerce").fillna(0)
        t = pd.to_datetime(df[COL_B], errors="coerce", dayfirst=True)
        mask = (t.dt.year == year) & (t.dt.month == month)
        if not mask.any(): return
        keep_cols = [c for c in REQUIRED_COLS if c in df.columns]
        if "AMT_INC" in df.columns: keep_cols.append("AMT_INC")
        sub = df.loc[mask, keep_cols].copy()
        sub["Tanggal"] = t.loc[mask].dt.date
        _apply_rules_and_update(sub, agg)

def _process_xlsb(data: bytes, year: int, month: int, agg) -> None:
    try:
        df = pd.read_excel(io.BytesIO(data), sheet_name=0, engine="pyxlsb")
    except Exception:
        return
    if df is None or df.empty: return
    header_map = _resolve_payment_headers(list(df.columns))
    have_all = header_map and all(v in header_map.values() for v in REQUIRED_COLS)
    if not have_all: return
    df = _apply_header_mapping_df(df, header_map)
    t = pd.to_datetime(df[COL_B], errors="coerce", dayfirst=True)
    mask = (t.dt.year == year) & (t.dt.month == month)
    if not mask.any(): return
    sub = df.loc[mask, [c for c in REQUIRED_COLS if c in df.columns]].copy()
    sub["Tanggal"] = t.loc[mask].dt.date
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
                    if not fn.endswith((".xlsx",".xls",".xlsb",".csv")): continue
                    content = zf.read(info)
                    if fn.endswith((".xlsx",".xls")): _process_xlsx_streaming(content, year, month, agg)
                    elif fn.endswith(".xlsb"): _process_xlsb(content, year, month, agg)
                    else: _process_csv_fast(content, year, month, agg, chunk_rows)
        elif low.endswith((".xlsx",".xls")): _process_xlsx_streaming(data, year, month, agg)
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
        if key not in target: target[key] = dict(bucket)
        else:
            for k2, v2 in bucket.items():
                target[key][k2] = float(target[key].get(k2, 0.0)) + float(v2)

def load_and_aggregate_fast(files, year: int, month: int, max_workers: int, csv_chunk_rows: int) -> dict:
    merged_plain = {}
    if not files: return defaultdict(lambda: defaultdict(float))
    def submit_one(executor, f):
        try:
            data = f.getvalue() if hasattr(f, "getvalue") else f.read()
        except Exception:
            data = b""
        name = getattr(f, "name", "file")
        return executor.submit(_process_payment_file, name, data, year, month, csv_chunk_rows)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [submit_one(ex, f) for f in files]
        for fut in as_completed(futs):
            try: _merge_plain_aggs(merged_plain, fut.result() or {})
            except Exception: continue
    agg = defaultdict(lambda: defaultdict(float))
    for key, bucket in merged_plain.items(): agg[key].update(bucket)
    return agg

# ---------- Settlement ESPAY ----------
def _pick_col(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    if df is None or df.empty: return None
    norm_map = {_norm_colname(c): c for c in df.columns}
    alias_norms = [_norm_colname(a) for a in aliases]
    for a in alias_norms:
        if a in norm_map: return norm_map[a]
    for a in alias_norms:
        for n, real in norm_map.items():
            if n.startswith(a) or a.startswith(n): return real
    for a in alias_norms:
        for n, real in norm_map.items():
            if a in n or n in a: return real
    return None

def _read_settlement_single_table(content: bytes, filename: str) -> Optional[pd.DataFrame]:
    low = str(filename).lower()
    is_xlsx = low.endswith(".xlsx")
    try:
        if low.endswith(".csv"):
            buf = io.BytesIO(content)
            head = buf.read(2048); buf.seek(0)
            delim = _sniff_delimiter(head)
            if _HAS_PYARROW: df = pd.read_csv(buf, sep=delim, engine="pyarrow")
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
    if df is None or df.empty: return None
    df.rename(columns={c: str(c).strip() for c in df.columns}, inplace=True)

    col_product = _pick_col(df, ["Product Name","Channel","Product","Payment Channel","Payment Method","Method"])
    col_date    = _pick_col(df, ["Settlement Date","settlement_date","Tanggal","Date"])
    col_va      = _pick_col(df, ["VA NAME","VA Name","va_name","Merchant_name","Merchant name","Merchant Name"])
    if col_product is None or col_date is None or col_va is None:
        return None

    net_series = None
    c_amt = _pick_col(df, ["Amount","Amt","Total Amount"])
    c_fee = _pick_col(df, ["Tx Fee","Fee","Tx_Fee","Tx-Fee","Transaction Fee","MDR Fee"])
    if c_amt is not None and c_fee is not None:
        amt = _parse_money_series(df[c_amt]); fee = _parse_money_series(df[c_fee])
        net_series = (amt - fee)
    else:
        c_net = _pick_col(df, ["Amount - Tx Fee","Settlement Amount","Net Amount"])
        if c_net is not None: net_series = _parse_money_series(df[c_net])
    if net_series is None: return None

    if not is_xlsx:
        try:
            ser = pd.Series(net_series).fillna(0)
            cond = (ser != 0).mean() > 0 and (ser % 100 == 0).mean() > 0.8 and ser.median() > 10_000
            if cond: net_series = ser / 100.0
        except Exception:
            pass

    out = pd.DataFrame({
        "Product Name": df[col_product],
        "Settlement Date": df[col_date],
        "VA NAME": df[col_va],
        "Settlement Amount": pd.to_numeric(net_series, errors="coerce").fillna(0.0),
    })
    return out

def _load_settlement_espay(files) -> pd.DataFrame:
    all_dfs = []
    for f in files or []:
        try:
            data = f.getvalue() if hasattr(f, "getvalue") else f.read()
        except Exception:
            continue
        try:
            df_part = _read_settlement_single_table(data, f.name)
            if df_part is not None and not df_part.empty:
                all_dfs.append(df_part)
        except Exception:
            continue
    if not all_dfs: return pd.DataFrame()
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

    amt = pd.to_numeric(df["Settlement Amount"], errors="coerce").fillna(0.0)
    pn = df["Product Name"].fillna("").astype(str).str.lower()
    is_va  = pn.str.contains("va", na=False)
    is_bca = pn.str.contains("bca", na=False) | pn.str.contains("blu", na=False)

    df["VIRTUAL ACCOUNT"] = amt.where(is_va, 0.0)
    df["E-MONEY"] = amt.where(~is_va, 0.0)
    df["BCA"] = amt.where(is_bca, 0.0)
    df["NON BCA"] = amt.where(~is_bca, 0.0)

    grouped = df.groupby(["Tanggal","Pelabuhan"], dropna=False)[["VIRTUAL ACCOUNT","E-MONEY","BCA","NON BCA"]].sum().reset_index()
    for c in ["VIRTUAL ACCOUNT","E-MONEY","BCA","NON BCA"]:
        grouped[c] = grouped[c].fillna(0.0)

    unique_ports = grouped["Pelabuhan"].dropna().unique()
    if len(unique_ports) == 0: return pd.DataFrame()
    days_in_month = monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days_in_month + 1)]

    full_idx = pd.MultiIndex.from_product([all_dates, unique_ports], names=["Tanggal","Pelabuhan"])
    out = pd.DataFrame(index=full_idx).reset_index().merge(grouped, on=["Tanggal","Pelabuhan"], how="left")
    for c in ["VIRTUAL ACCOUNT","E-MONEY","BCA","NON BCA"]:
        out[c] = out[c].fillna(0.0)
    out["TOTAL VA + E-MONEY"] = out["VIRTUAL ACCOUNT"] + out["E-MONEY"]
    out["TOTAL BCA + NON BCA"] = out["BCA"] + out["NON BCA"]
    desired = ["Tanggal","Pelabuhan","VIRTUAL ACCOUNT","E-MONEY","TOTAL VA + E-MONEY","BCA","NON BCA","TOTAL BCA + NON BCA"]
    return out.sort_values(["Pelabuhan","Tanggal"]).reset_index(drop=True)[desired]

# ---------- Settlement FINNET (CSV/ZIP) ----------
def _read_finnet_single_csv(content: bytes) -> Optional[pd.DataFrame]:
    buf = io.BytesIO(content)
    head = buf.read(2048); buf.seek(0)
    delim = _sniff_delimiter(head)
    try:
        if _HAS_PYARROW: return pd.read_csv(buf, sep=delim, engine="pyarrow")
        try:
            return pd.read_csv(buf, sep=delim, encoding="utf-8-sig", engine="python", on_bad_lines="skip")
        except UnicodeDecodeError:
            buf.seek(0)
            return pd.read_csv(buf, sep=delim, encoding="latin1", engine="python", on_bad_lines="skip")
    except Exception:
        return None

def _load_settlement_finnet(files) -> pd.DataFrame:
    all_dfs = []
    for f in files or []:
        try: data = f.getvalue() if hasattr(f, "getvalue") else f.read()
        except Exception: continue
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
                        if part is not None: all_dfs.append(part)
            elif name.endswith(".csv"):
                part = _read_finnet_single_csv(data)
                if part is not None: all_dfs.append(part)
        except Exception:
            continue
    if not all_dfs: return pd.DataFrame()
    df = pd.concat(all_dfs, ignore_index=True)
    df.rename(columns={c: str(c).strip() for c in df.columns}, inplace=True)
    norm_cols = {c: _norm_colname(c) for c in df.columns}
    rename_map = {}
    for req in ["Payment Method","Merchant Amount","Payment Date Time","Merchant Name"]:
        req_norm = _norm_colname(req)
        for real, nm in norm_cols.items():
            if nm == req_norm or nm.startswith(req_norm) or req_norm.startswith(nm):
                rename_map[real] = req; break
    df.rename(columns=rename_map, inplace=True)
    return df

def _build_finnet_settlement_table(df_finnet: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    if df_finnet is None or df_finnet.empty: return pd.DataFrame()
    df = df_finnet.copy()
    needed = ["Payment Date Time","Merchant Amount","Merchant Name","Payment Method"]
    for c in needed:
        if c not in df.columns: return pd.DataFrame()

    raw_dt = df["Payment Date Time"].astype(str).str.strip()
    t = pd.to_datetime(raw_dt.str.slice(0, 10), errors="coerce")
    df["Tanggal"] = t.dt.date
    df = df.loc[(t.dt.year == year) & (t.dt.month == month)].copy()
    if df.empty: return pd.DataFrame()

    def map_pelabuhan(name: str) -> str:
        n = str(name).upper()
        if "BAKAUHENI" in n: return "ASDP Bakauheni"
        if "GILIMANUK" in n: return "ASDP Gilimanuk"
        if "KETAPANG" in n:  return "ASDP Ketapang"
        if "MERAK" in n:     return "ASDP Merak"
        return "ASDP Lainnya"

    df["Pelabuhan"] = df["Merchant Name"].map(map_pelabuhan)
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
    for c in ["VIRTUAL ACCOUNT","E-MONEY","BCA","NON BCA"]: grouped[c] = grouped[c].fillna(0.0)

    unique_ports = grouped["Pelabuhan"].dropna().unique()
    if len(unique_ports) == 0: return pd.DataFrame()
    days = monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days+1)]
    full_idx = pd.MultiIndex.from_product([all_dates, unique_ports], names=["Tanggal","Pelabuhan"])
    out = pd.DataFrame(index=full_idx).reset_index().merge(grouped, on=["Tanggal","Pelabuhan"], how="left")
    for c in ["VIRTUAL ACCOUNT","E-MONEY","BCA","NON BCA"]: out[c] = out[c].fillna(0.0)
    out["TOTAL VA + E-MONEY"] = out["VIRTUAL ACCOUNT"] + out["E-MONEY"]
    out["TOTAL BCA + NON BCA"] = out["BCA"] + out["NON BCA"]
    desired = ["Tanggal","Pelabuhan","VIRTUAL ACCOUNT","E-MONEY","TOTAL VA + E-MONEY","BCA","NON BCA","TOTAL BCA + NON BCA"]
    return out.sort_values(["Pelabuhan","Tanggal"]).reset_index(drop=True)[desired]

# ---------- RK readers ----------
def _read_any_table_with_header(content: bytes, filename: str, header_row: int) -> Optional[pd.DataFrame]:
    skiprows = range(0, max(header_row - 1, 0))
    low = str(filename).lower()
    ext = low.rsplit(".", 1)[-1] if "." in low else ""
    try:
        if ext in {"xlsx","xlsm"}: return pd.read_excel(io.BytesIO(content), engine="openpyxl", skiprows=skiprows, header=0)
        if ext == "xls": return pd.read_excel(io.BytesIO(content), engine="xlrd", skiprows=skiprows, header=0)
        if ext == "xlsb": return pd.read_excel(io.BytesIO(content), engine="pyxlsb", skiprows=skiprows, header=0)
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
            if eng:
                df = pd.read_excel(io.BytesIO(content), engine=eng, header=0)
            else:
                df = pd.read_excel(io.BytesIO(content), header=0)
            break
        except Exception:
            df = None
    if df is None:
        try:
            if _HAS_PYARROW:
                df = pd.read_csv(io.BytesIO(content), header=0, engine="pyarrow")
            else:
                df = pd.read_csv(io.StringIO(content.decode("utf-8-sig","ignore")), header=0)
        except Exception:
            return None
    return df if (df is not None and not df.empty) else None

def _load_rk_bca_generic(files, keys: List[str], port_from_name) -> Dict[Tuple[date, str], float]:
    totals = defaultdict(float)
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
    totals = defaultdict(float)
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
        g = sub.groupby("Tanggal")["Amount"].sum()
        for dt_val, amt in g.items():
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

# ---------- Helper gabungan KTP+GLM ----------
def _append_ketapang_gilimanuk_combined(df: pd.DataFrame, final_cols: list) -> pd.DataFrame:
    if df is None or df.empty or "Pelabuhan" not in df.columns or "Tanggal" not in df.columns: return df
    ports_src = ["ASDP Ketapang", "ASDP Gilimanuk"]
    mask = df["Pelabuhan"].isin(ports_src)
    if not mask.any(): return df
    numeric_cols = [c for c in final_cols if c not in ("Tanggal","Pelabuhan") and c in df.columns]
    grouped = df.loc[mask].groupby("Tanggal", as_index=False)[numeric_cols].sum()
    if grouped.empty: return df
    grouped.insert(1, "Pelabuhan", "ASDP Gilimanuk + ASDP Ketapang")
    out = pd.concat([df, grouped[final_cols]], ignore_index=True)
    return out.sort_values(["Pelabuhan","Tanggal"]).reset_index(drop=True)

# ---------- Rekon Table Builders ----------
def _build_finnet_rekon_table(agg, df_finnet_settlement: Optional[pd.DataFrame], year: int, month: int,
                              bca_inflow_by_dt_port=None, nonbca_inflow_by_dt_port=None) -> pd.DataFrame:
    ports_from_payment = {_canonical_port_name(asal) for (_, asal) in agg.keys() if asal is not None}
    ports_from_settle = set()
    if df_finnet_settlement is not None and not df_finnet_settlement.empty and "Pelabuhan" in df_finnet_settlement.columns:
        ports_from_settle = set(df_finnet_settlement["Pelabuhan"].dropna().apply(_canonical_port_name).unique())
    unique_ports = sorted(ports_from_payment.union(ports_from_settle))
    if not unique_ports: return pd.DataFrame()

    days = monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days + 1)]
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
        settle_df = (df_finnet_settlement[need].copy() if len(need) == 4 else pd.DataFrame(columns=["Tanggal","Pelabuhan","BCA","NON BCA"]))
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

def _build_espay_rekon_table(agg, df_espay_settlement: Optional[pd.DataFrame], year: int, month: int,
                              bca_inflow_by_dt_port_sgw=None, nonbca_inflow_by_dt_port_sgw=None) -> pd.DataFrame:
    ports_from_payment = {_canonical_port_name(asal) for (_, asal) in agg.keys() if asal is not None}
    ports_from_settle = set()
    if df_espay_settlement is not None and not df_espay_settlement.empty and "Pelabuhan" in df_espay_settlement.columns:
        ports_from_settle = set(df_espay_settlement["Pelabuhan"].dropna().apply(_canonical_port_name).unique())
    unique_ports = sorted(ports_from_payment.union(ports_from_settle))
    if not unique_ports: return pd.DataFrame()

    days = monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days + 1)]
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
        settle_df = (df_espay_settlement[need].copy() if len(need) == 4 else pd.DataFrame(columns=["Tanggal","Pelabuhan","BCA","NON BCA"]))
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
    out["Tiket_BCA"] = out.get("Tiket_BCA", 0.0).fillna(0.0)
    out["Tiket_NON_BCA"] = out.get("Tiket_NON_BCA", 0.0).fillna(0.0)
    out["BCA"] = out.get("BCA", 0.0).fillna(0.0)
    out["NON BCA"] = out.get("NON BCA", 0.0).fillna(0.0)

    out["Tiket Detail - BCA"] = out["Tiket_BCA"]
    out["Tiket Detail - Non BCA"] = out["Tiket_NON_BCA"]
    out["Settlement Report - BCA"] = out["BCA"]
    out["Settlement Report - Non BCA"] = out["NON BCA"]

    bca_map = bca_inflow_by_dt_port_sgw or {}
    nonbca_map = nonbca_inflow_by_dt_port_sgw or {}
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

# ---------- Summary helpers ----------
def _count_tx_finnet(df_raw: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    if df_raw is None or df_raw.empty: return pd.DataFrame(columns=["Pelabuhan","Jumlah Transaksi"])
    need = ["Payment Date Time","Merchant Name"]
    for c in need:
        if c not in df_raw.columns: return pd.DataFrame(columns=["Pelabuhan","Jumlah Transaksi"])
    raw_dt = df_raw["Payment Date Time"].astype(str).str.strip()
    t = pd.to_datetime(raw_dt.str.slice(0, 10), errors="coerce")
    df = pd.DataFrame({"Tanggal": t.dt.date, "Merchant Name": df_raw["Merchant Name"].astype(str)})
    df = df.loc[(t.dt.year == year) & (t.dt.month == month)]
    if df.empty: return pd.DataFrame(columns=["Pelabuhan","Jumlah Transaksi"])

    def map_port(name: str) -> str:
        n = str(name).upper()
        if "BAKAUHENI" in n: return "ASDP Bakauheni"
        if "GILIMANUK" in n: return "ASDP Gilimanuk"
        if "KETAPANG" in n:  return "ASDP Ketapang"
        if "MERAK" in n:     return "ASDP Merak"
        return "ASDP Lainnya"
    ports = df["Merchant Name"].map(map_port)
    return ports.value_counts().rename_axis("Pelabuhan").rename("Jumlah Transaksi").reset_index()

def _espay_stats_raw_map(df_espay_raw: pd.DataFrame, year: int, month: int) -> Dict[str, Tuple[int, float]]:
    out = {}
    if df_espay_raw is None or df_espay_raw.empty: return out
    for c in ["Settlement Date","VA NAME","Settlement Amount"]:
        if c not in df_espay_raw.columns: return out
    t = pd.to_datetime(df_espay_raw["Settlement Date"], errors="coerce")
    m = (t.dt.year == year) & (t.dt.month == month)
    tmp = df_espay_raw.loc[m].copy()
    if tmp.empty: return out
    def pmap(v: str) -> Optional[str]:
        u = str(v).upper()
        if "BAKAUHENI" in u: return "ASDP Bakauheni"
        if "GILIMANUK" in u: return "ASDP Gilimanuk"
        if "KETAPANG"  in u: return "ASDP Ketapang"
        if "MERAK"     in u: return "ASDP Merak"
        return None
    tmp["Port"] = tmp["VA NAME"].map(pmap)
    tmp = tmp[tmp["Port"].notna()]
    if tmp.empty: return out
    g = tmp.groupby("Port").agg(cnt=("VA NAME","size"), amt=("Settlement Amount","sum"))
    for port, row in g.iterrows():
        out[port] = (int(row["cnt"]), float(row["amt"]))
    return out

def _amount_exc_finnet_telkom(df_raw: pd.DataFrame, year: int, month: int) -> Dict[str, float]:
    if df_raw is None or df_raw.empty: return {}
    needed = ["Payment Date Time", "Merchant Amount", "Merchant Name"]
    for c in needed:
        if c not in df_raw.columns: return {}
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

def _payment_stats_by_port(agg, scheme: str) -> Dict[str, Tuple[int, float, float]]:
    if not agg: return {}
    if scheme.upper() == "FINNET":
        exc_keys = ("FINNET_TIKET_BCA","FINNET_TIKET_NON_BCA")
        cnt_keys = ("FINNET_TIKET_BCA_CNT","FINNET_TIKET_NON_BCA_CNT")
        inc_key  = "FINNET_INC"
    else:
        exc_keys = ("ESPAY_TIKET_BCA","ESPAY_TIKET_NON_BCA")
        cnt_keys = ("ESPAY_TIKET_BCA_CNT","ESPAY_TIKET_NON_BCA_CNT")
        inc_key  = "ESPAY_INC"
    out = defaultdict(lambda: [0, 0.0, 0.0])  # cnt, exc, inc
    for (_, asal), bucket in agg.items():
        port = _canonical_port_name(asal)
        cnt = float(bucket.get(cnt_keys[0], 0.0)) + float(bucket.get(cnt_keys[1], 0.0))
        exc = float(bucket.get(exc_keys[0], 0.0)) + float(bucket.get(exc_keys[1], 0.0))
        inc = float(bucket.get(inc_key, 0.0))
        tmp = out[port]
        tmp[0] += cnt; tmp[1] += exc; tmp[2] += inc
    res = {}
    for k, v in out.items():
        cnt = int(v[0]); exc = float(v[1]); inc = float(v[2]) if v[2] > 0 else float(v[1])
        res[k] = (cnt, exc, inc)
    return res

def _rk_sum_by_port(*rk_maps: Dict[Tuple[date, str], float]) -> Dict[str, float]:
    total = defaultdict(float)
    for mp in rk_maps:
        for (dt, port), amt in (mp or {}).items():
            total[_canonical_port_name(port)] += float(amt)
    return dict(total)

# ---------- Summary builders ----------
def _build_summary_finnet_menu_vs_settlement(agg, df_finnet_telkom_raw: pd.DataFrame, year: int, month: int,
                                             rk_finnet_map: Optional[Dict[str, float]] = None) -> pd.DataFrame:
    periode = _period_label(year, month)
    pm_stats = _payment_stats_by_port(agg, "FINNET")
    telkom_cnt = _count_tx_finnet(df_finnet_telkom_raw, year, month)
    telkom_exc = _amount_exc_finnet_telkom(df_finnet_telkom_raw, year, month)
    rk_map = rk_finnet_map or {}
    telkom_cnt_map = {}
    if telkom_cnt is not None and not telkom_cnt.empty:
        for _, r in telkom_cnt.iterrows():
            telkom_cnt_map[str(r["Pelabuhan"])] = int(r["Jumlah Transaksi"])

    def pm_cnt(p): return int(pm_stats.get(p, (0,0.0,0.0))[0])
    def pm_inc(p): return float(pm_stats.get(p, (0,0.0,0.0))[2])
    def pm_exc(p): return float(pm_stats.get(p, (0,0.0,0.0))[1])
    def tk_cnt(p): return int(telkom_cnt_map.get(p, 0))
    def tk_exc(p): return float(telkom_exc.get(p, 0.0))
    def rk(p): return float(rk_map.get(p, 0.0))

    rows = [
        {
            "Periode": periode, "Cabang": "ASDP Bakauheni",
            ("Menu Payment Ferizy","Jumlah Transaksi"): pm_cnt("ASDP Bakauheni"),
            ("Menu Payment Ferizy","Nominal Transaksi (inc fee)"): pm_inc("ASDP Bakauheni"),
            ("Menu Payment Ferizy","Nominal Transaksi (exc fee)"): pm_exc("ASDP Bakauheni"),
            ("Data Settlement FINNET","Jumlah Transaksi"): tk_cnt("ASDP Bakauheni"),
            ("Data Settlement FINNET","Nominal Transaksi (exc fee)"): tk_exc("ASDP Bakauheni"),
            ("Data Settlement FINNET","Rekening Koran"): rk("ASDP Bakauheni"),
        },
        {
            "Periode": periode, "Cabang": "ASDP Gilimanuk + ASDP Ketapang",
            ("Menu Payment Ferizy","Jumlah Transaksi"): pm_cnt("ASDP Gilimanuk") + pm_cnt("ASDP Ketapang"),
            ("Menu Payment Ferizy","Nominal Transaksi (inc fee)"): pm_inc("ASDP Gilimanuk") + pm_inc("ASDP Ketapang"),
            ("Menu Payment Ferizy","Nominal Transaksi (exc fee)"): pm_exc("ASDP Gilimanuk") + pm_exc("ASDP Ketapang"),
            ("Data Settlement FINNET","Jumlah Transaksi"): tk_cnt("ASDP Gilimanuk") + tk_cnt("ASDP Ketapang"),
            ("Data Settlement FINNET","Nominal Transaksi (exc fee)"): tk_exc("ASDP Gilimanuk") + tk_exc("ASDP Ketapang"),
            ("Data Settlement FINNET","Rekening Koran"): rk("ASDP Gilimanuk") + rk("ASDP Ketapang"),
        },
        {
            "Periode": periode, "Cabang": "ASDP Merak",
            ("Menu Payment Ferizy","Jumlah Transaksi"): pm_cnt("ASDP Merak"),
            ("Menu Payment Ferizy","Nominal Transaksi (inc fee)"): pm_inc("ASDP Merak"),
            ("Menu Payment Ferizy","Nominal Transaksi (exc fee)"): pm_exc("ASDP Merak"),
            ("Data Settlement FINNET","Jumlah Transaksi"): tk_cnt("ASDP Merak"),
            ("Data Settlement FINNET","Nominal Transaksi (exc fee)"): tk_exc("ASDP Merak"),
            ("Data Settlement FINNET","Rekening Koran"): rk("ASDP Merak"),
        },
    ]
    df = pd.DataFrame(rows)
    total = {"Periode": periode, "Cabang": "Total"}
    for k in [("Menu Payment Ferizy","Jumlah Transaksi"),
              ("Menu Payment Ferizy","Nominal Transaksi (inc fee)"),
              ("Menu Payment Ferizy","Nominal Transaksi (exc fee)"),
              ("Data Settlement FINNET","Jumlah Transaksi"),
              ("Data Settlement FINNET","Nominal Transaksi (exc fee)"),
              ("Data Settlement FINNET","Rekening Koran")]:
        total[k] = df[k].sum()
    df = pd.concat([df, pd.DataFrame([total])], ignore_index=True)
    df.columns = pd.MultiIndex.from_tuples([(c if isinstance(c, tuple) else ("", c)) for c in df.columns])
    df = df[[("", "Periode"), ("", "Cabang"),
             ("Menu Payment Ferizy","Jumlah Transaksi"),
             ("Menu Payment Ferizy","Nominal Transaksi (inc fee)"),
             ("Menu Payment Ferizy","Nominal Transaksi (exc fee)"),
             ("Data Settlement FINNET","Jumlah Transaksi"),
             ("Data Settlement FINNET","Nominal Transaksi (exc fee)"),
             ("Data Settlement FINNET","Rekening Koran")]]
    return df

def _build_summary_espay_menu_vs_settlement(agg, df_espay_raw: pd.DataFrame, year: int, month: int,
                                            rk_espay_map: Optional[Dict[str, float]] = None) -> pd.DataFrame:
    periode = _period_label(year, month)
    pm_stats = _payment_stats_by_port(agg, "ESPAY")
    espay_raw_stats = _espay_stats_raw_map(df_espay_raw, year, month)
    rk_map = rk_espay_map or {}

    def pm_cnt(p): return int(pm_stats.get(p, (0,0.0,0.0))[0])
    def pm_inc(p): return float(pm_stats.get(p, (0,0.0,0.0))[2])
    def pm_exc(p): return float(pm_stats.get(p, (0,0.0,0.0))[1])
    def esp_cnt(p): return int(espay_raw_stats.get(p, (0,0.0))[0])
    def esp_exc(p): return float(espay_raw_stats.get(p, (0,0.0))[1])
    def rk(p): return float(rk_map.get(p, 0.0))

    rows = [
        {
            "Periode": periode, "Cabang": "ASDP Bakauheni",
            ("Menu Payment Ferizy","Jumlah Transaksi"): pm_cnt("ASDP Bakauheni"),
            ("Menu Payment Ferizy","Nominal Transaksi (inc fee)"): pm_inc("ASDP Bakauheni"),
            ("Menu Payment Ferizy","Nominal Transaksi (exc fee)"): pm_exc("ASDP Bakauheni"),
            ("Data Settlement ESPAY","Jumlah Transaksi"): esp_cnt("ASDP Bakauheni"),
            ("Data Settlement ESPAY","Nominal Transaksi (exc fee)"): esp_exc("ASDP Bakauheni"),
            ("Data Settlement ESPAY","Rekening Koran"): rk("ASDP Bakauheni"),
        },
        {
            "Periode": periode, "Cabang": "ASDP Gilimanuk + ASDP Ketapang",
            ("Menu Payment Ferizy","Jumlah Transaksi"): pm_cnt("ASDP Gilimanuk") + pm_cnt("ASDP Ketapang"),
            ("Menu Payment Ferizy","Nominal Transaksi (inc fee)"): pm_inc("ASDP Gilimanuk") + pm_inc("ASDP Ketapang"),
            ("Menu Payment Ferizy","Nominal Transaksi (exc fee)"): pm_exc("ASDP Gilimanuk") + pm_exc("ASDP Ketapang"),
            ("Data Settlement ESPAY","Jumlah Transaksi"): esp_cnt("ASDP Gilimanuk") + esp_cnt("ASDP Ketapang"),
            ("Data Settlement ESPAY","Nominal Transaksi (exc fee)"): esp_exc("ASDP Gilimanuk") + esp_exc("ASDP Ketapang"),
            ("Data Settlement ESPAY","Rekening Koran"): rk("ASDP Gilimanuk") + rk("ASDP Ketapang"),
        },
        {
            "Periode": periode, "Cabang": "ASDP Merak",
            ("Menu Payment Ferizy","Jumlah Transaksi"): pm_cnt("ASDP Merak"),
            ("Menu Payment Ferizy","Nominal Transaksi (inc fee)"): pm_inc("ASDP Merak"),
            ("Menu Payment Ferizy","Nominal Transaksi (exc fee)"): pm_exc("ASDP Merak"),
            ("Data Settlement ESPAY","Jumlah Transaksi"): esp_cnt("ASDP Merak"),
            ("Data Settlement ESPAY","Nominal Transaksi (exc fee)"): esp_exc("ASDP Merak"),
            ("Data Settlement ESPAY","Rekening Koran"): rk("ASDP Merak"),
        },
    ]
    df = pd.DataFrame(rows)
    total = {"Periode": periode, "Cabang": "Total"}
    for k in [("Menu Payment Ferizy","Jumlah Transaksi"),
              ("Menu Payment Ferizy","Nominal Transaksi (inc fee)"),
              ("Menu Payment Ferizy","Nominal Transaksi (exc fee)"),
              ("Data Settlement ESPAY","Jumlah Transaksi"),
              ("Data Settlement ESPAY","Nominal Transaksi (exc fee)"),
              ("Data Settlement ESPAY","Rekening Koran")]:
        total[k] = df[k].sum()
    df = pd.concat([df, pd.DataFrame([total])], ignore_index=True)
    df.columns = pd.MultiIndex.from_tuples([(c if isinstance(c, tuple) else ("", c)) for c in df.columns])
    df = df[[("", "Periode"), ("", "Cabang"),
             ("Menu Payment Ferizy","Jumlah Transaksi"),
             ("Menu Payment Ferizy","Nominal Transaksi (inc fee)"),
             ("Menu Payment Ferizy","Nominal Transaksi (exc fee)"),
             ("Data Settlement ESPAY","Jumlah Transaksi"),
             ("Data Settlement ESPAY","Nominal Transaksi (exc fee)"),
             ("Data Settlement ESPAY","Rekening Koran")]]
    return df

def _render_summary(df_sum: pd.DataFrame):
    if df_sum is None or df_sum.empty:
        st.info("Summary kosong."); return
    fmt = {}
    keys = []
    for grp in ["Menu Payment Ferizy","Data Settlement FINNET","Data Settlement ESPAY"]:
        for col in ["Jumlah Transaksi","Nominal Transaksi (inc fee)","Nominal Transaksi (exc fee)","Rekening Koran"]:
            keys.append((grp, col))
    if isinstance(df_sum.columns, pd.MultiIndex):
        for k in keys:
            if k in df_sum.columns: fmt[k] = "{:,.0f}"
    try:
        st.dataframe(df_sum.style.format(fmt), use_container_width=True)
    except Exception:
        st.dataframe(df_sum, use_container_width=True)

# ---------- Export Excel ----------
def _safe_sheetname(name: str) -> str:
    s = re.sub(r"[:\\/?*\[\]]", "-", str(name))
    if len(s) > 31: s = s[:31]
    return s

def _short_port(port: str) -> str:
    return re.sub(r"^ASDP\s+", "", str(port).strip(), flags=re.I).upper()

def _prep_numeric_dates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "Tanggal" in out.columns: out["Tanggal"] = pd.to_datetime(out["Tanggal"], errors="coerce")
    num_cols = out.select_dtypes(include="number").columns
    out[num_cols] = out[num_cols].fillna(0).round(0).astype("Int64")
    return out

def _to_excel_workbook_bytes(df_payment: pd.DataFrame, df_rekon_finnet: pd.DataFrame, df_rekon_espay: pd.DataFrame,
                             df_summary_finnet: pd.DataFrame, df_summary_espay: pd.DataFrame,
                             year: int, month: int):
    wrote = False
    for engine in ("xlsxwriter","openpyxl"):
        try:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine=engine) as writer:
                if df_summary_finnet is not None and not df_summary_finnet.empty:
                    df_summary_finnet.to_excel(writer, sheet_name=_safe_sheetname("Summary - Finnet"), index=False, merge_cells=True); wrote = True
                if df_summary_espay is not None and not df_summary_espay.empty:
                    df_summary_espay.to_excel(writer, sheet_name=_safe_sheetname("Summary - Espay"), index=False, merge_cells=True); wrote = True
                if df_payment is not None and not df_payment.empty:
                    for p in sorted([x for x in df_payment["Pelabuhan"].dropna().unique() if "+" not in str(x)]):
                        _prep_numeric_dates(df_payment[df_payment["Pelabuhan"] == p]).to_excel(
                            writer, sheet_name=_safe_sheetname(f"Payment - {_short_port(p)}"), index=False
                        ); wrote = True
                if df_rekon_finnet is not None and not df_rekon_finnet.empty:
                    for p in sorted([x for x in df_rekon_finnet["Pelabuhan"].dropna().unique() if "+" not in str(x)]):
                        _prep_numeric_dates(df_rekon_finnet[df_rekon_finnet["Pelabuhan"] == p]).to_excel(
                            writer, sheet_name=_safe_sheetname(f"Finnet - {_short_port(p)}"), index=False
                        ); wrote = True
                if df_rekon_espay is not None and not df_rekon_espay.empty:
                    for p in sorted([x for x in df_rekon_espay["Pelabuhan"].dropna().unique() if "+" not in str(x)]):
                        _prep_numeric_dates(df_rekon_espay[df_rekon_espay["Pelabuhan"] == p]).to_excel(
                            writer, sheet_name=_safe_sheetname(f"Espay - {_short_port(p)}"), index=False
                        ); wrote = True
                if not wrote:
                    pd.DataFrame({"Info":["Tidak ada data untuk diekspor"]}).to_excel(writer, sheet_name="Empty", index=False)
            return buf.getvalue(), engine, None
        except ImportError:
            continue
        except Exception as e:
            return None, None, f"Gagal menulis Excel ({engine}): {e}"
    return None, None, "Tidak ada engine Excel (xlsxwriter/openpyxl)."

# ---------- UI Helpers ----------
def _render_df(df_show: pd.DataFrame, highlight: bool, max_rows_style: int = 1500) -> None:
    df_show = df_show.copy()
    df_show["Tanggal"] = pd.to_datetime(df_show["Tanggal"]).dt.strftime("%d/%m/%Y")
    df_show = _add_subtotal_row(df_show, label="Subtotal", date_col="Tanggal")
    numeric_cols = df_show.select_dtypes(include="number").columns
    df_show[numeric_cols] = df_show[numeric_cols].fillna(0).round(0).astype("Int64")
    if len(df_show) <= max_rows_style and highlight:
        try:
            st.dataframe(_style_table(df_show, True), use_container_width=True, height=520); return
        except Exception:
            pass
    st.dataframe(df_show, use_container_width=True, height=520)

# ---------- MAIN ----------
def main() -> None:
    st.title("Rekonsiliasi Payment Report")
    st.sidebar.success("Upload file lalu klik ▶️ Mulai Proses. Tiap uploader diproses mandiri.")

    today = date.today()
    years_options = list(range(today.year - 5, today.year + 6))
    year = st.sidebar.selectbox("Tahun", options=years_options, index=years_options.index(today.year))
    month_names = {1:"01 - Januari",2:"02 - Februari",3:"03 - Maret",4:"04 - April",5:"05 - Mei",6:"06 - Juni",
                   7:"07 - Juli",8:"08 - Agustus",9:"09 - September",10:"10 - Oktober",11:"11 - November",12:"12 - Desember"}
    month = st.sidebar.selectbox("Bulan", options=list(range(1,13)), index=today.month-1, format_func=lambda m: month_names[m])

    st.sidebar.markdown("### ⚙️ Kinerja")
    max_workers = st.sidebar.slider("Parallel workers (antar file)", 1, 4, 2)
    chunk_rows = st.sidebar.number_input("CSV chunk rows", min_value=50_000, step=50_000, value=DEFAULT_CSV_CHUNK_ROWS)
    st.sidebar.caption(("pyarrow ✔️" if _HAS_PYARROW else "pyarrow ❌"))

    ss_get_set("upload_rev", 0)
    if st.sidebar.button("🔄 Reset semua upload"): st.session_state.upload_rev += 1

    up_files = st.sidebar.file_uploader("Upload Payment Report: ZIP / Excel (.xlsx/.xls/.xlsb) / CSV",
        type=["zip","xlsx","xls","xlsb","csv"], accept_multiple_files=True, key=f"payment_{st.session_state.upload_rev}")
    settlement_files = st.sidebar.file_uploader("Upload Settlement ESPAY (.xlsx / .csv)",
        type=["xlsx","csv"], accept_multiple_files=True, key=f"settlement_espay_{st.session_state.upload_rev}")
    finnet_files = st.sidebar.file_uploader("Upload Settlement Finnet by Telkom (ZIP / .csv)",
        type=["zip","csv"], accept_multiple_files=True, key=f"settlement_finnet_{st.session_state.upload_rev}")
    finnet_espay_files = st.sidebar.file_uploader("Upload Settlement Finnet (ESPAY) (ZIP / .csv)",
        type=["zip","csv"], accept_multiple_files=True, key=f"settlement_finnet_espay_{st.session_state.upload_rev}")
    rek_bca_files = st.sidebar.file_uploader("Upload Rekening Koran BCA",
        type=["zip","xlsx","xls","xlsb","csv"], accept_multiple_files=True, key=f"rek_bca_{st.session_state.upload_rev}")
    rek_nonbca_files = st.sidebar.file_uploader("Upload Rekening Koran Non BCA",
        type=["zip","xlsx","xls","xlsb","csv"], accept_multiple_files=True, key=f"rek_nonbca_{st.session_state.upload_rev}")

    highlight = st.sidebar.checkbox("Highlight Selisih ≠ 0", value=True)
    ss_get_set("run_started", False); ss_get_set("results", {})

    c1, c2, c3, _ = st.columns([1,1,1,6])
    if c1.button("▶️ Mulai Proses", type="primary"): st.session_state.run_started, st.session_state.results = True, {}
    if c2.button("🔁 Proses Ulang"): st.session_state.run_started, st.session_state.results = True, {}
    if c3.button("🧹 Bersihkan Hasil"): st.session_state.run_started, st.session_state.results = False, {}

    if not st.session_state.run_started:
        st.info("Unggah berkas apa saja, lalu klik **▶️ Mulai Proses**. Tiap step berjalan walau step lain kosong.")
        return

    progress = st.progress(0); results = st.session_state.results

    # 1) Payment
    progress.progress(5)
    if "payment" not in results:
        with st.spinner("1/5 • Memproses Payment Report (jika ada)…"):
            if up_files:
                agg = load_and_aggregate_fast(up_files, year, month, max_workers, chunk_rows)
                results["agg"] = agg; results["payment"] = _build_result_from_agg(agg)
            else:
                results["agg"] = _empty_agg(); results["payment"] = pd.DataFrame()
    df_payment = results["payment"]
    st.subheader(f"1) Hasil Rekonsiliasi Payment • {month_names[month]} {year}")
    if df_payment.empty:
        st.info("Payment Report tidak diupload / kosong.")
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

    # 2) Settlement ESPAY
    st.divider(); st.subheader("2) DETAIL SETTLEMENT ESPAY")
    if "espay" not in results:
        with st.spinner("2/5 • Memproses Settlement ESPAY (jika ada)…"):
            if settlement_files:
                raw = _load_settlement_espay(settlement_files)
                results["espay_raw"] = raw; results["espay"] = _build_espay_settlement_table(raw, year, month)
            else:
                results["espay_raw"] = pd.DataFrame(); results["espay"] = pd.DataFrame()
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

    # 3) FINNET by Telkom
    st.divider(); st.subheader("3) DETAIL SETTLEMENT FINNET BY TELKOM")
    if "finnet_telkom" not in results:
        with st.spinner("3/5 • Memproses Settlement FINNET (Telkom) (jika ada)…"):
            if finnet_files:
                raw = _load_settlement_finnet(finnet_files)
                results["finnet_telkom_raw"] = raw; results["finnet_telkom"] = _build_finnet_settlement_table(raw, year, month)
            else:
                results["finnet_telkom_raw"] = pd.DataFrame(); results["finnet_telkom"] = pd.DataFrame()
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

    # 4) FINNET (ESPAY)
    st.divider(); st.subheader("4) DETAIL SETTLEMENT FINNET (ESPAY)")
    if "finnet_espay" not in results:
        with st.spinner("4/5 • Memproses Settlement FINNET (ESPAY) (jika ada)…"):
            if finnet_espay_files:
                raw = _load_settlement_finnet(finnet_espay_files)
                results["finnet_espay_raw"] = raw; results["finnet_espay"] = _build_finnet_settlement_table(raw, year, month)
            else:
                results["finnet_espay_raw"] = pd.DataFrame(); results["finnet_espay"] = pd.DataFrame()
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

    # 5) Rekonsiliasi gabungan
    st.divider(); st.subheader("5) TABEL REKONSILIASI GABUNGAN PAYMENT - SETTLEMENT DANA - REKENING KORAN")
    agg = results.get("agg", _empty_agg())

    st.markdown("**1. Rekonsiliasi Finnet**")
    bca_finif = _load_rk_bca_finif_by_dt_port(rek_bca_files) if rek_bca_files else {}
    nonbca_finif = _load_rk_nonbca_inflow_by_dt_port_from_files(rek_nonbca_files, header_row=13) if rek_nonbca_files else {}
    df_rekon_finnet = _build_finnet_rekon_table(agg, results.get("finnet_telkom", pd.DataFrame()), year, month,
                                                bca_inflow_by_dt_port=bca_finif, nonbca_inflow_by_dt_port=nonbca_finif)
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
    df_rekon_espay = _build_espay_rekon_table(agg, results.get("espay", pd.DataFrame()), year, month,
                                              bca_inflow_by_dt_port_sgw=bca_sgw, nonbca_inflow_by_dt_port_sgw=nonbca_sgw)
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

    # Summary
    st.divider(); st.subheader("TABEL SUMMARY REKONSILIASI")
    rk_finnet_port = _rk_sum_by_port(bca_finif, nonbca_finif)
    rk_espay_port = _rk_sum_by_port(bca_sgw, nonbca_sgw)
    with st.expander("Summary • FINNET", expanded=True):
        sum_finnet = _build_summary_finnet_menu_vs_settlement(
            agg, results.get("finnet_telkom_raw", pd.DataFrame()), year, month, rk_finnet_port
        )
        _render_summary(sum_finnet)
    with st.expander("Summary • ESPAY", expanded=True):
        sum_espay = _build_summary_espay_menu_vs_settlement(
            agg, results.get("espay_raw", pd.DataFrame()), year, month, rk_espay_port
        )
        _render_summary(sum_espay)

    progress.progress(100)

    # Unduh Excel (saja)
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

if __name__ == "__main__":
    try:
        main()
    except BaseException as e:
        st.error("Aplikasi error saat render awal.")
        st.exception(e)

# path: streamlit_app.py
import io
import re
import zipfile
from datetime import date
from calendar import monthrange
from collections import defaultdict, OrderedDict
from typing import List, Optional, Tuple, Dict

import pandas as pd
import streamlit as st
from openpyxl import load_workbook  # streaming .xlsx read_only

# =========================== Konfigurasi & Konstanta ===========================
COL_H = "TIPE PEMBAYARAN"
COL_B = "TANGGAL PEMBAYARAN"
COL_AA = "REF NO"
COL_K = "TOTAL TARIF TANPA BIAYA ADMIN (Rp.)"
COL_X = "SOF ID"
COL_ASAL = "ASAL"
REQUIRED_COLS = [COL_H, COL_B, COL_AA, COL_K, COL_X, COL_ASAL]

CAT_COLS = [
    "Cash", "Prepaid BRI", "Prepaid BNI", "Prepaid Mandiri",
    "Prepaid BCA", "SKPT", "IFCS", "Reedem", "ESPAY", "Finnet",
]
NON_COMPONENTS = [
    "Cash", "Prepaid BRI", "Prepaid BNI", "Prepaid Mandiri",
    "Prepaid BCA", "SKPT", "IFCS", "Reedem",
]

CSV_CHUNK_ROWS = 200_000
XLSX_BATCH_ROWS = 50_000
VALID_EXTS = (".xlsx", ".xls", ".xlsb", ".csv")

SETTLEMENT_REQUIRED_COLS = ["Product Name", "Settlement Amount", "Settlement Date", "VA NAME"]
FINNET_REQUIRED_COLS = ["Payment Method", "Merchant Amount", "Payment Date Time", "Merchant Name"]

NONBCA_ACC_TO_PORT = {"0188-01-000735-30-4": "ASDP Merak"}
NONBCA_CREDIT_COL_INDEX = 9  # kolom J (0-based)

# =========================== Utilitas umum ===========================
def _ensure_required_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError("Kolom wajib tidak ditemukan: " + ", ".join(missing) + ".")

def _style_table(df_display: pd.DataFrame, highlight: bool) -> "pd.io.formats.style.Styler":
    numeric_cols = df_display.select_dtypes(include="number").columns.tolist()
    styler = df_display.style.format("{:,.0f}", subset=numeric_cols)
    if highlight and "Selisih" in df_display.columns:
        styler = styler.apply(
            lambda s: [
                "background-color:#fdecea; color:#b71c1c; font-weight:600;" if (pd.notna(v) and float(v) != 0) else ""
                for v in s
            ],
            subset=["Selisih"],
        )
    return styler

def _add_subtotal_row(df_display: pd.DataFrame, label: str = "Subtotal", date_col: str = "Tanggal") -> pd.DataFrame:
    numeric_cols = df_display.select_dtypes(include="number").columns.tolist()
    totals = df_display[numeric_cols].sum()
    subtotal = {c: (totals[c] if c in totals else None) for c in df_display.columns}
    subtotal[date_col] = label
    return pd.concat([df_display, pd.DataFrame([subtotal])], ignore_index=True)

def _norm_colname(name: str) -> str:
    return "".join(ch.lower() for ch in str(name) if alnum := ch.isalnum())

def _canonical_port_name(name: Optional[str]) -> str:
    if name is None:
        return "Tidak diketahui"
    s = str(name).strip()
    up = s.upper()
    if "BAKAUHENI" in up: return "ASDP Bakauheni"
    if "GILIMANUK" in up: return "ASDP Gilimanuk"
    if "KETAPANG" in up: return "ASDP Ketapang"
    if "MERAK" in up: return "ASDP Merak"
    return s

def _normalize_alnum_upper(ser: pd.Series) -> pd.Series:
    return ser.astype(str).str.upper().str.replace(r"[^A-Z0-9]", "", regex=True)

def _remark_mask_contains_codes(remark: pd.Series, codes: List[str]) -> pd.Series:
    if not codes:
        return pd.Series([True] * len(remark), index=remark.index)
    norm = _normalize_alnum_upper(remark)
    code_norms = [re.sub(r"[^A-Z0-9]", "", str(c).upper()) for c in codes if str(c).strip()]
    mask = pd.Series(False, index=remark.index)
    for cn in code_norms:
        mask = mask | norm.str.contains(cn, na=False)
    return mask

def _to_date(v):
    return pd.to_datetime(v, errors="coerce", dayfirst=True)

# =========================== Agregator streaming Payment ===========================
def _empty_agg():
    return defaultdict(lambda: defaultdict(float))

def _update_agg_series(agg, ser: pd.Series, colname: str) -> None:
    if ser.empty: return
    for (dt, asal), val in ser.items():
        agg[(dt, asal)][colname] += float(val)

def _apply_rules_and_update(df_chunk: pd.DataFrame, agg) -> None:
    H = df_chunk[COL_H].fillna("").astype(str).str.lower()
    AA = df_chunk[COL_AA].fillna("").astype(str).str.lower()
    X = df_chunk[COL_X].fillna("").astype(str).str.lower()
    ASAL = df_chunk[COL_ASAL].fillna("Tidak diketahui").astype(str).str.strip()
    amt = pd.to_numeric(df_chunk[COL_K], errors="coerce").fillna(0)
    tgl = df_chunk["Tanggal"]

    def sum_by_key(mask) -> pd.Series:
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

# =========================== Pembaca cepat (CSV & Excel) ===========================
def _process_csv_fast(data: bytes, year: int, month: int, agg) -> None:
    itr = pd.read_csv(
        io.BytesIO(data),
        usecols=REQUIRED_COLS,
        chunksize=CSV_CHUNK_ROWS,
        dtype={COL_H: "string", COL_AA: "string", COL_X: "string", COL_ASAL: "string"},
    )
    for chunk in itr:
        t = pd.to_datetime(chunk[COL_B], errors="coerce")
        mask = (t.dt.year == year) & (t.dt.month == month)
        if not mask.any(): continue
        sub = chunk.loc[mask].copy()
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
        sub = df.loc[mask].copy()
        sub["Tanggal"] = t.loc[mask].dt.date
        _apply_rules_and_update(sub, agg)

def _process_xlsb(data: bytes, year: int, month: int, agg) -> None:
    try:
        df = pd.read_excel(io.BytesIO(data), sheet_name=0, usecols=REQUIRED_COLS, engine="pyxlsb")
    except Exception:
        return
    t = pd.to_datetime(df[COL_B], errors="coerce")
    mask = (t.dt.year == year) & (t.dt.month == month)
    if not mask.any(): return
    sub = df.loc[mask].copy()
    sub["Tanggal"] = t.loc[mask].dt.date
    _apply_rules_and_update(sub, agg)

def _flush_xlsx_batch(buf: List[List], year: int, month: int, agg) -> None:
    df = pd.DataFrame(buf, columns=[COL_H, COL_B, COL_AA, COL_K, COL_X, COL_ASAL])
    t = pd.to_datetime(df[COL_B], errors="coerce")
    mask = (t.dt.year == year) & (t.dt.month == month)
    if not mask.any(): return
    sub = df.loc[mask].copy()
    sub["Tanggal"] = t.loc[mask].dt.date
    _apply_rules_and_update(sub, agg)

# =========================== Loader multi-file Payment ===========================
def _load_and_aggregate(files: List["st.runtime.uploaded_file_manager.UploadedFile"], year: int, month: int):
    agg = _empty_agg()
    for f in files:
        data = f.getvalue()
        name = f.name.lower()
        try:
            if name.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for m in zf.infolist():
                        if m.is_dir(): continue
                        low = m.filename.lower()
                        if not low.endswith(VALID_EXTS): continue
                        content = zf.read(m)
                        if low.endswith((".xlsx", ".xls")):
                            _process_xlsx_streaming(content, year, month, agg)
                        elif low.endswith(".xlsb"):
                            _process_xlsb(content, year, month, agg)
                        else:
                            _process_csv_fast(content, year, month, agg)
            elif name.endswith((".xlsx", ".xls")):
                _process_xlsx_streaming(data, year, month, agg)
            elif name.endswith(".xlsb"):
                _process_xlsb(data, year, month, agg)
            elif name.endswith(".csv"):
                _process_csv_fast(data, year, month, agg)
        except Exception:
            continue
    return agg

# =========================== Build hasil Payment ===========================
def _build_result_from_agg(agg) -> pd.DataFrame:
    if not agg: return pd.DataFrame()
    rows: List[dict] = []
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
    df = df[["Tanggal", "Pelabuhan"] + CAT_COLS + ["Total", "BCA", "NON BCA", "NON", "TOTAL", "Selisih"]]
    df = df.sort_values(["Pelabuhan", "Tanggal"]).reset_index(drop=True)
    return df

# =========================== Settlement ESPAY ===========================
def _read_settlement_single_csv(content: bytes) -> Optional[pd.DataFrame]:
    try:
        text = content.decode("utf-8-sig", errors="ignore")
        df = pd.read_csv(io.StringIO(text), sep=",")
    except Exception:
        return None
    df.rename(columns={c: c.strip() for c in df.columns}, inplace=True)
    lower_to_real = {c.lower(): c for c in df.columns}
    rename_map = {}
    for req in SETTLEMENT_REQUIRED_COLS:
        key = req.lower()
        if key in lower_to_real: rename_map[lower_to_real[key]] = req
    df.rename(columns=rename_map, inplace=True)
    missing = [c for c in SETTLEMENT_REQUIRED_COLS if c not in df.columns]
    if missing: return None
    return df[SETTLEMENT_REQUIRED_COLS].copy()

def _load_settlement_espay(files: List["st.runtime.uploaded_file_manager.UploadedFile"]) -> pd.DataFrame:
    all_dfs: List[pd.DataFrame] = []
    for f in files:
        data = f.getvalue()
        name = f.name.lower()
        try:
            if name.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for info in zf.infolist():
                        if info.is_dir(): continue
                        low = info.filename.lower()
                        if not low.endswith(".csv"): continue
                        content = zf.read(info)
                        df_part = _read_settlement_single_csv(content)
                        if df_part is not None: all_dfs.append(df_part)
            elif name.endswith(".csv"):
                df_part = _read_settlement_single_csv(data)
                if df_part is not None: all_dfs.append(df_part)
        except Exception:
             continue
    if not all_dfs: return pd.DataFrame()
    return pd.concat(all_dfs, ignore_index=True)

def _build_espay_settlement_table(df_settlement: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    if df_settlement is None or df_settlement.empty: return pd.DataFrame()
    df = df_settlement.copy()
    t = pd.to_datetime(df["Settlement Date"], errors="coerce")
    df["Tanggal"] = t.dt.date
    mask = (t.dt.year == year) & (t.dt.month == month)
    df = df.loc[mask].copy()
    if df.empty: return pd.DataFrame()
    va_name = df["VA NAME"].fillna("").astype(str).str.upper()

    def map_pelabuhan(name: str) -> Optional[str]:
        if "BAKAUHENI" in name: return "ASDP Bakauheni"
        if "GILIMANUK" in name: return "ASDP Gilimanuk"
        if "KETAPANG" in name: return "ASDP Ketapang"
        if "MERAK" in name: return "ASDP Merak"
        return None

    df["Pelabuhan"] = va_name.apply(map_pelabuhan)
    df = df[df["Pelabuhan"].notna()].copy()
    if df.empty: return pd.DataFrame()

    amt_raw = df["Settlement Amount"].astype(str).str.strip()
    amt_clean = amt_raw.str.replace(r"[^\d\-]", "", regex=True)
    amt_parsed = pd.to_numeric(amt_clean, errors="coerce")
    amt = (amt_parsed / 100.0).fillna(0.0)

    pn = df["Product Name"].fillna("").astype(str).str.lower()
    is_va = pn.str.contains("va", na=False)
    is_bca = pn.str.contains("bca va online", na=False) | pn.str.contains("blu by bca digital", na=False)

    df["VIRTUAL ACCOUNT"] = amt.where(is_va, 0.0)
    df["E-MONEY"] = amt.where(~is_va, 0.0)
    df["BCA"] = amt.where(is_bca, 0.0)
    df["NON BCA"] = amt.where(~is_bca, 0.0)

    grouped = (
        df.groupby(["Tanggal", "Pelabuhan"], dropna=False)[
            ["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]
        ].sum().reset_index()
    )
    grouped[["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]] = grouped[
        ["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]
    ].fillna(0.0)

    unique_ports = grouped["Pelabuhan"].dropna().unique()
    if len(unique_ports) == 0: return pd.DataFrame()

    days_in_month = monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days_in_month + 1)]
    full_idx = pd.MultiIndex.from_product([all_dates, unique_ports], names=["Tanggal", "Pelabuhan"])
    full_df = pd.DataFrame(index=full_idx).reset_index()

    out = full_df.merge(grouped, on=["Tanggal", "Pelabuhan"], how="left")
    for col in ["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]:
        if col not in out.columns: out[col] = 0.0
        else: out[col] = out[col].fillna(0.0)

    out["TOTAL VA + E-MONEY"] = out["VIRTUAL ACCOUNT"] + out["E-MONEY"]
    out["TOTAL BCA + NON BCA"] = out["BCA"] + out["NON BCA"]
    out = out.sort_values(["Pelabuhan", "Tanggal"]).reset_index(drop=True)

    desired_order = [
        "Tanggal","Pelabuhan","VIRTUAL ACCOUNT","E-MONEY","TOTAL VA + E-MONEY",
        "BCA","NON BCA","TOTAL BCA + NON BCA",
    ]
    existing = [c for c in desired_order if c in out.columns]
    others = [c for c in out.columns if c not in existing]
    out = out[existing + others]
    return out

# =========================== Settlement Finnet (CSV) ===========================
def _read_finnet_single_csv(content: bytes) -> Optional[pd.DataFrame]:
    try:
        text = content.decode("utf-8-sig", errors="ignore")
        df = pd.read_csv(io.StringIO(text), sep=",")
    except Exception:
        return None
    df.rename(columns={c: c.strip() for c in df.columns}, inplace=True)
    norm_cols = {c: _norm_colname(c) for c in df.columns}
    rename_map = {}
    for req in FINNET_REQUIRED_COLS:
        req_norm = _norm_colname(req)
        for real, norm in norm_cols.items():
            if norm == req_norm or norm.startswith(req_norm) or req_norm.startswith(norm):
                rename_map[real] = req; break
    df.rename(columns=rename_map, inplace=True)
    return df

def _load_settlement_finnet(files: List["st.runtime.uploaded_file_manager.UploadedFile"]) -> pd.DataFrame:
    all_dfs: List[pd.DataFrame] = []
    for f in files:
        data = f.getvalue()
        name = f.name.lower()
        try:
            if name.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for info in zf.infolist():
                        if info.is_dir(): continue
                        low = info.filename.lower()
                        if not low.endswith(".csv"): continue
                        content = zf.read(info)
                        df_part = _read_finnet_single_csv(content)
                        if df_part is not None: all_dfs.append(df_part)
            elif name.endswith(".csv"):
                df_part = _read_finnet_single_csv(data)
                if df_part is not None: all_dfs.append(df_part)
        except Exception:
            continue
    if not all_dfs: return pd.DataFrame()
    return pd.concat(all_dfs, ignore_index=True)

def _build_finnet_settlement_table(df_finnet: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    if df_finnet is None or df_finnet.empty: return pd.DataFrame()
    df = df_finnet.copy()
    needed = ["Payment Date Time", "Merchant Amount", "Merchant Name", "Payment Method"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        st.warning("Settlement Finnet: kolom berikut tidak ditemukan di file: " + ", ".join(missing))
        return pd.DataFrame()

    raw_dt = df["Payment Date Time"].astype(str).str.strip()
    date_only_str = raw_dt.str.slice(0, 10)
    t = pd.to_datetime(date_only_str, errors="coerce")
    df["Tanggal"] = t.dt.date
    mask_period = (t.dt.year == year) & (t.dt.month == month)
    df = df.loc[mask_period].copy()
    if df.empty: return pd.DataFrame()

    mn = df["Merchant Name"].fillna("").astype(str).str.upper()
    def map_pelabuhan(name: str) -> str:
        if "BAKAUHENI" in name: return "ASDP Bakauheni"
        if "GILIMANUK" in name: return "ASDP Gilimanuk"
        if "KETAPANG" in name: return "ASDP Ketapang"
        if "MERAK" in name: return "ASDP Merak"
        return "ASDP Lainnya"
    df["Pelabuhan"] = mn.apply(map_pelabuhan)

    amt_raw = df["Merchant Amount"].astype(str).str.strip()
    amt_clean = amt_raw.str.replace(r"[^\d\-]", "", regex=True)
    amt = pd.to_numeric(amt_clean, errors="coerce").fillna(0.0)

    pm = df["Payment Method"].fillna("").astype(str)
    pm_lower = pm.str.lower()
    is_va = pm_lower.str.contains("va", na=False)
    is_bca = pm_lower.str.contains("bca", na=False) | pm_lower.str.contains("blu", na=False)
    is_emoney = ~is_va
    is_non_bca = ~(pm_lower.str.contains("bca", na=False) | pm_lower.str.contains("blu", na=False))

    df["VIRTUAL ACCOUNT"] = amt.where(is_va, 0.0)
    df["E-MONEY"] = amt.where(is_emoney, 0.0)
    df["BCA"] = amt.where(is_bca, 0.0)
    df["NON BCA"] = amt.where(is_non_bca, 0.0)

    grouped = df.groupby(["Tanggal", "Pelabuhan"], dropna=False)[
        ["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]
    ].sum().reset_index()
    grouped[["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]] = grouped[
        ["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]
    ].fillna(0.0)

    unique_ports = grouped["Pelabuhan"].dropna().unique()
    if len(unique_ports) == 0: return pd.DataFrame()

    days_in_month = monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days_in_month + 1)]
    full_idx = pd.MultiIndex.from_product([all_dates, unique_ports], names=["Tanggal", "Pelabuhan"])
    full_df = pd.DataFrame(index=full_idx).reset_index()

    out = full_df.merge(grouped, on=["Tanggal", "Pelabuhan"], how="left")
    for col in ["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]:
        if col not in out.columns: out[col] = 0.0
        else: out[col] = out[col].fillna(0.0)

    out["TOTAL VA + E-MONEY"] = out["VIRTUAL ACCOUNT"] + out["E-MONEY"]
    out["TOTAL BCA + NON BCA"] = out["BCA"] + out["NON BCA"]
    out = out.sort_values(["Pelabuhan", "Tanggal"]).reset_index(drop=True)

    desired_order = [
        "Tanggal","Pelabuhan","VIRTUAL ACCOUNT","E-MONEY","TOTAL VA + E-MONEY",
        "BCA","NON BCA","TOTAL BCA + NON BCA",
    ]
    existing = [c for c in desired_order if c in out.columns]
    others = [c for c in out.columns if c not in existing]
    out = out[existing + others]
    return out

# =========================== RK Non BCA (helpers) ===========================
def _extract_account_no_from_row7(content: bytes, filename: str) -> Optional[str]:
    pattern = re.compile(r"\b\d{4}-\d{2}-\d{6}-\d{2}-\d\b")
    low = filename.lower()
    if low.endswith(".xlsx"):
        try:
            wb = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
            ws = wb[wb.sheetnames[0]]
            vals = [str(c.value).strip() if c.value is not None else "" for c in next(ws.iter_rows(min_row=7, max_row=7))]
            wb.close()
            joined = " ".join(vals)
            m = pattern.search(joined)
            return m.group(0) if m else None
        except Exception:
            pass
    try:
        if low.endswith(".xlsb"):
            df7 = pd.read_excel(io.BytesIO(content), engine="pyxlsb", header=None, nrows=7)
        elif low.endswith((".xls", ".xlsx")):
            df7 = pd.read_excel(io.BytesIO(content), header=None, nrows=7)
        else:
            text = content.decode("utf-8-sig", errors="ignore")
            df7 = pd.read_csv(io.StringIO(text), header=None, nrows=7)
        if df7.shape[0] >= 7:
            row = df7.iloc[6].astype(str).fillna("").tolist()
            m = pattern.search(" ".join(row))
            return m.group(0) if m else None
    except Exception:
        return None
    return None

def _map_nonbca_account_to_port(account_no: Optional[str]) -> str:
    if not account_no: return "ASDP Lainnya"
    return NONBCA_ACC_TO_PORT.get(str(account_no).strip(), "ASDP Lainnya")

def _detect_excel_type(content: bytes) -> str:
    sig4 = content[:4]
    if sig4.startswith(b"\x50\x4B\x03\x04"): return "xlsx"
    if sig4.startswith(b"\xD0\xCF\x11\xE0"): return "xls"
    return "unknown"

def _read_any_table_with_header(content: bytes, filename: str, header_row: int) -> Optional[pd.DataFrame]:
    skiprows = range(0, max(header_row - 1, 0))
    low = str(filename).lower()
    ext = low.rsplit(".", 1)[-1] if "." in low else ""
    kind = _detect_excel_type(content) if ext in {"xls", "xlsx", "xlsm"} else ""
    try:
        if ext in {"xlsx", "xlsm"} or kind == "xlsx":
            try:
                return pd.read_excel(io.BytesIO(content), engine="openpyxl", skiprows=skiprows, header=0)
            except ImportError:
                st.warning("Butuh **openpyxl** untuk .xlsx/.xlsm. `pip install openpyxl`"); return None
        if ext == "xls" or kind == "xls":
            try:
                return pd.read_excel(io.BytesIO(content), engine="xlrd", skiprows=skiprows, header=0)
            except ImportError:
                st.warning("Butuh **xlrd** untuk .xls. `pip install xlrd`"); return None
            except Exception as e:
                if "xlsx file; not supported" in str(e).lower():
                    try:
                        return pd.read_excel(io.BytesIO(content), engine="openpyxl", skiprows=skiprows, header=0)
                    except Exception:
                        pass
                st.warning(f"Gagal membaca .xls: {e}"); return None
        if ext == "xlsb":
            try:
                return pd.read_excel(io.BytesIO(content), engine="pyxlsb", skiprows=skiprows, header=0)
            except ImportError:
                st.warning("Butuh **pyxlsb** untuk .xlsb. `pip install pyxlsb`"); return None
        text = content.decode("utf-8-sig", errors="ignore")
        return pd.read_csv(io.StringIO(text), skiprows=skiprows, header=0)
    except Exception:
        return None

def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cand = [_norm_colname(c) for c in candidates]
    for col in df.columns:
        lc = _norm_colname(col)
        if any((lc == c) or lc.startswith(c) or c.startswith(lc) for c in cand):
            return col
    return None

def _parse_amount_credit_series(s: pd.Series) -> pd.Series:
    x = s.astype(str)
    neg = (
        x.str.contains(r"\(", regex=True)
        | x.str.contains(r"\bDR\b", flags=re.I, regex=True)
        | x.str.contains("\u2212", regex=False)
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
        try:
            return float(val)
        except Exception:
            return 0.0

    vals = x.apply(_to_float)
    vals = vals.where(~neg, -vals.abs())
    return vals.round(0).astype("Int64")

def _remark_prefixes_from_mode(mode: str) -> List[str]:
    m = mode.lower()
    if "finon & finif" in m: return ["FINON", "FINIF"]
    if "finon" in m and "finif" not in m: return ["FINON"]
    if "finif" in m and "finon" not in m: return ["FINIF"]
    return ["FINON"]

def _mask_remark_startswith(remark: pd.Series, prefixes: List[str]) -> pd.Series:
    norm = remark.astype(str).str.upper().str.replace(r"[^A-Z0-9]", "", regex=True)
    pref = tuple(p.upper() for p in prefixes)
    return norm.str.startswith(pref, na=False)

def _port_from_filename(fname: str) -> str:
    up = str(fname).upper()
    if "MERAK" in up: return "ASDP Merak"
    if "BAKAUHENI" in up: return "ASDP Bakauheni"
    if "KETAPANG" in up: return "ASDP Ketapang"
    if "GILIMANUK" in up: return "ASDP Gilimanuk"
    return "ASDP Lainnya"

def _to_date_minus1(v: pd.Series) -> pd.Series:
    t = pd.to_datetime(v, errors="coerce", dayfirst=True)
    return t - pd.Timedelta(days=1)

def _load_rk_nonbca_inflow_by_dt_port_from_files(
    files: List["st.runtime.uploaded_file_manager.UploadedFile"],
    header_row: int,
    remark_mode: str,
) -> Dict[Tuple[date, str], float]:
    if not files: return {}
    prefixes = _remark_prefixes_from_mode(remark_mode)
    totals: Dict[Tuple[date, str], float] = defaultdict(float)

    def handle_one(content: bytes, fname: str):
        port = _port_from_filename(fname)
        df = _read_any_table_with_header(content, fname, header_row)
        if df is None or df.empty or df.shape[1] <= NONBCA_CREDIT_COL_INDEX: return
        c_date = _find_col(df, ["Date","Tanggal","Transaction Date","Tgl"])
        c_remark = _find_col(df, ["Remark","Keterangan","Description","Deskripsi"])
        if not c_date or not c_remark: return
        sub = pd.DataFrame({
            "Tanggal": _to_date_minus1(df[c_date]).dt.date,
            "Remark": df[c_remark].astype(str),
            "Amount": _parse_amount_credit_series(df.iloc[:, NONBCA_CREDIT_COL_INDEX]).astype("float64"),
        })
        sub = sub[sub["Tanggal"].notna()]
        if sub.empty: return
        mask = _mask_remark_startswith(sub["Remark"], prefixes)
        sub = sub.loc[mask, ["Tanggal", "Amount"]]
        if sub.empty: return
        g = sub.groupby("Tanggal")["Amount"].sum()
        for dt_val, amt in g.items():
            totals[(dt_val, _canonical_port_name(port))] += float(amt)

    for f in files:
        data = f.getvalue()
        if not data: continue
        name = f.name
        if str(name).lower().endswith(".zip"):
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for info in zf.infolist():
                        if info.is_dir(): continue
                        if not info.filename.lower().endswith((".csv", ".xls", ".xlsx", ".xlsb")): continue
                        handle_one(zf.read(info), info.filename)
            except Exception:
                continue
        else:
            handle_one(data, name)

    return dict(totals)

# =========================== RK BCA (untuk Dana Masuk BCA) ===========================
def _read_rek_koran_base(content: bytes, remark_codes: List[str]) -> Optional[pd.DataFrame]:
    def try_read_excel() -> Optional[pd.DataFrame]:
        for eng in (None, "openpyxl", "pyxlsb"):
            try:
                if eng: return pd.read_excel(io.BytesIO(content), skiprows=range(0, 13), engine=eng)
                else:   return pd.read_excel(io.BytesIO(content), skiprows=range(0, 13))
            except Exception:
                continue
        return None
    def try_read_csv() -> Optional[pd.DataFrame]:
        try:
            text = content.decode("utf-8-sig", errors="ignore")
            return pd.read_csv(io.StringIO(text), skiprows=range(0, 13))
        except Exception:
            return None
    df = try_read_excel();  df = df if df is not None else try_read_csv()
    if df is None or df.empty: return None

    cols = [str(c) for c in df.columns]
    date_col = remark_col = amount_col = None
    for c in cols:
        lc = c.lower()
        if date_col is None and ("tanggal" in lc or "date" in lc): date_col = c
        if remark_col is None and ("remark" in lc or "keterangan" in lc or "description" in lc): remark_col = c
    for c in cols:
        lc = c.strip().lower()
        if lc in ("credit","kredit"): amount_col = c; break
    if amount_col is None:
        for c in cols:
            lc = c.lower()
            if any(k in lc for k in ["kredit","credit","amount","nominal"]):
                amount_col = c; break
    if date_col is None or remark_col is None or amount_col is None: return None

    if remark_codes:
        norm = _normalize_alnum_upper(df[remark_col].astype(str))
        prefixes = tuple(re.sub(r"[^A-Z0-9]", "", p) for p in remark_codes)
        mask = norm.str.startswith(prefixes, na=False)
        df_filt = df.loc[mask].copy()
    else:
        df_filt = df.copy()
    if df_filt.empty: return None

    t = pd.to_datetime(df_filt[date_col], errors="coerce", dayfirst=True)
    df_filt["Tanggal"] = t.dt.date
    df_filt = df_filt[df_filt["Tanggal"].notna()].copy()
    if df_filt.empty: return None

    amt_raw = df_filt[amount_col].astype(str).str.strip()
    amt_clean = amt_raw.str.replace(r"[^\d\-]", "", regex=True)
    amt = pd.to_numeric(amt_clean, errors="coerce").fillna(0.0)
    df_filt["Amount"] = amt
    return df_filt

def _read_rek_koran_single(content: bytes, remark_codes: List[str]) -> Dict[date, float]:
    df_filt = _read_rek_koran_base(content, remark_codes)
    if df_filt is None or df_filt.empty: return {}
    grouped = df_filt.groupby("Tanggal")["Amount"].sum()
    return {dt: float(val) for dt, val in grouped.items()}

def _load_rek_koran(files: List["st.runtime.uploaded_file_manager.UploadedFile"], remark_codes: List[str]) -> Dict[date, float]:
    total_map: Dict[date, float] = defaultdict(float)
    if not files: return {}
    for f in files:
        data = f.getvalue()
        if data is None: continue
        name = f.name.lower()
        try:
            if name.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for info in zf.infolist():
                        if info.is_dir(): continue
                        low = info.filename.lower()
                        if not low.endswith((".xlsx",".xls",".xlsb",".csv")): continue
                        content = zf.read(info)
                        m = _read_rek_koran_single(content, remark_codes)
                        for dt, val in (m or {}).items(): total_map[dt] += val
            else:
                m = _read_rek_koran_single(data, remark_codes)
                for dt, val in (m or {}).items(): total_map[dt] += val
        except Exception:
            continue
    return dict(total_map)

# =========================== Tabel Rekonsiliasi Finnet ===========================
def _build_finnet_rekon_table(
    agg,
    df_finnet_settlement: Optional[pd.DataFrame],
    year: int,
    month: int,
    bca_inflow_by_date: Optional[Dict[date, float]] = None,
    nonbca_inflow_by_dt_port: Optional[Dict[Tuple[date, str], float]] = None,
) -> pd.DataFrame:
    ports_from_payment = {_canonical_port_name(asal) for (_, asal) in agg.keys() if asal is not None}
    ports_from_settle = set()
    if df_finnet_settlement is not None and not df_finnet_settlement.empty and "Pelabuhan" in df_finnet_settlement.columns:
        ports_from_settle = set(df_finnet_settlement["Pelabuhan"].dropna().apply(_canonical_port_name).unique())
    unique_ports = sorted(ports_from_payment.union(ports_from_settle))
    if not unique_ports: return pd.DataFrame()

    days_in_month = monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days_in_month + 1)]
    base_idx = pd.MultiIndex.from_product([all_dates, unique_ports], names=["Tanggal", "Pelabuhan"])
    base_df = pd.DataFrame(index=base_idx).reset_index()

    rows = []
    for (dt, asal), bucket in agg.items():
        asal_norm = _canonical_port_name(asal)
        if asal_norm not in unique_ports: continue
        dt_val = dt.date() if isinstance(dt, pd.Timestamp) else dt
        if dt_val is None or dt_val.year != year or dt_val.month != month: continue
        bca_val = float(bucket.get("FINNET_TIKET_BCA", 0.0))
        non_bca_val = float(bucket.get("FINNET_TIKET_NON_BCA", 0.0))
        if bca_val == 0.0 and non_bca_val == 0.0: continue
        rows.append({"Tanggal": dt_val, "Pelabuhan": asal_norm, "Tiket_BCA": bca_val, "Tiket_NON_BCA": non_bca_val})

    ticket_df = (
        pd.DataFrame(rows).groupby(["Tanggal", "Pelabuhan"], as_index=False)[["Tiket_BCA","Tiket_NON_BCA"]].sum()
        if rows else pd.DataFrame(columns=["Tanggal","Pelabuhan","Tiket_BCA","Tiket_NON_BCA"])
    )

    if df_finnet_settlement is not None and not df_finnet_settlement.empty:
        needed_cols = [c for c in ["Tanggal","Pelabuhan","BCA","NON BCA"] if c in df_finnet_settlement.columns]
        if len(needed_cols) == 4:
            settle_df = df_finnet_settlement[needed_cols].copy()
            settle_df["Pelabuhan"] = settle_df["Pelabuhan"].apply(_canonical_port_name)
            settle_df = settle_df.groupby(["Tanggal","Pelabuhan"], as_index=False)[["BCA","NON BCA"]].sum()
        else:
            settle_df = pd.DataFrame(columns=["Tanggal","Pelabuhan","BCA","NON BCA"])
    else:
        settle_df = pd.DataFrame(columns=["Tanggal","Pelabuhan","BCA","NON BCA"])

    out = base_df.copy()
    if not ticket_df.empty: out = out.merge(ticket_df, on=["Tanggal","Pelabuhan"], how="left")
    if not settle_df.empty: out = out.merge(settle_df, on=["Tanggal","Pelabuhan"], how="left")

    for col in ["Tiket_BCA","Tiket_NON_BCA","BCA","NON BCA"]:
        if col not in out.columns: out[col] = 0.0
        else: out[col] = out[col].fillna(0.0)

    out["Tiket Detail - BCA"] = out["Tiket_BCA"]
    out["Tiket Detail - Non BCA"] = out["Tiket_NON_BCA"]
    out["Settlement Report - BCA"] = out["BCA"]
    out["Settlement Report - Non BCA"] = out["NON BCA"]

    bca_map = bca_inflow_by_date or {}
    nonbca_map = nonbca_inflow_by_dt_port or {}
    out["Dana Masuk - BCA"] = out["Tanggal"].map(lambda d: bca_map.get(d, 0.0)).astype(float)
    out["Dana Masuk - Non BCA"] = out.apply(
        lambda r: float(nonbca_map.get((r["Tanggal"], _canonical_port_name(r["Pelabuhan"])), 0.0)), axis=1
    )

    # === Total & Selisih (ditaruh sebelum kolom selisih, lalu selisih di kanan Total Dana Masuk) ===
    out["Total Tiket Detail"] = out["Tiket Detail - BCA"] + out["Tiket Detail - Non BCA"]
    out["Total Settlement Report"] = out["Settlement Report - BCA"] + out["Settlement Report - Non BCA"]
    out["Total Dana Masuk"] = out["Dana Masuk - BCA"] + out["Dana Masuk - Non BCA"]

    # Kolom selisih (kanan dari Total Dana Masuk)
    out["Selisih Tiket Detail vs Settlement Report"] = out["Total Tiket Detail"] - out["Total Settlement Report"]
    out["Selisih Dana Masuk vs Settlement Report"] = out["Total Dana Masuk"] - out["Total Settlement Report"]

    out = out.sort_values(["Pelabuhan","Tanggal"]).reset_index(drop=True)
    final_cols = [
        "Tanggal","Pelabuhan",
        "Tiket Detail - BCA","Tiket Detail - Non BCA","Total Tiket Detail",
        "Settlement Report - BCA","Settlement Report - Non BCA","Total Settlement Report",
        "Dana Masuk - BCA","Dana Masuk - Non BCA","Total Dana Masuk",
        "Selisih Tiket Detail vs Settlement Report","Selisih Dana Masuk vs Settlement Report",
    ]
    return out[final_cols]

# =========================== Streamlit UI helpers ===========================
def _to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Rekonsiliasi") -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
    for engine in ("xlsxwriter","openpyxl"):
        try:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine=engine) as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            return buf.getvalue(), engine, None
        except ImportError:
            continue
        except Exception as e:
            return None, None, f"Gagal menulis Excel dengan {engine}: {e}"
    return None, None, "Tidak ada engine Excel (xlsxwriter/openpyxl)."

def _render_port_table(port_name: str, df_port: pd.DataFrame, highlight: bool) -> None:
    df_show = df_port.copy()
    df_show["Tanggal"] = pd.to_datetime(df_show["Tanggal"]).dt.strftime("%d/%m/%Y")
    df_show = _add_subtotal_row(df_show, label="Subtotal", date_col="Tanggal")
    numeric_cols = df_show.select_dtypes(include="number").columns
    df_show[numeric_cols] = df_show[numeric_cols].round(0).astype("Int64")
    try:
        st.dataframe(_style_table(df_show, highlight=highlight), use_container_width=True)
    except Exception:
        st.dataframe(df_show, use_container_width=True)

def _render_espay_port_table(df_port: pd.DataFrame) -> None:
    df_show = df_port.copy()
    df_show["Tanggal"] = pd.to_datetime(df_show["Tanggal"]).dt.strftime("%d/%m/%Y")
    df_show = _add_subtotal_row(df_show, label="Subtotal", date_col="Tanggal")
    numeric_cols = df_show.select_dtypes(include="number").columns
    df_show[numeric_cols] = df_show[numeric_cols].fillna(0).round(0).astype("Int64")
    st.dataframe(df_show, use_container_width=True)

def _render_finnet_port_table(df_port: pd.DataFrame) -> None:
    df_show = df_port.copy()
    df_show["Tanggal"] = pd.to_datetime(df_show["Tanggal"]).dt.strftime("%d/%m/%Y")
    df_show = _add_subtotal_row(df_show, label="Subtotal", date_col="Tanggal")
    numeric_cols = df_show.select_dtypes(include="number").columns
    df_show[numeric_cols] = df_show[numeric_cols].fillna(0).round(0).astype("Int64")
    col_rename = {
        "VIRTUAL ACCOUNT": "Virtual Account",
        "E-MONEY": "E-Money",
        "TOTAL VA + E-MONEY": "Total VA + E-Money",
        "BCA": "BCA",
        "NON BCA": "Non BCA",
        "TOTAL BCA + NON BCA": "Total BCA + Non BCA",
    }
    df_show.rename(columns=col_rename, inplace=True)
    st.dataframe(df_show, use_container_width=True)

def _render_finnet_rekon_port_table(df_port: pd.DataFrame) -> None:
    df_show = df_port.copy()
    df_show["Tanggal"] = pd.to_datetime(df_show["Tanggal"]).dt.strftime("%d/%m/%Y")
    df_show = _add_subtotal_row(df_show, label="Subtotal", date_col="Tanggal")
    numeric_cols = df_show.select_dtypes(include="number").columns
    df_show[numeric_cols] = df_show[numeric_cols].fillna(0).round(0).astype("Int64")
    st.dataframe(df_show, use_container_width=True)

# =========================== MAIN ===========================
def main() -> None:
    st.set_page_config(page_title="Rekonsiliasi Payment Report", layout="wide")
    st.title("Rekonsiliasi Payment Report")

    today = date.today()
    years_options = list(range(today.year - 5, today.year + 6))
    year = st.sidebar.selectbox("Tahun", options=years_options, index=years_options.index(today.year))
    month_names = {
        1: "01 - Januari", 2: "02 - Februari", 3: "03 - Maret", 4: "04 - April",
        5: "05 - Mei", 6: "06 - Juni", 7: "07 - Juli", 8: "08 - Agustus",
        9: "09 - September", 10: "10 - Oktober", 11: "11 - November", 12: "12 - Desember",
    }
    month = st.sidebar.selectbox("Bulan", options=list(range(1, 13)), index=today.month - 1,
                                 format_func=lambda m: month_names[m])

    if "upload_rev" not in st.session_state:
        st.session_state.upload_rev = 0
    if st.sidebar.button("🔄 Reset semua upload"):
        st.session_state.upload_rev += 1

    up_files = st.sidebar.file_uploader(
        "Upload Payment Report: ZIP / beberapa Excel (.xlsx/.xls/.xlsb) / CSV",
        type=["zip", "xlsx", "xls", "xlsb", "csv"], accept_multiple_files=True,
        key=f"payment_{st.session_state.upload_rev}",
    )
    settlement_files = st.sidebar.file_uploader(
        "Upload Settlement ESPAY (ZIP / .csv)", type=["zip", "csv"],
        accept_multiple_files=True, key=f"settlement_espay_{st.session_state.upload_rev}",
    )
    finnet_files = st.sidebar.file_uploader(
        "Upload Settlement Finnet by Telkom (ZIP / .csv)", type=["zip", "csv"],
        accept_multiple_files=True, key=f"settlement_finnet_{st.session_state.upload_rev}",
    )
    finnet_espay_files = st.sidebar.file_uploader(
        "Upload Settlement Finnet (Espay) (ZIP / .csv)", type=["zip", "csv"],
        accept_multiple_files=True, key=f"settlement_finnet_espay_{st.session_state.upload_rev}",
    )
    rek_bca_files = st.sidebar.file_uploader(
        "Upload Rekening Koran BCA", type=["zip", "xlsx", "xls", "xlsb", "csv"],
        accept_multiple_files=True, key=f"rek_bca_{st.session_state.upload_rev}",
    )
    rek_nonbca_files = st.sidebar.file_uploader(
        "Upload Rekening Koran Non BCA", type=["zip", "xlsx", "xls", "xlsb", "csv"],
        accept_multiple_files=True, key=f"rek_nonbca_{st.session_state.upload_rev}",
    )

    highlight = st.sidebar.checkbox("Highlight kolom Selisih ≠ 0 (Payment Report)", value=True)

    # ===== Payment Report =====
    if not up_files:
        st.info("Silakan upload file Payment Report di panel kiri (bisa banyak file atau ZIP) untuk melanjutkan rekonsiliasi.")
        return
    with st.spinner("Memproses file Payment Report secara streaming…"):
        agg = _load_and_aggregate(up_files, year=year, month=month)
    result = _build_result_from_agg(agg)
    if result.empty:
        st.warning("Tidak ada data valid setelah filter periode & kolom wajib.")
        return

    st.subheader(f"Hasil Rekonsiliasi Payment • Periode: {month_names[month]} {year}")
    ports = sorted(result["Pelabuhan"].dropna().unique())
    tabs = st.tabs(ports if ports else ["(Tidak ada Pelabuhan)"])
    for tab, port in zip(tabs, ports):
        with tab:
            st.markdown(f"**Pelabuhan: {port}**")
            _render_port_table(port, result[result["Pelabuhan"] == port], highlight=highlight)

    # ===== Settlement ESPAY =====
    st.divider()
    st.subheader("DETAIL SETTLEMENT ESPAY")
    if settlement_files:
        with st.spinner("Memproses Settlement ESPAY…"):
            df_settlement_raw = _load_settlement_espay(settlement_files)
            df_espay = _build_espay_settlement_table(df_settlement_raw, year=year, month=month)
        if df_espay.empty:
            st.warning("File Settlement ESPAY tidak memiliki data lengkap / tidak ada untuk periode yang dipilih.")
        else:
            ports_espay = sorted(df_espay["Pelabuhan"].dropna().unique())
            tabs_espay = st.tabs(ports_espay if ports_espay else ["(Tidak ada Pelabuhan)"])
            for tab, port in zip(tabs_espay, ports_espay):
                with tab:
                    st.markdown(f"**Pelabuhan: {port}**")
                    _render_espay_port_table(df_espay[df_espay["Pelabuhan"] == port])
    else:
        st.info("Belum ada file Settlement ESPAY yang di-upload.")

    # ===== Settlement Finnet by Telkom =====
    st.divider()
    st.subheader("DETAIL SETTLEMENT FINNET BY TELKOM")
    df_finnet = None
    if finnet_files:
        with st.spinner("Memproses Settlement Finnet…"):
            try:
                df_finnet_raw = _load_settlement_finnet(finnet_files)
            except NameError:
                st.error("Internal: fungsi _load_settlement_finnet tidak ditemukan.")
                df_finnet_raw = pd.DataFrame()
            df_finnet = _build_finnet_settlement_table(df_finnet_raw, year=year, month=month)
        if df_finnet is None or df_finnet.isna().all(axis=None) or df_finnet.empty:
            st.warning("File Settlement Finnet tidak memiliki data lengkap / tidak ada untuk periode yang dipilih.")
        else:
            ports_finnet = sorted(df_finnet["Pelabuhan"].dropna().unique())
            tabs_finnet = st.tabs(ports_finnet if ports_finnet else ["(Tidak ada Pelabuhan)"])
            for tab, port in zip(tabs_finnet, ports_finnet):
                with tab:
                    st.markdown(f"**Pelabuhan: {port}**")
                    _render_finnet_port_table(df_finnet[df_finnet["Pelabuhan"] == port])
    else:
        st.info("Belum ada file Settlement Finnet (Telkom) yang di-upload.")

    # ===== Rekap Settlement Finnet (Espay) =====
    st.divider()
    st.subheader("REKAP SETTLEMENT FINNET (ESPAY) PER PELABUHAN")
    if finnet_espay_files:
        with st.spinner("Memproses Settlement Finnet (Espay)…"):
            df_finnet_espay_raw = _load_settlement_finnet(finnet_espay_files)
            df_finnet_espay = _build_finnet_settlement_table(df_finnet_espay_raw, year=year, month=month)
        if df_finnet_espay is None or df_finnet_espay.empty:
            st.warning("File Settlement Finnet (Espay) tidak memiliki data lengkap / tidak ada untuk periode yang dipilih.")
        else:
            ports_finnet_espay = sorted(df_finnet_espay["Pelabuhan"].dropna().unique())
            tabs_finnet_espay = st.tabs(ports_finnet_espay if ports_finnet_espay else ["(Tidak ada Pelabuhan)"])
            for tab, port in zip(tabs_finnet_espay, ports_finnet_espay):
                with tab:
                    st.markdown(f"**Pelabuhan: {port}**")
                    _render_finnet_port_table(df_finnet_espay[df_finnet_espay["Pelabuhan"] == port])
    else:
        st.info("Belum ada file Settlement Finnet (Espay) yang di-upload.")

    # ===== Rekonsiliasi Gabungan: Finnet =====
    st.divider()
    st.subheader("TABEL REKONSILIASI GABUNGAN PAYMENT - SETTLEMENT DANA - REKENING KORAN")
    st.markdown("**1. Tabel Rekonsiliasi Finnet**")

    bca_inflow_by_date = _load_rek_koran(rek_bca_files, ["FINIF"]) if rek_bca_files else {}
    nonbca_inflow_by_dt_port = _load_rk_nonbca_inflow_by_dt_port_from_files(
        rek_nonbca_files, header_row=13, remark_mode="FINON & FINIF"
    ) if rek_nonbca_files else {}

    df_rekon_finnet = _build_finnet_rekon_table(
        agg, df_finnet, year=year, month=month,
        bca_inflow_by_date=bca_inflow_by_date,
        nonbca_inflow_by_dt_port=nonbca_inflow_by_dt_port,
    )

    if df_rekon_finnet.empty:
        st.warning("Tabel Rekonsiliasi Finnet belum dapat dibentuk.")
    else:
        ports_rekon = sorted(df_rekon_finnet["Pelabuhan"].dropna().unique())
        combo_label = "ASDP Gilimanuk + ASDP Ketapang"
        combo_ports = {"ASDP Gilimanuk", "ASDP Ketapang"}
        df_combo_src = df_rekon_finnet[df_rekon_finnet["Pelabuhan"].isin(combo_ports)].copy()
        show_labels = ports_rekon.copy()
        if not df_combo_src.empty:
            show_labels.append(combo_label)

        tabs_rekon = st.tabs(show_labels if show_labels else ["(Tidak ada Pelabuhan)"])
        for tab, label in zip(tabs_rekon, show_labels):
            with tab:
                if label == combo_label:
                    df_combo = df_combo_src.drop(columns=["Pelabuhan"], errors="ignore")
                    num_cols = df_combo.select_dtypes(include="number").columns
                    df_combo = df_combo.groupby("Tanggal", as_index=False)[num_cols].sum()
                    st.markdown("**Pelabuhan: ASDP Gilimanuk + ASDP Ketapang (gabungan)**")
                    _render_finnet_rekon_port_table(df_combo)
                else:
                    st.markdown(f"**Pelabuhan: {label}**")
                    _render_finnet_rekon_port_table(df_rekon_finnet[df_rekon_finnet["Pelabuhan"] == label])

    # ===== Unduh hasil Payment =====
    st.divider()
    st.subheader("Unduh Hasil Payment (Gabungan Semua Pelabuhan)")
    export_df = result.copy()
    export_df["Tanggal"] = pd.to_datetime(export_df["Tanggal"]).dt.strftime("%d/%m/%Y")
    num_cols = export_df.select_dtypes(include="number").columns
    export_df[num_cols] = export_df[num_cols].round(0).astype("Int64")

    csv_bytes = export_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "Unduh CSV (Gabungan Payment)",
        data=csv_bytes,
        file_name=f"rekonsiliasi_payment_{year}_{month:02d}_per_pelabuhan.csv",
        mime="text/csv",
    )
    excel_bytes, engine_used, err_msg = _to_excel_bytes(export_df, sheet_name="Rekonsiliasi")
    if excel_bytes:
        st.download_button(
            f"Unduh Excel (.xlsx) (Gabungan Payment){' • ' + engine_used if engine_used else ''}",
            data=excel_bytes,
            file_name=f"rekonsiliasi_payment_{year}_{month:02d}_per_pelabuhan.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.warning("Ekspor Excel gagal. Tambahkan `xlsxwriter` atau `openpyxl` di requirements."
                   + (f"\nDetail: {err_msg}" if err_msg else ""))

if __name__ == "__main__":
    main()

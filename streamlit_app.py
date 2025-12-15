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

# ---- Streamlit conf (hindari SessionInfo error) ----
st.set_page_config(page_title="Rekonsiliasi Payment Report", layout="wide")


# =========================== Konfigurasi & Konstanta ===========================

COL_H = "TIPE PEMBAYARAN"                      # H
COL_B = "TANGGAL PEMBAYARAN"                   # B
COL_AA = "REF NO"                              # AA
COL_K = "TOTAL TARIF TANPA BIAYA ADMIN (Rp.)"  # K
COL_X = "SOF ID"                               # X
COL_ASAL = "ASAL"                              # (Pelabuhan)
REQUIRED_COLS = [COL_H, COL_B, COL_AA, COL_K, COL_X, COL_ASAL]

CAT_COLS = [
    "Cash", "Prepaid BRI", "Prepaid BNI", "Prepaid Mandiri", "Prepaid BCA",
    "SKPT", "IFCS", "Reedem", "ESPAY", "Finnet",
]
NON_COMPONENTS = ["Cash", "Prepaid BRI", "Prepaid BNI", "Prepaid Mandiri", "Prepaid BCA", "SKPT", "IFCS", "Reedem"]

CSV_CHUNK_ROWS = 200_000
XLSX_BATCH_ROWS = 50_000
VALID_EXTS = (".xlsx", ".xls", ".xlsb", ".csv")

SETTLEMENT_REQUIRED_COLS = ["Product Name", "Settlement Amount", "Settlement Date", "VA NAME"]
FINNET_REQUIRED_COLS = ["Payment Method", "Merchant Amount", "Payment Date Time", "Merchant Name"]

NONBCA_ACC_TO_PORT = {"0188-01-000735-30-4": "ASDP Merak"}  # mapping Non-BCA → pelabuhan
NONBCA_CREDIT_COL_INDEX = 9  # kolom J (0-based)


# =========================== Utilities ===========================

def ss_get_set(key: str, default):
    if key not in st.session_state:
        st.session_state[key] = default
    return st.session_state[key]

def _style_table(df_display: pd.DataFrame, highlight: bool) -> "pd.io.formats.style.Styler":
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
    # handle lokal format, CR/DR, kurung, minus unicode
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
        kk = str(k).upper()
        mask = mask | norm.str.contains(kk, na=False)
    return mask

def _read_any_table_with_header(content: bytes, filename: str, header_row: int) -> Optional[pd.DataFrame]:
    skiprows = range(0, max(header_row - 1, 0))
    low = str(filename).lower()
    ext = low.rsplit(".", 1)[-1] if "." in low else ""
    try:
        if ext in {"xlsx", "xlsm"}:
            try:
                return pd.read_excel(io.BytesIO(content), engine="openpyxl", skiprows=skiprows, header=0)
            except ImportError:
                st.warning("Butuh openpyxl untuk .xlsx/.xlsm (`pip install openpyxl`).")
                return None
        if ext == "xls":
            try:
                return pd.read_excel(io.BytesIO(content), engine="xlrd", skiprows=skiprows, header=0)
            except ImportError:
                st.warning("Butuh xlrd untuk .xls (`pip install xlrd`).")
                return None
        if ext == "xlsb":
            try:
                return pd.read_excel(io.BytesIO(content), engine="pyxlsb", skiprows=skiprows, header=0)
            except ImportError:
                st.warning("Butuh pyxlsb untuk .xlsb (`pip install pyxlsb`).")
                return None
        text = content.decode("utf-8-sig", errors="ignore")
        return pd.read_csv(io.StringIO(text), skiprows=skiprows, header=0)
    except Exception:
        return None

def _read_bca_table_row2(content: bytes) -> Optional[pd.DataFrame]:
    df = None
    for eng in ("openpyxl", "xlrd", "pyxlsb", None):
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
            text = content.decode("utf-8-sig", errors="ignore")
            df = pd.read_csv(io.StringIO(text), header=0)
        except Exception:
            return None
    return df if (df is not None and not df.empty) else None

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
    if "MERAK" in up:
        return "ASDP Merak"
    if ("BEKAUHENI" in up) or ("BAKAUHENI" in up):
        return "ASDP Bakauheni"
    if "KETAPANG" in up:
        return "ASDP Bakauheni"  # sesuai instruksi
    if "GILIMANUK" in up:
        return "ASDP Gilimanuk"
    return "ASDP Lainnya"


# =========================== Agregator Payment (streaming) ===========================

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

    is_spay = X.str.contains("spay", na=False)
    _update_agg_series(agg, sum_by_key(is_spay & is_bca_tag), "ESPAY_TIKET_BCA")
    _update_agg_series(agg, sum_by_key(is_spay & (~is_bca_tag)), "ESPAY_TIKET_NON_BCA")

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
        if not mask.any():
            continue
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

def _flush_xlsx_batch(buf: List[List], year: int, month: int, agg) -> None:
    df = pd.DataFrame(buf, columns=[COL_H, COL_B, COL_AA, COL_K, COL_X, COL_ASAL])
    t = pd.to_datetime(df[COL_B], errors="coerce")
    mask = (t.dt.year == year) & (t.dt.month == month)
    if not mask.any(): return
    sub = df.loc[mask].copy(); sub["Tanggal"] = t.loc[mask].dt.date
    _apply_rules_and_update(sub, agg)

def _load_and_aggregate(files: List["st.runtime.uploaded_file_manager.UploadedFile"], year: int, month: int):
    agg = _empty_agg()
    for f in files:
        try: data = f.getvalue()
        except Exception: data = f.read()
        name = f.name.lower()
        try:
            if name.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for m in zf.infolist():
                        if m.is_dir(): continue
                        low = m.filename.lower()
                        if not low.endswith(VALID_EXTS): continue
                        content = zf.read(m)
                        if low.endswith((".xlsx", ".xls")): _process_xlsx_streaming(content, year, month, agg)
                        elif low.endswith(".xlsb"): _process_xlsb(content, year, month, agg)
                        else: _process_csv_fast(content, year, month, agg)
            elif name.endswith((".xlsx", ".xls")): _process_xlsx_streaming(data, year, month, agg)
            elif name.endswith(".xlsb"): _process_xlsb(data, year, month, agg)
            elif name.endswith(".csv"): _process_csv_fast(data, year, month, agg)
        except Exception:
            continue
    return agg

def _build_result_from_agg(agg) -> pd.DataFrame:
    if not agg: return pd.DataFrame()
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
    if df.empty: return df
    df = df[["Tanggal", "Pelabuhan"] + CAT_COLS + ["Total", "BCA", "NON BCA", "NON", "TOTAL", "Selisih"]]
    return df.sort_values(["Pelabuhan", "Tanggal"]).reset_index(drop=True)


# =========================== Settlement ESPAY (CSV/XLSX ONLY) ===========================

def _read_settlement_single_table(content: bytes, filename: str) -> Optional[pd.DataFrame]:
    # menerima .csv atau .xlsx
    low = str(filename).lower()
    df = None
    try:
        if low.endswith(".csv"):
            text = content.decode("utf-8-sig", errors="ignore")
            df = pd.read_csv(io.StringIO(text), sep=",")
        elif low.endswith(".xlsx"):
            try:
                df = pd.read_excel(io.BytesIO(content), engine="openpyxl")
            except ImportError:
                st.warning("Butuh openpyxl untuk membaca .xlsx. Jalankan: `pip install openpyxl`")
                return None
        else:
            return None
    except Exception:
        return None

    if df is None or df.empty:
        return None
    df.rename(columns={c: str(c).strip() for c in df.columns}, inplace=True)

    lower_to_real = {str(c).strip().lower(): c for c in df.columns}
    rename_map = {}
    for req in SETTLEMENT_REQUIRED_COLS:
        key = req.lower()
        if key in lower_to_real:
            rename_map[lower_to_real[key]] = req
    df.rename(columns=rename_map, inplace=True)

    missing = [c for c in SETTLEMENT_REQUIRED_COLS if c not in df.columns]
    if missing:
        return None
    return df[SETTLEMENT_REQUIRED_COLS].copy()

def _load_settlement_espay(files: List["st.runtime.uploaded_file_manager.UploadedFile"]) -> pd.DataFrame:
    all_dfs: List[pd.DataFrame] = []
    for f in files:
        try: data = f.getvalue()
        except Exception: data = f.read()
        if not data: continue
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
    amt = pd.to_numeric(amt_raw.str.replace(r"[^\d\-]", "", regex=True), errors="coerce").fillna(0.0) / 100.0

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


# =========================== Settlement FINNET (helpers) ===========================

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
                rename_map[real] = req
                break
    df.rename(columns=rename_map, inplace=True)
    return df

def _load_settlement_finnet(files: List["st.runtime.uploaded_file_manager.UploadedFile"]) -> pd.DataFrame:
    all_dfs: List[pd.DataFrame] = []
    for f in files:
        try: data = f.getvalue()
        except Exception: data = f.read()
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
    is_emoney = ~is_va
    is_non_bca = ~(pm.str.contains("bca", na=False) | pm.str.contains("blu", na=False))

    df["VIRTUAL ACCOUNT"] = amt.where(is_va, 0.0)
    df["E-MONEY"] = amt.where(~is_va, 0.0)
    df["BCA"] = amt.where(is_bca, 0.0)
    df["NON BCA"] = amt.where(is_non_bca, 0.0)

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


# =========================== RK Loaders (route by remark) ===========================

def _load_rk_bca_sgw_by_dt_port(files) -> Dict[Tuple[date, str], float]:
    totals: Dict[Tuple[date, str], float] = defaultdict(float)
    if not files: return {}
    def extract_sgw_tgl_amount(df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if df is None or df.empty: return None
        cols = list(df.columns); norm = {c: _norm_colname(c) for c in cols}
        c_tgl = next((c for c in cols if norm[c] in {"tanggal","date","transactiondate","tgl"} or "tanggal" in norm[c] or "date" in norm[c]), None)
        c_ket = next((c for c in cols if any(k in norm[c] for k in ["keterangan","remark","description","deskripsi"])), None)
        c_amt = next((c for c in cols if norm[c] in {"mutasi","credit","kredit","amount","nominal"} or norm[c]=="mutasi"), None)
        if not (c_tgl and c_ket and c_amt): return None
        t = pd.to_datetime(df[c_tgl], errors="coerce", dayfirst=True)
        sub = pd.DataFrame({"Tanggal": t.dt.date, "Keterangan": df[c_ket].astype(str), "Amount": _parse_amount_credit_series(df[c_amt]).astype("float64")})
        sub = sub[sub["Tanggal"].notna()]
        sub = sub[_mask_remark_contains(sub["Keterangan"], ["SGW"])]
        return sub[["Tanggal","Amount"]] if not sub.empty else None
    def handle_one(content: bytes, fname: str):
        port = _port_from_bca_filename(fname)
        df = _read_bca_table_row2(content)
        if df is None or df.empty: return
        part = extract_sgw_tgl_amount(df)
        if part is None or part.empty: return
        for dt_val, amt in part.groupby("Tanggal")["Amount"].sum().items():
            totals[(dt_val, port)] += float(amt)
    for f in files:
        try: data = f.getvalue()
        except Exception: data = f.read()
        if not data: continue
        fname = f.name
        try:
            if fname.lower().endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for info in zf.infolist():
                        if info.is_dir(): continue
                        if not info.filename.lower().endswith((".xlsx",".xls",".xlsb",".csv")): continue
                        handle_one(zf.read(info), info.filename)
            else:
                handle_one(data, fname)
        except Exception:
            continue
    return dict(totals)

def _load_rk_bca_finif_by_dt_port(files) -> Dict[Tuple[date, str], float]:
    totals: Dict[Tuple[date, str], float] = defaultdict(float)
    if not files: return {}
    def extract_finxx_tgl_amount(df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if df is None or df.empty: return None
        cols = list(df.columns); norm = {c: _norm_colname(c) for c in cols}
        c_tgl = next((c for c in cols if norm[c] in {"tanggal","date","transactiondate","tgl"} or "tanggal" in norm[c] or "date" in norm[c]), None)
        c_ket = next((c for c in cols if any(k in norm[c] for k in ["keterangan","remark","description","deskripsi"])), None)
        c_amt = next((c for c in cols if norm[c] in {"mutasi","credit","kredit","amount","nominal"} or norm[c]=="mutasi"), None)
        if not (c_tgl and c_ket and c_amt): return None
        t = pd.to_datetime(df[c_tgl], errors="coerce", dayfirst=True)
        sub = pd.DataFrame({"Tanggal": t.dt.date, "Keterangan": df[c_ket].astype(str), "Amount": _parse_amount_credit_series(df[c_amt]).astype("float64")})
        sub = sub[sub["Tanggal"].notna()]
        sub = sub[_mask_remark_contains(sub["Keterangan"], ["FINIF","FINON"])]
        return sub[["Tanggal","Amount"]] if not sub.empty else None
    def handle_one(content: bytes, fname: str):
        port = _port_from_bca_filename(fname)
        df = _read_bca_table_row2(content)
        if df is None or df.empty: return
        part = extract_finxx_tgl_amount(df)
        if part is None or part.empty: return
        for dt_val, amt in part.groupby("Tanggal")["Amount"].sum().items():
            totals[(dt_val, port)] += float(amt)
    for f in files:
        try: data = f.getvalue()
        except Exception: data = f.read()
        if not data: continue
        fname = f.name
        try:
            if fname.lower().endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for info in zf.infolist():
                        if info.is_dir(): continue
                        if not info.filename.lower().endswith((".xlsx",".xls",".xlsb",".csv")): continue
                        handle_one(zf.read(info), info.filename)
            else:
                handle_one(data, fname)
        except Exception:
            continue
    return dict(totals)

def _load_rk_nonbca_inflow_by_dt_port_from_files(files, header_row: int) -> Dict[Tuple[date, str], float]:
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
        if sub.empty: return
        sub = sub[_mask_remark_contains(sub["Remark"], ["FINIF","FINON"])]
        if sub.empty: return
        g = sub.groupby("Tanggal")["Amount"].sum()
        for dt_val, amt in g.items():
            totals[(dt_val, _canonical_port_name(port))] += float(amt)
    for f in files:
        try: data = f.getvalue()
        except Exception: data = f.read()
        if not data: continue
        name = f.name
        if str(name).lower().endswith(".zip"):
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for info in zf.infolist():
                        if info.is_dir(): continue
                        if not info.filename.lower().endswith((".csv",".xls",".xlsx",".xlsb")): continue
                        handle_one(zf.read(info), info.filename)
            except Exception:
                continue
        else:
            handle_one(data, name)
    return dict(totals)

def _load_rk_nonbca_inflow_by_dt_port_from_files_sgw(files, header_row: int) -> Dict[Tuple[date, str], float]:
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
        if sub.empty: return
        sub = sub[_mask_remark_contains(sub["Remark"], ["SGW"])]
        if sub.empty: return
        g = sub.groupby("Tanggal")["Amount"].sum()
        for dt_val, amt in g.items():
            totals[(dt_val, _canonical_port_name(port))] += float(amt)
    for f in files:
        try: data = f.getvalue()
        except Exception: data = f.read()
        if not data: continue
        name = f.name
        if str(name).lower().endswith(".zip"):
            try:
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for info in zf.infolist():
                        if info.is_dir(): continue
                        if not info.filename.lower().endswith((".csv",".xls",".xlsx",".xlsb")): continue
                        handle_one(zf.read(info), info.filename)
            except Exception:
                continue
        else:
            handle_one(data, name)
    return dict(totals)


# =========================== Helper gabungan KTP+GLM ===========================

def _append_ketapang_gilimanuk_combined(df: pd.DataFrame, final_cols: list) -> pd.DataFrame:
    if df is None or df.empty or "Pelabuhan" not in df.columns or "Tanggal" not in df.columns:
        return df
    ports_src = ["ASDP Ketapang", "ASDP Gilimanuk"]
    mask = df["Pelabuhan"].isin(ports_src)
    if not mask.any():
        return df
    numeric_cols = [c for c in final_cols if c not in ("Tanggal", "Pelabuhan") and c in df.columns]
    grouped = df.loc[mask].groupby("Tanggal", as_index=False)[numeric_cols].sum()
    if grouped.empty:
        return df
    grouped.insert(1, "Pelabuhan", "ASDP Gilimanuk + ASDP Ketapang")
    out = pd.concat([df, grouped[final_cols]], ignore_index=True)
    return out.sort_values(["Pelabuhan", "Tanggal"]).reset_index(drop=True)


# =========================== Tabel Rekonsiliasi ===========================

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

    ticket_df = (pd.DataFrame(rows).groupby(["Tanggal", "Pelabuhan"], as_index=False)[["Tiket_BCA", "Tiket_NON_BCA"]].sum()
                 if rows else pd.DataFrame(columns=["Tanggal", "Pelabuhan", "Tiket_BCA", "Tiket_NON_BCA"]))

    if df_finnet_settlement is not None and not df_finnet_settlement.empty:
        need = [c for c in ["Tanggal", "Pelabuhan", "BCA", "NON BCA"] if c in df_finnet_settlement.columns]
        settle_df = (df_finnet_settlement[need].copy() if len(need) == 4 else pd.DataFrame(columns=["Tanggal","Pelabuhan","BCA","NON BCA"]))
        if not settle_df.empty:
            settle_df["Pelabuhan"] = settle_df["Pelabuhan"].apply(_canonical_port_name)
            settle_df = settle_df.groupby(["Tanggal", "Pelabuhan"], as_index=False)[["BCA", "NON BCA"]].sum()
    else:
        settle_df = pd.DataFrame(columns=["Tanggal","Pelabuhan","BCA","NON BCA"])

    out = base_df.copy()
    if not ticket_df.empty: out = out.merge(ticket_df, on=["Tanggal", "Pelabuhan"], how="left")
    if not settle_df.empty: out = out.merge(settle_df, on=["Tanggal", "Pelabuhan"], how="left")

    for c in ["Tiket_BCA", "Tiket_NON_BCA", "BCA", "NON BCA"]:
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
    all_dates = [date(year, month, d) for d in range(1, days_in_month + 1)]
    base_idx = pd.MultiIndex.from_product([all_dates, unique_ports], names=["Tanggal", "Pelabuhan"])
    base_df = pd.DataFrame(index=base_idx).reset_index()

    rows = []
    for (dt, asal), bucket in agg.items():
        asal_norm = _canonical_port_name(asal)
        if asal_norm not in unique_ports: continue
        dt_val = dt.date() if isinstance(dt, pd.Timestamp) else dt
        if dt_val is None or dt_val.year != year or dt_val.month != month: continue
        bca_val = float(bucket.get("ESPAY_TIKET_BCA", 0.0))
        non_bca_val = float(bucket.get("ESPAY_TIKET_NON_BCA", 0.0))
        if bca_val == 0.0 and non_bca_val == 0.0: continue
        rows.append({"Tanggal": dt_val, "Pelabuhan": asal_norm, "Tiket_BCA": bca_val, "Tiket_NON_BCA": non_bca_val})

    ticket_df = (pd.DataFrame(rows).groupby(["Tanggal", "Pelabuhan"], as_index=False)[["Tiket_BCA", "Tiket_NON_BCA"]].sum()
                 if rows else pd.DataFrame(columns=["Tanggal","Pelabuhan","Tiket_BCA","Tiket_NON_BCA"]))

    if df_espay_settlement is not None and not df_espay_settlement.empty:
        need = [c for c in ["Tanggal", "Pelabuhan", "BCA", "NON BCA"] if c in df_espay_settlement.columns]
        settle_df = (df_espay_settlement[need].copy() if len(need) == 4 else pd.DataFrame(columns=["Tanggal","Pelabuhan","BCA","NON BCA"]))
        if not settle_df.empty:
            settle_df["Pelabuhan"] = settle_df["Pelabuhan"].apply(_canonical_port_name)
            settle_df = settle_df.groupby(["Tanggal", "Pelabuhan"], as_index=False)[["BCA", "NON BCA"]].sum()
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


# =========================== UI Helpers ===========================

def _to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Rekonsiliasi") -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
    for engine in ("xlsxwriter", "openpyxl"):
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

def _render_df(df_show: pd.DataFrame, highlight: bool) -> None:
    df_show = df_show.copy()
    df_show["Tanggal"] = pd.to_datetime(df_show["Tanggal"]).dt.strftime("%d/%m/%Y")
    df_show = _add_subtotal_row(df_show, label="Subtotal", date_col="Tanggal")
    numeric_cols = df_show.select_dtypes(include="number").columns
    df_show[numeric_cols] = df_show[numeric_cols].fillna(0).round(0).astype("Int64")
    try:
        st.dataframe(_style_table(df_show, highlight=highlight), use_container_width=True)
    except Exception:
        st.dataframe(df_show, use_container_width=True)


# =========================== MAIN ===========================

def main() -> None:
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

    ss_get_set("upload_rev", 0)
    if st.sidebar.button("🔄 Reset semua upload"):
        st.session_state.upload_rev += 1

    up_files = st.sidebar.file_uploader(
        "Upload Payment Report: ZIP / beberapa Excel (.xlsx/.xls/.xlsb) / CSV",
        type=["zip", "xlsx", "xls", "xlsb", "csv"], accept_multiple_files=True,
        key=f"payment_{st.session_state.upload_rev}",
    )
    # ======= UPDATED: hanya CSV/XLSX =======
    settlement_files = st.sidebar.file_uploader(
        "Upload Settlement ESPAY (.xlsx / .csv)",
        type=["xlsx", "csv"], accept_multiple_files=True,
        key=f"settlement_espay_{st.session_state.upload_rev}",
    )
    # =======================================
    finnet_files = st.sidebar.file_uploader(
        "Upload Settlement Finnet by Telkom (ZIP / .csv)", type=["zip", "csv"], accept_multiple_files=True,
        key=f"settlement_finnet_{st.session_state.upload_rev}",
    )
    finnet_espay_files = st.sidebar.file_uploader(
        "Upload Settlement Finnet (Espay) (ZIP / .csv)", type=["zip", "csv"], accept_multiple_files=True,
        key=f"settlement_finnet_espay_{st.session_state.upload_rev}",
    )
    rek_bca_files = st.sidebar.file_uploader(
        "Upload Rekening Koran BCA", type=["zip", "xlsx", "xls", "xlsb", "csv"], accept_multiple_files=True,
        key=f"rek_bca_{st.session_state.upload_rev}",
    )
    rek_nonbca_files = st.sidebar.file_uploader(
        "Upload Rekening Koran Non BCA", type=["zip", "xlsx", "xls", "xlsb", "csv"], accept_multiple_files=True,
        key=f"rek_nonbca_{st.session_state.upload_rev}",
    )

    highlight = st.sidebar.checkbox("Highlight Selisih ≠ 0 (tabel rekonsiliasi)", value=True)

    # ===== Payment Report =====
    if not up_files:
        st.info("Silakan upload Payment Report (bisa banyak file atau ZIP) untuk melanjutkan.")
        return

    with st.spinner("Memproses Payment Report…"):
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
            _render_df(result[result["Pelabuhan"] == port], highlight=False)

    # ===== Settlement ESPAY =====
    st.divider(); st.subheader("DETAIL SETTLEMENT ESPAY")
    df_espay_for_rekon = pd.DataFrame()
    if settlement_files:
        with st.spinner("Memproses Settlement ESPAY…"):
            df_settlement_raw = _load_settlement_espay(settlement_files)
            df_espay = _build_espay_settlement_table(df_settlement_raw, year=year, month=month)
            df_espay_for_rekon = df_espay.copy()
        if df_espay.empty:
            st.warning("Settlement ESPAY kosong / tidak sesuai periode, atau kolom wajib tidak lengkap.")
        else:
            ports_espay = sorted(df_espay["Pelabuhan"].dropna().unique())
            tabs_espay = st.tabs(ports_espay if ports_espay else ["(Tidak ada Pelabuhan)"])
            for tab, port in zip(tabs_espay, ports_espay):
                with tab:
                    st.markdown(f"**Pelabuhan: {port}**")
                    _render_df(df_espay[df_espay["Pelabuhan"] == port], highlight=False)
    else:
        st.info("Belum ada file Settlement ESPAY (.xlsx/.csv).")

    # ===== Settlement FINNET by Telkom =====
    st.divider(); st.subheader("DETAIL SETTLEMENT FINNET BY TELKOM")
    df_finnet = None
    if finnet_files:
        with st.spinner("Memproses Settlement Finnet…"):
            df_finnet_raw = _load_settlement_finnet(finnet_files)
            df_finnet = _build_finnet_settlement_table(df_finnet_raw, year=year, month=month)
        if df_finnet is None or df_finnet.empty:
            st.warning("Settlement Finnet kosong / tidak sesuai periode.")
        else:
            ports_finnet = sorted(df_finnet["Pelabuhan"].dropna().unique())
            tabs_finnet = st.tabs(ports_finnet if ports_finnet else ["(Tidak ada Pelabuhan)"])
            for tab, port in zip(tabs_finnet, ports_finnet):
                with tab:
                    st.markdown(f"**Pelabuhan: {port}**")
                    _render_df(df_finnet[df_finnet["Pelabuhan"] == port], highlight=False)
    else:
        st.info("Belum ada file Settlement Finnet (Telkom).")

    # ===== Rekonsiliasi Gabungan =====
    st.divider()
    st.subheader("TABEL REKONSILIASI GABUNGAN PAYMENT - SETTLEMENT DANA - REKENING KORAN")

    # --- 1. Rekon FINNET (Dana Masuk: FINIF/FINON) ---
    st.markdown("**1. Tabel Rekonsiliasi Finnet**")
    bca_finif_by_dt_port = _load_rk_bca_finif_by_dt_port(rek_bca_files) if rek_bca_files else {}
    nonbca_finif_by_dt_port = _load_rk_nonbca_inflow_by_dt_port_from_files(
        rek_nonbca_files, header_row=13
    ) if rek_nonbca_files else {}
    df_rekon_finnet = _build_finnet_rekon_table(
        agg, df_finnet, year=year, month=month,
        bca_inflow_by_dt_port=bca_finif_by_dt_port,
        nonbca_inflow_by_dt_port=nonbca_finif_by_dt_port,
    )
    if df_rekon_finnet.empty:
        st.warning("Tabel Rekonsiliasi Finnet belum dapat dibentuk.")
    else:
        ports_rekon = sorted(df_rekon_finnet["Pelabuhan"].dropna().unique())
        tabs_rekon = st.tabs(ports_rekon if ports_rekon else ["(Tidak ada Pelabuhan)"])
        for tab, label in zip(tabs_rekon, ports_rekon):
            with tab:
                st.markdown(f"**Pelabuhan: {label}**")
                _render_df(df_rekon_finnet[df_rekon_finnet["Pelabuhan"] == label], highlight=True)

    # --- 2. Rekon ESPAY (Dana Masuk: SGW) ---
    st.markdown("**2. Tabel Rekonsiliasi ESPAY**")
    bca_sgw_by_dt_port = _load_rk_bca_sgw_by_dt_port(rek_bca_files) if rek_bca_files else {}
    nonbca_sgw_by_dt_port = _load_rk_nonbca_inflow_by_dt_port_from_files_sgw(
        rek_nonbca_files, header_row=13
    ) if rek_nonbca_files else {}
    df_rekon_espay = _build_espay_rekon_table(
        agg, df_espay_for_rekon, year=year, month=month,
        bca_inflow_by_dt_port_sgw=bca_sgw_by_dt_port,
        nonbca_inflow_by_dt_port_sgw=nonbca_sgw_by_dt_port,
    )
    if df_rekon_espay.empty:
        st.warning("Tabel Rekonsiliasi ESPAY belum dapat dibentuk.")
    else:
        ports_rekon_espay = sorted(df_rekon_espay["Pelabuhan"].dropna().unique())
        tabs_rekon_espay = st.tabs(ports_rekon_espay if ports_rekon_espay else ["(Tidak ada Pelabuhan)"])
        for tab, label in zip(tabs_rekon_espay, ports_rekon_espay):
            with tab:
                st.markdown(f"**Pelabuhan: {label}**")
                _render_df(df_rekon_espay[df_rekon_espay["Pelabuhan"] == label], highlight=True)

    # ===== Unduh hasil Payment gabungan =====
    st.divider(); st.subheader("Unduh Hasil Payment (Gabungan Semua Pelabuhan)")
    export_df = result.copy()
    export_df["Tanggal"] = pd.to_datetime(export_df["Tanggal"]).dt.strftime("%d/%m/%Y")
    num_cols = export_df.select_dtypes(include="number").columns
    export_df[num_cols] = export_df[num_cols].round(0).astype("Int64")
    csv_bytes = export_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Unduh CSV (Gabungan Payment)", data=csv_bytes,
                       file_name=f"rekonsiliasi_payment_{year}_{month:02d}_per_pelabuhan.csv", mime="text/csv")
    excel_bytes, engine_used, err_msg = _to_excel_bytes(export_df, sheet_name="Rekonsiliasi")
    if excel_bytes:
        st.download_button(f"Unduh Excel (.xlsx) (Gabungan Payment){' • ' + engine_used if engine_used else ''}",
                           data=excel_bytes,
                           file_name=f"rekonsiliasi_payment_{year}_{month:02d}_per_pelabuhan.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.warning("Ekspor Excel gagal. Tambahkan `xlsxwriter` atau `openpyxl` ke requirements."
                   + (f"\nDetail: {err_msg}" if err_msg else ""))


if __name__ == "__main__":
    main()

# path: streamlit_app.py
import io
import zipfile
from datetime import date
from collections import defaultdict, OrderedDict
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from openpyxl import load_workbook
from zoneinfo import ZoneInfo  # TZ handling


# =========================== Konfigurasi & Konstanta (TABEL UTAMA) ===========================

COL_H = "TIPE PEMBAYARAN"                      # H
COL_B = "TANGGAL PEMBAYARAN"                   # B
COL_AA = "REF NO"                              # AA
COL_K = "TOTAL TARIF TANPA BIAYA ADMIN (Rp.)"  # K
COL_X = "SOF ID"                               # X
COL_ASAL = "ASAL"                              # Pelabuhan (untuk split tabel utama)
REQUIRED_COLS = [COL_H, COL_B, COL_AA, COL_K, COL_X, COL_ASAL]

CAT_COLS = [
    "Cash", "Prepaid BRI", "Prepaid BNI", "Prepaid Mandiri", "Prepaid BCA",
    "SKPT", "IFCS", "Reedem", "ESPAY", "Finnet",
]
NON_COMPONENTS = ["Cash", "Prepaid BRI", "Prepaid BNI", "Prepaid Mandiri", "Prepaid BCA", "SKPT", "IFCS", "Reedem"]

CSV_CHUNK_ROWS = 200_000
XLSX_BATCH_ROWS = 50_000
VALID_EXTS = (".xlsx", ".xls", ".csv")


# =========================== Konfigurasi & Konstanta (TABEL ESPAY BARU) ===========================

ESPAY_DATE = "Settlement Date"   # E
ESPAY_PROD = "Product Name"      # P
ESPAY_VA_NAME = "VA Name"        # X (nama VA, pemetaan pelabuhan)
ESPAY_REQUIRED = [ESPAY_DATE, ESPAY_PROD, ESPAY_VA_NAME]
ESPAY_AMOUNT_CANDIDATES = ["Amount", "Settlement Amount", "Amount (Rp.)", "Total Amount", "TOTAL", "Total"]

PORT_MAP = {
    "asdp merak": "Merak",
    "asdp bakauheni": "Bakauheni",
    "asdp ketapang": "Ketapang",
    "asdp gilimanuk": "Gilimanuk",
}


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
    return None, None, "Tidak ada engine Excel (xlsxwriter/openpyxl). Tambahkan ke requirements."


# =========================== Agregator streaming (TABEL UTAMA) ===========================

def _empty_agg_main():
    return defaultdict(lambda: defaultdict(float))  # key: (date, asal) -> {col -> sum}

def _update_agg_series_main(agg, ser: pd.Series, colname: str) -> None:
    if ser.empty:
        return
    for (dt, asal), val in ser.items():
        agg[(dt, asal)][colname] += float(val)

def _apply_rules_and_update_main(df_chunk: pd.DataFrame, agg) -> None:
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
        _update_agg_series_main(agg, sum_by_key(m), name)

    is_finpay = H.str.contains("finpay", na=False)
    is_bca_tag = X.str.contains("vabcaespay", na=False) | X.str.contains("bluespay", na=False)
    _update_agg_series_main(agg, sum_by_key(is_finpay & is_bca_tag), "BCA")
    _update_agg_series_main(agg, sum_by_key(is_finpay & (~is_bca_tag)), "NON BCA")

def _process_csv_main(data: bytes, year: int, month: int, agg) -> None:
    itr = pd.read_csv(
        io.BytesIO(data),
        usecols=REQUIRED_COLS,
        chunksize=CSV_CHUNK_ROWS,
        dtype={COL_H: "string", COL_AA: "string", COL_X: "string", COL_ASAL: "string"},
    )
    for chunk in itr:
        t = pd.to_datetime(chunk[COL_B], errors="coerce", dayfirst=True)
        mask = (t.dt.year == year) & (t.dt.month == month)
        if not mask.any():
            continue
        sub = chunk.loc[mask].copy()
        sub["Tanggal"] = t.loc[mask].dt.date
        _apply_rules_and_update_main(sub, agg)

def _process_xlsx_main(data: bytes, year: int, month: int, agg) -> None:
    try:
        wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]
        rows = ws.iter_rows(values_only=True)
        header = next(rows, None)
        if header is None:
            wb.close()
            return
        name_to_idx = {str(h).strip(): i for i, h in enumerate(header) if h is not None}
        if not all(c in name_to_idx for c in REQUIRED_COLS):
            wb.close()
            return
        buf = []
        for r in rows:
            try:
                buf.append([
                    r[name_to_idx[COL_H]], r[name_to_idx[COL_B]], r[name_to_idx[COL_AA]],
                    r[name_to_idx[COL_K]], r[name_to_idx[COL_X]], r[name_to_idx[COL_ASAL]],
                ])
            except Exception:
                continue
            if len(buf) >= XLSX_BATCH_ROWS:
                _flush_xlsx_batch_main(buf, year, month, agg); buf.clear()
        if buf:
            _flush_xlsx_batch_main(buf, year, month, agg); buf.clear()
        wb.close()
    except Exception:
        try:
            df = pd.read_excel(io.BytesIO(data), sheet_name=0, usecols=REQUIRED_COLS)
        except Exception:
            return
        t = pd.to_datetime(df[COL_B], errors="coerce", dayfirst=True)
        mask = (t.dt.year == year) & (t.dt.month == month)
        if not mask.any(): return
        sub = df.loc[mask].copy(); sub["Tanggal"] = t.loc[mask].dt.date
        _apply_rules_and_update_main(sub, agg)

def _flush_xlsx_batch_main(buf: List[List], year: int, month: int, agg) -> None:
    df = pd.DataFrame(buf, columns=[COL_H, COL_B, COL_AA, COL_K, COL_X, COL_ASAL])
    t = pd.to_datetime(df[COL_B], errors="coerce", dayfirst=True)
    mask = (t.dt.year == year) & (t.dt.month == month)
    if not mask.any(): return
    sub = df.loc[mask].copy(); sub["Tanggal"] = t.loc[mask].dt.date
    _apply_rules_and_update_main(sub, agg)

def _load_and_aggregate_main(files: List["st.runtime.uploaded_file_manager.UploadedFile"], year: int, month: int):
    agg = _empty_agg_main()
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
                        if low.endswith((".xlsx", ".xls")): _process_xlsx_main(content, year, month, agg)
                        else: _process_csv_main(content, year, month, agg)
            elif name.endswith((".xlsx", ".xls")): _process_xlsx_main(data, year, month, agg)
            elif name.endswith(".csv"): _process_csv_main(data, year, month, agg)
        except Exception:
            continue
    return agg

def _build_result_from_agg_main(agg) -> pd.DataFrame:
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
    return df.sort_values(["Pelabuhan", "Tanggal"]).reset_index(drop=True)


# =========================== Agregator Settlement ESPAY (tabel baru) ===========================

def _detect_amount_col(cols: List[str]) -> Optional[str]:
    norm = {c.strip().lower(): c for c in cols}
    for cand in ESPAY_AMOUNT_CANDIDATES:
        if cand.lower() in norm:
            return norm[cand.lower()]
    return None

def _port_from_va_name(val: str) -> str:
    s = (val or "").strip().lower()
    for key, label in PORT_MAP.items():
        if key in s:
            return label
    return "Lainnya"

def _localize_to_tz(series_dt: pd.Series, tz_name: str) -> pd.Series:
    """Lokalisasi/konversi ke TZ terpilih. Naive → diasumsikan TZ tsb."""
    tz = ZoneInfo(tz_name)
    try:
        if series_dt.dt.tz is not None:  # tz-aware
            return series_dt.dt.tz_convert(tz)
        else:  # naive
            return series_dt.dt.tz_localize(tz, nonexistent="shift_forward", ambiguous="NaT")
    except Exception:
        # Fallback aman: treat sebagai naive lokal
        try:
            return series_dt.dt.tz_localize(tz, nonexistent="shift_forward", ambiguous="NaT")
        except Exception:
            return series_dt  # terakhir: biarkan apa adanya

def _empty_agg_espay():
    return defaultdict(lambda: defaultdict(float))  # key: (date, port) -> {"Virtual Account": x, "E-Money": y}

def _update_agg_series_espay(agg, ser: pd.Series, field: str) -> None:
    if ser.empty: return
    for (dt, port), val in ser.items():
        agg[(dt, port)][field] += float(val)

def _apply_and_update_espay(df_chunk: pd.DataFrame, agg) -> None:
    cols_map = {c.strip(): c for c in df_chunk.columns}
    for need in ESPAY_REQUIRED:
        if need not in cols_map:
            return

    prod = df_chunk[cols_map[ESPAY_PROD]].fillna("").astype(str).str.lower()
    va_name = df_chunk[cols_map[ESPAY_VA_NAME]].fillna("").astype(str)
    tgl = df_chunk["Tanggal"]

    amt_col = _detect_amount_col(list(df_chunk.columns))
    if amt_col:
        amt = pd.to_numeric(df_chunk[amt_col], errors="coerce").fillna(0)
    else:
        amt = pd.Series(1.0, index=df_chunk.index)

    port = va_name.map(_port_from_va_name)
    is_va = prod.str.contains("va", na=False)

    def sum_by(mask) -> pd.Series:
        if mask.any():
            return amt[mask].groupby([tgl[mask], port[mask]], dropna=False).sum(min_count=1)
        mi = pd.MultiIndex.from_arrays([[], []], names=["Tanggal", "Pelabuhan"])
        return pd.Series(index=mi, dtype="float64")

    _update_agg_series_espay(agg, sum_by(is_va), "Virtual Account")
    _update_agg_series_espay(agg, sum_by(~is_va), "E-Money")

def _process_csv_espay(data: bytes, year: int, month: int, agg, tz_name: str) -> Tuple[Optional[str], int, int]:
    itr = pd.read_csv(io.BytesIO(data), chunksize=CSV_CHUNK_ROWS, dtype="unicode")
    amount_used = None; total_rows = 0; used_rows = 0
    for chunk in itr:
        total_rows += len(chunk)
        chunk.columns = [str(c).strip() for c in chunk.columns]
        if ESPAY_DATE not in chunk.columns:
            continue
        t = pd.to_datetime(chunk[ESPAY_DATE], errors="coerce", dayfirst=True)
        t_local = _localize_to_tz(t, tz_name)
        mask = (t_local.dt.year == year) & (t_local.dt.month == month)
        if not mask.any():
            continue
        used_rows += int(mask.sum())
        sub = chunk.loc[mask].copy()
        sub["Tanggal"] = t_local.loc[mask].dt.date
        amt_col = _detect_amount_col(list(sub.columns))
        if amt_col and amount_used is None: amount_used = amt_col
        _apply_and_update_espay(sub, agg)
    return amount_used, total_rows, used_rows

def _process_xlsx_espay(data: bytes, year: int, month: int, agg, tz_name: str) -> Tuple[Optional[str], int, int]:
    rows_count = 0; rows_used = 0
    try:
        wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]
        rows = ws.iter_rows(values_only=True)
        header = next(rows, None)
        if header is None: wb.close(); return None, 0, 0
        header = [str(h).strip() if h is not None else "" for h in header]
        name_to_idx = {h: i for i, h in enumerate(header) if h}
        if not all(c in name_to_idx for c in ESPAY_REQUIRED): wb.close(); return None, 0, 0
        amt_col_name = _detect_amount_col(header)
        buf = []; amount_used = None
        for r in rows:
            rows_count += 1
            try:
                buf.append([
                    r[name_to_idx[ESPAY_PROD]],
                    r[name_to_idx[ESPAY_DATE]],
                    r[name_to_idx[ESPAY_VA_NAME]],
                    (r[name_to_idx[amt_col_name]] if amt_col_name and amt_col_name in name_to_idx else None),
                ])
            except Exception:
                continue
            if len(buf) >= XLSX_BATCH_ROWS:
                used = _flush_xlsx_batch_espay(buf, year, month, agg, amt_col_name, tz_name)
                rows_used += used
                if amt_col_name and amount_used is None: amount_used = amt_col_name
                buf.clear()
        if buf:
            used = _flush_xlsx_batch_espay(buf, year, month, agg, amt_col_name, tz_name)
            rows_used += used
            if amt_col_name and amount_used is None: amount_used = amt_col_name
            buf.clear()
        wb.close()
        return amount_used, rows_count, rows_used
    except Exception:
        try:
            df = pd.read_excel(io.BytesIO(data), sheet_name=0)
        except Exception:
            return None, 0, 0
        df.columns = [str(c).strip() for c in df.columns]
        if not all(c in df.columns for c in ESPAY_REQUIRED):
            return None, 0, 0
        t = pd.to_datetime(df[ESPAY_DATE], errors="coerce", dayfirst=True)
        t_local = _localize_to_tz(t, tz_name)
        mask = (t_local.dt.year == year) & (t_local.dt.month == month)
        used = int(mask.sum())
        if not used: return _detect_amount_col(list(df.columns)), len(df), 0
        sub = df.loc[mask].copy()
        sub["Tanggal"] = t_local.loc[mask].dt.date
        _apply_and_update_espay(sub, agg)
        return _detect_amount_col(list(df.columns)), len(df), used

def _flush_xlsx_batch_espay(buf: List[List], year: int, month: int, agg, amt_col_name: Optional[str], tz_name: str) -> int:
    cols = [ESPAY_PROD, ESPAY_DATE, ESPAY_VA_NAME, "__AMT__"]
    df = pd.DataFrame(buf, columns=cols)
    t = pd.to_datetime(df[ESPAY_DATE], errors="coerce", dayfirst=True)
    t_local = _localize_to_tz(t, tz_name)
    mask = (t_local.dt.year == year) & (t_local.dt.month == month)
    if not mask.any(): return 0
    sub = df.loc[mask, [ESPAY_PROD, ESPAY_VA_NAME, "__AMT__"]].copy()
    sub["Tanggal"] = t_local.loc[mask].dt.date
    if "__AMT__" in sub.columns:
        sub.rename(columns={"__AMT__": ESPAY_AMOUNT_CANDIDATES[0]}, inplace=True)
    _apply_and_update_espay(sub, agg)
    return int(mask.sum())

def _load_and_aggregate_espay(files: List["st.runtime.uploaded_file_manager.UploadedFile"], year: int, month: int, tz_name: str):
    agg = _empty_agg_espay()
    info: List[str] = []
    for f in files:
        try: data = f.getvalue()
        except Exception: data = f.read()
        name = f.name
        try:
            low = name.lower()
            if low.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for m in zf.infolist():
                        if m.is_dir(): continue
                        fname = m.filename
                        flow = fname.lower()
                        if not flow.endswith(VALID_EXTS): continue
                        content = zf.read(m)
                        if flow.endswith((".xlsx", ".xls")):
                            c, r_all, r_used = _process_xlsx_espay(content, year, month, agg, tz_name)
                        else:
                            c, r_all, r_used = _process_csv_espay(content, year, month, agg, tz_name)
                        info.append(f"{name}::{fname} → {'sum ' + c if c else 'count trx'} (rows: {r_used}/{r_all} terpakai)")
            elif low.endswith((".xlsx", ".xls")):
                c, r_all, r_used = _process_xlsx_espay(data, year, month, agg, tz_name)
                info.append(f"{name} → {'sum ' + c if c else 'count trx'} (rows: {r_used}/{r_all} terpakai)")
            elif low.endswith(".csv"):
                c, r_all, r_used = _process_csv_espay(data, year, month, agg, tz_name)
                info.append(f"{name} → {'sum ' + c if c else 'count trx'} (rows: {r_used}/{r_all} terpakai)")
        except Exception as e:
            info.append(f"{name} → error: {e}")
            continue
    return agg, info

def _build_result_from_agg_espay(agg) -> pd.DataFrame:
    if not agg: return pd.DataFrame()
    rows = []
    for (dt, port), bucket in agg.items():
        rows.append({
            "Tanggal": dt,
            "Pelabuhan": port,
            "Virtual Account": bucket.get("Virtual Account", 0.0),
            "E-Money": bucket.get("E-Money", 0.0),
        })
    df = pd.DataFrame(rows)
    if df.empty: return df
    df = df[["Tanggal", "Pelabuhan", "Virtual Account", "E-Money"]]
    return df.sort_values(["Pelabuhan", "Tanggal"]).reset_index(drop=True)


# =========================== Render helper ===========================

def _render_port_table(df_port: pd.DataFrame, highlight: bool) -> None:
    df_show = df_port.copy()
    df_show["Tanggal"] = pd.to_datetime(df_show["Tanggal"]).dt.strftime("%d/%m/%Y")
    df_show = _add_subtotal_row(df_show, label="Subtotal", date_col="Tanggal")
    numeric_cols = df_show.select_dtypes(include="number").columns
    df_show[numeric_cols] = df_show[numeric_cols].round(0).astype("Int64")
    try:
        st.dataframe(_style_table(df_show, highlight=highlight), use_container_width=True)
    except Exception:
        st.dataframe(df_show, use_container_width=True)


# =========================== Streamlit UI ===========================

def main() -> None:
    st.set_page_config(page_title="Rekonsiliasi Payment Report", layout="wide")
    st.title("Rekonsiliasi Payment Report")

    # ---- Filter global ----
    today = date.today()
    years_options = list(range(today.year - 5, today.year + 6))
    year = st.sidebar.selectbox("Tahun", options=years_options, index=years_options.index(today.year))
    month_names = {1:"01 - Januari",2:"02 - Februari",3:"03 - Maret",4:"04 - April",5:"05 - Mei",6:"06 - Juni",7:"07 - Juli",8:"08 - Agustus",9:"09 - September",10:"10 - Oktober",11:"11 - November",12:"12 - Desember"}
    month = st.sidebar.selectbox("Bulan", options=list(range(1, 13)), index=today.month - 1, format_func=lambda m: month_names[m])
    highlight = st.sidebar.checkbox("Highlight kolom Selisih ≠ 0 (tabel utama)", value=True)

    # ---- Uploader TABEL UTAMA ----
    up_files_main = st.sidebar.file_uploader("Upload ZIP / Excel / CSV untuk TABEL UTAMA", type=["zip", "xlsx", "xls", "csv"], accept_multiple_files=True, key="up_main")

    # ---- Uploader TABEL ESPAY + TZ ----
    tz_choice = st.sidebar.selectbox("Zona waktu untuk Settlement Date (ESPAY)", options=[
        "Asia/Jakarta", "UTC", "Asia/Makassar", "Asia/Jayapura"
    ], index=0)
    up_files_espay = st.sidebar.file_uploader("Upload ZIP / Excel / CSV untuk Settlement ESPAY", type=["zip", "xlsx", "xls", "csv"], accept_multiple_files=True, key="up_espay")

    # ================== TABEL UTAMA ==================
    st.subheader(f"Hasil Rekonsiliasi • Periode: {month_names[month]} {year}")
    if not up_files_main:
        st.info("Upload file (Tabel Utama) di panel kiri.")
    else:
        with st.spinner("Memproses Tabel Utama…"):
            agg_main = _load_and_aggregate_main(up_files_main, year=year, month=month)
        result_main = _build_result_from_agg_main(agg_main)
        if result_main.empty:
            st.warning("Tabel utama: tidak ada data valid setelah filter.")
        else:
            ports = list(result_main["Pelabuhan"].dropna().unique()); ports.sort()
            tabs = st.tabs(ports if ports else ["(Tidak ada Pelabuhan)"])
            for tab, port in zip(tabs, ports):
                with tab:
                    st.markdown(f"**Pelabuhan: {port}**")
                    _render_port_table(result_main[result_main["Pelabuhan"] == port], highlight=highlight)

            st.divider()
            st.subheader("Unduh Hasil Tabel Utama (Gabungan)")
            export_df = result_main.copy()
            export_df["Tanggal"] = pd.to_datetime(export_df["Tanggal"]).dt.strftime("%d/%m/%Y")
            num_cols = export_df.select_dtypes(include="number").columns
            export_df[num_cols] = export_df[num_cols].round(0).astype("Int64")
            csv_bytes = export_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button("Unduh CSV (Tabel Utama)", data=csv_bytes, file_name=f"rekonsiliasi_payment_{year}_{month:02d}_per_pelabuhan.csv", mime="text/csv")
            excel_bytes, engine_used, err_msg = _to_excel_bytes(export_df, sheet_name="Rekonsiliasi")
            if excel_bytes:
                st.download_button(f"Unduh Excel (.xlsx) (Tabel Utama){' • ' + engine_used if engine_used else ''}", data=excel_bytes, file_name=f"rekonsiliasi_payment_{year}_{month:02d}_per_pelabuhan.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ================== TABEL BARU: Settlement Dana ESPAY ==================
    st.divider()
    st.subheader("Settlement Dana ESPAY")

    if not up_files_espay:
        st.info("Upload file Settlement ESPAY di panel kiri (ZIP / banyak file).")
        return

    with st.spinner("Memproses Settlement Dana ESPAY…"):
        agg_espay, info_espay = _load_and_aggregate_espay(up_files_espay, year=year, month=month, tz_name=tz_choice)

    result_espay = _build_result_from_agg_espay(agg_espay)
    if result_espay.empty:
        st.warning("Settlement ESPAY: tidak ada data valid setelah filter.")
        if info_espay:
            st.caption("Log pemrosesan:\n- " + "\n- ".join(info_espay))
        return

    # ---- Filter Pelabuhan (multi-select) untuk ESPAY ----
    all_ports = sorted(result_espay["Pelabuhan"].unique().tolist())
    selected_ports = st.multiselect("Filter Pelabuhan (ESPAY)", options=all_ports, default=all_ports)
    result_espay_filtered = result_espay[result_espay["Pelabuhan"].isin(selected_ports)] if selected_ports else result_espay.iloc[0:0]

    df_show = result_espay_filtered.copy()
    df_show["Tanggal"] = pd.to_datetime(df_show["Tanggal"]).dt.strftime("%d/%m/%Y")
    df_show = _add_subtotal_row(df_show, label="Subtotal", date_col="Tanggal")
    num_cols = df_show.select_dtypes(include="number").columns
    df_show[num_cols] = df_show[num_cols].round(0).astype("Int64")
    st.dataframe(df_show.style.format("{:,.0f}", subset=num_cols), use_container_width=True)

    if info_espay:
        st.caption("Ringkasan pemrosesan Settlement ESPAY:\n- " + "\n- ".join(info_espay))

    st.subheader("Unduh Settlement Dana ESPAY")
    csv_bytes2 = df_show.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Unduh CSV (ESPAY)", data=csv_bytes2, file_name=f"settlement_espay_{year}_{month:02d}.csv", mime="text/csv")
    excel_bytes2, engine_used2, err_msg2 = _to_excel_bytes(df_show, sheet_name="Settlement ESPAY")
    if excel_bytes2:
        st.download_button(f"Unduh Excel (.xlsx) (ESPAY){' • ' + engine_used2 if engine_used2 else ''}", data=excel_bytes2, file_name=f"settlement_espay_{year}_{month:02d}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


if __name__ == "__main__":
    main()

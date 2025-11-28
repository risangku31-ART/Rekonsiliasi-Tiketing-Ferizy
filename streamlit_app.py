# path: streamlit_app.py
import io
import zipfile
from datetime import date
from calendar import monthrange
from collections import defaultdict, OrderedDict
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st
from openpyxl import load_workbook  # streaming .xlsx read_only


# =========================== Konfigurasi & Konstanta ===========================

COL_H = "TIPE PEMBAYARAN"                      # H
COL_B = "TANGGAL PEMBAYARAN"                   # B
COL_AA = "REF NO"                              # AA
COL_K = "TOTAL TARIF TANPA BIAYA ADMIN (Rp.)"  # K
COL_X = "SOF ID"                               # X
COL_ASAL = "ASAL"                              # (Pelabuhan)
REQUIRED_COLS = [COL_H, COL_B, COL_AA, COL_K, COL_X, COL_ASAL]

CAT_COLS = [
    "Cash",
    "Prepaid BRI",
    "Prepaid BNI",
    "Prepaid Mandiri",
    "Prepaid BCA",
    "SKPT",
    "IFCS",
    "Reedem",
    "ESPAY",
    "Finnet",
]

NON_COMPONENTS = [
    "Cash",
    "Prepaid BRI",
    "Prepaid BNI",
    "Prepaid Mandiri",
    "Prepaid BCA",
    "SKPT",
    "IFCS",
    "Reedem",
]

CSV_CHUNK_ROWS = 200_000
XLSX_BATCH_ROWS = 50_000
VALID_EXTS = (".xlsx", ".xls", ".xlsb", ".csv")

# Settlement ESPAY (CSV): kolom wajib (case-insensitive)
SETTLEMENT_REQUIRED_COLS = ["Product Name", "Settlement Amount", "Settlement Date", "VA NAME"]

# Settlement Finnet by Telkom (CSV): kolom wajib (case-insensitive)
FINNET_REQUIRED_COLS = ["Payment Method", "Merchant Amount", "Payment Date Time", "Merchant Name"]


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
    """Normalisasi nama kolom: buang spasi/underscore, huruf kecil semua, hanya alfanumerik."""
    return "".join(ch.lower() for ch in str(name) if ch.isalnum())


# =========================== Agregator streaming (per Tanggal & Pelabuhan) ===========================

def _empty_agg():
    # key: (date, asal) -> {col -> sum}
    return defaultdict(lambda: defaultdict(float))


def _update_agg_series(agg, ser: pd.Series, colname: str) -> None:
    if ser.empty:
        return
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
        if not mask.any():
            continue
        sub = chunk.loc[mask].copy()
        sub["Tanggal"] = t.loc[mask].dt.date
        _apply_rules_and_update(sub, agg)


def _process_xlsx_streaming(data: bytes, year: int, month: int, agg) -> None:
    """Streaming .xlsx (read_only). Fallback ke pandas.read_excel jika gagal (termasuk .xls)."""
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
                    r[name_to_idx[COL_H]],
                    r[name_to_idx[COL_B]],
                    r[name_to_idx[COL_AA]],
                    r[name_to_idx[COL_K]],
                    r[name_to_idx[COL_X]],
                    r[name_to_idx[COL_ASAL]],
                ])
            except Exception:
                continue
            if len(buf) >= XLSX_BATCH_ROWS:
                _flush_xlsx_batch(buf, year, month, agg)
                buf.clear()
        if buf:
            _flush_xlsx_batch(buf, year, month, agg)
            buf.clear()
        wb.close()
    except Exception:
        try:
            df = pd.read_excel(io.BytesIO(data), sheet_name=0, usecols=REQUIRED_COLS)
        except Exception:
            return
        t = pd.to_datetime(df[COL_B], errors="coerce")
        mask = (t.dt.year == year) & (t.dt.month == month)
        if not mask.any():
            return
        sub = df.loc[mask].copy()
        sub["Tanggal"] = t.loc[mask].dt.date
        _apply_rules_and_update(sub, agg)


def _process_xlsb(data: bytes, year: int, month: int, agg) -> None:
    """Proses .xlsb dengan pyxlsb (non-streaming)."""
    try:
        df = pd.read_excel(
            io.BytesIO(data),
            sheet_name=0,
            usecols=REQUIRED_COLS,
            engine="pyxlsb",
        )
    except Exception:
        return

    t = pd.to_datetime(df[COL_B], errors="coerce")
    mask = (t.dt.year == year) & (t.dt.month == month)
    if not mask.any():
        return
    sub = df.loc[mask].copy()
    sub["Tanggal"] = t.loc[mask].dt.date
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


# =========================== Loader multi-file & ZIP (streaming) ===========================

def _load_and_aggregate(files: List["st.runtime.uploaded_file_manager.UploadedFile"], year: int, month: int):
    agg = _empty_agg()
    for f in files:
        try:
            data = f.getvalue()
        except Exception:
            data = f.read()
        name = f.name.lower()

        try:
            if name.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for m in zf.infolist():
                        if m.is_dir():
                            continue
                        low = m.filename.lower()
                        if not low.endswith(VALID_EXTS):
                            continue
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


# =========================== Build hasil dari aggregator (TABEL UTAMA) ===========================

def _build_result_from_agg(agg) -> pd.DataFrame:
    if not agg:
        return pd.DataFrame()

    rows: List[dict] = []
    for (dt, asal), bucket in agg.items():
        row = {"Tanggal": dt, "Pelabuhan": asal}
        for c in CAT_COLS:
            row[c] = bucket.get(c, 0.0)
        row["Total"] = sum(row[c] for c in CAT_COLS)
        bca = bucket.get("BCA", 0.0)
        nonbca = bucket.get("NON BCA", 0.0)
        row["BCA"] = bca
        row["NON BCA"] = nonbca
        row["NON"] = sum(row[c] for c in NON_COMPONENTS)
        row["TOTAL"] = bca + nonbca + row["NON"]
        row["Selisih"] = row["TOTAL"] - row["Total"]
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df[["Tanggal", "Pelabuhan"] + CAT_COLS + ["Total", "BCA", "NON BCA", "NON", "TOTAL", "Selisih"]]
    df = df.sort_values(["Pelabuhan", "Tanggal"]).reset_index(drop=True)
    return df


# =========================== Settlement ESPAY (CSV only) ===========================

def _read_settlement_single_csv(content: bytes) -> Optional[pd.DataFrame]:
    """Settlement ESPAY: baca satu CSV."""
    try:
        text = content.decode("utf-8-sig", errors="ignore")
        df = pd.read_csv(io.StringIO(text), sep=",")
    except Exception:
        return None

    original_cols = list(df.columns.astype(str))
    norm_map = {c: c.strip() for c in original_cols}
    df.rename(columns=norm_map, inplace=True)

    lower_to_real = {c.lower(): c for c in df.columns}
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
    """Settlement ESPAY: CSV langsung / di dalam ZIP."""
    all_dfs: List[pd.DataFrame] = []
    for f in files:
        try:
            data = f.getvalue()
        except Exception:
            data = f.read()
        name = f.name.lower()

        try:
            if name.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for info in zf.infolist():
                        if info.is_dir():
                            continue
                        low = info.filename.lower()
                        if not low.endswith(".csv"):
                            continue
                        content = zf.read(info)
                        df_part = _read_settlement_single_csv(content)
                        if df_part is not None:
                            all_dfs.append(df_part)
            elif name.endswith(".csv"):
                df_part = _read_settlement_single_csv(data)
                if df_part is not None:
                    all_dfs.append(df_part)
            else:
                continue
        except Exception:
            continue

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)


def _build_espay_settlement_table(df_settlement: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """
    DETAIL SETTLEMENT ESPAY, per Tanggal & Pelabuhan (VA NAME).
    Kolom:
      - VIRTUAL ACCOUNT
      - E-MONEY
      - BCA
      - NON BCA
      - TOTAL VA + E-MONEY
      - TOTAL BCA + NON BCA
    """
    if df_settlement is None or df_settlement.empty:
        return pd.DataFrame()

    df = df_settlement.copy()

    t = pd.to_datetime(df["Settlement Date"], errors="coerce")
    df["Tanggal"] = t.dt.date
    mask = (t.dt.year == year) & (t.dt.month == month)
    df = df.loc[mask].copy()
    if df.empty:
        return pd.DataFrame()

    va_name = df["VA NAME"].fillna("").astype(str).str.upper()

    def map_pelabuhan(name: str) -> Optional[str]:
        if "BAKAUHENI" in name:
            return "ASDP Bakauheni"
        if "GILIMANUK" in name:
            return "ASDP Gilimanuk"
        if "KETAPANG" in name:
            return "ASDP Ketapang"
        if "MERAK" in name:
            return "ASDP Merak"
        return None

    df["Pelabuhan"] = va_name.apply(map_pelabuhan)
    df = df[df["Pelabuhan"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    # Settlement Amount dibagi 100 (sumber kelebihan 2 nol)
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
        ]
        .sum()
        .reset_index()
    )
    grouped[["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]] = grouped[
        ["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]
    ].fillna(0.0)

    unique_ports = grouped["Pelabuhan"].dropna().unique()
    if len(unique_ports) == 0:
        return pd.DataFrame()

    days_in_month = monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days_in_month + 1)]

    full_idx = pd.MultiIndex.from_product(
        [all_dates, unique_ports], names=["Tanggal", "Pelabuhan"]
    )
    full_df = pd.DataFrame(index=full_idx).reset_index()

    out = full_df.merge(grouped, on=["Tanggal", "Pelabuhan"], how="left")

    for col in ["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]:
        if col not in out.columns:
            out[col] = 0.0
        else:
            out[col] = out[col].fillna(0.0)

    # Kolom tambahan TOTAL VA + E-MONEY dan TOTAL BCA + NON BCA
    out["TOTAL VA + E-MONEY"] = out["VIRTUAL ACCOUNT"] + out["E-MONEY"]
    out["TOTAL BCA + NON BCA"] = out["BCA"] + out["NON BCA"]

    out = out.sort_values(["Pelabuhan", "Tanggal"]).reset_index(drop=True)

    desired_order = [
        "Tanggal",
        "Pelabuhan",
        "VIRTUAL ACCOUNT",
        "E-MONEY",
        "TOTAL VA + E-MONEY",
        "BCA",
        "NON BCA",
        "TOTAL BCA + NON BCA",
    ]
    existing = [c for c in desired_order if c in out.columns]
    others = [c for c in out.columns if c not in existing]
    out = out[existing + others]

    return out


# =========================== Settlement Finnet (CSV only, zipped) ===========================

def _read_finnet_single_csv(content: bytes) -> Optional[pd.DataFrame]:
    """
    Settlement Finnet by Telkom: baca satu CSV.
    Kolom wajib: Payment Method, Merchant Amount, Payment Date Time, Merchant Name.
    Nama kolom fleksibel: beda spasi/underscore, atau sedikit kepotong
    (misal "Payment Date Tim" tetap dikenali sebagai "Payment Date Time").
    """
    try:
        text = content.decode("utf-8-sig", errors="ignore")
        df = pd.read_csv(io.StringIO(text), sep=",")
    except Exception:
        return None

    original_cols = list(df.columns.astype(str))
    norm_map_trim = {c: c.strip() for c in original_cols}
    df.rename(columns=norm_map_trim, inplace=True)

    norm_cols = {c: _norm_colname(c) for c in df.columns}
    rename_map = {}
    for req in FINNET_REQUIRED_COLS:
        req_norm = _norm_colname(req)
        matched_col = None
        for real, norm in norm_cols.items():
            if norm == req_norm or norm.startswith(req_norm) or req_norm.startswith(norm):
                matched_col = real
                break
        if matched_col is not None:
            rename_map[matched_col] = req

    df.rename(columns=rename_map, inplace=True)

    missing = [c for c in FINNET_REQUIRED_COLS if c not in df.columns]
    if missing:
        st.warning("Settlement Finnet: Kolom wajib belum lengkap di salah satu file.")
        st.write("Kolom yang ada di file Finnet:", original_cols)
        st.write("Kolom yang masih kurang (versi yang diharapkan kode):", missing)
        return None

    return df[FINNET_REQUIRED_COLS].copy()


def _load_settlement_finnet(files: List["st.runtime.uploaded_file_manager.UploadedFile"]) -> pd.DataFrame:
    """Settlement Finnet: CSV di dalam ZIP (boleh juga CSV langsung)."""
    all_dfs: List[pd.DataFrame] = []
    for f in files:
        try:
            data = f.getvalue()
        except Exception:
            data = f.read()
        name = f.name.lower()

        try:
            if name.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    for info in zf.infolist():
                        if info.is_dir():
                            continue
                        low = info.filename.lower()
                        if not low.endswith(".csv"):
                            continue
                        content = zf.read(info)
                        df_part = _read_finnet_single_csv(content)
                        if df_part is not None:
                            all_dfs.append(df_part)
            elif name.endswith(".csv"):
                df_part = _read_finnet_single_csv(data)
                if df_part is not None:
                    all_dfs.append(df_part)
            else:
                continue
        except Exception:
            continue

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)


def _build_finnet_settlement_table(df_finnet: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """
    DETAIL SETTLEMENT FINNET BY TELKOM (per Tanggal & Pelabuhan = Merchant Name).

    - Tanggal: dari kolom Payment Date Time (jam diabaikan),
      difilter sesuai tahun & bulan parameter.
    - Pelabuhan: dari Merchant Name (Bakauheni/Gilimanuk/Ketapang/Merak).
    - Amount: dari Merchant Amount.
    - Klasifikasi (Payment Method):
        * VIRTUAL ACCOUNT : Payment Method mengandung "VA"
        * E-MONEY         : Payment Method TIDAK mengandung "VA"
        * BCA             : Payment Method mengandung "BCA" atau "blu"
        * NON BCA         : Payment Method TIDAK mengandung "BCA" dan TIDAK mengandung "blu"
    """
    if df_finnet is None or df_finnet.empty:
        return pd.DataFrame()

    df = df_finnet.copy()

    # ===== PARSING TANGGAL (abaikan jam) =====
    raw_dt = df["Payment Date Time"].astype(str).str.strip()
    date_only_str = raw_dt.str.replace(r"[T ].*$", "", regex=True)
    t = pd.to_datetime(date_only_str, errors="coerce", dayfirst=True)
    df["Tanggal"] = t.dt.date

    mask = (t.dt.year == year) & (t.dt.month == month)
    df = df.loc[mask].copy()
    if df.empty:
        return pd.DataFrame()

    # ===== PELABUHAN (Merchant Name) =====
    mn = df["Merchant Name"].fillna("").astype(str).str.upper()

    def map_pelabuhan(name: str) -> Optional[str]:
        if "BAKAUHENI" in name:
            return "ASDP Bakauheni"
        if "GILIMANUK" in name:
            return "ASDP Gilimanuk"
        if "KETAPANG" in name:
            return "ASDP Ketapang"
        if "MERAK" in name:
            return "ASDP Merak"
        return None

    df["Pelabuhan"] = mn.apply(map_pelabuhan)
    df = df[df["Pelabuhan"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    # ===== MERCHANT AMOUNT -> NUMERIC =====
    amt_raw = df["Merchant Amount"].astype(str).str.strip()
    amt_clean = amt_raw.str.replace(r"[^\d\-]", "", regex=True)
    amt = pd.to_numeric(amt_clean, errors="coerce").fillna(0.0)

    # ===== KLASIFIKASI BERDASARKAN PAYMENT METHOD =====
    pm = df["Payment Method"].fillna("").astype(str)

    is_va = pm.str.contains("VA", case=False, na=False)
    is_emoney = ~is_va
    is_bca = pm.str.contains("BCA", case=False, na=False) | pm.str.contains("blu", case=False, na=False)
    is_non_bca = ~(pm.str.contains("BCA", case=False, na=False) | pm.str.contains("blu", case=False, na=False))

    df["VIRTUAL ACCOUNT"] = amt.where(is_va, 0.0)
    df["E-MONEY"] = amt.where(is_emoney, 0.0)
    df["BCA"] = amt.where(is_bca, 0.0)
    df["NON BCA"] = amt.where(is_non_bca, 0.0)

    grouped = (
        df.groupby(["Tanggal", "Pelabuhan"], dropna=False)[
            ["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]
        ]
        .sum()
        .reset_index()
    )
    grouped[["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]] = grouped[
        ["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]
    ].fillna(0.0)

    unique_ports = grouped["Pelabuhan"].dropna().unique()
    if len(unique_ports) == 0:
        return pd.DataFrame()

    days_in_month = monthrange(year, month)[1]
    all_dates = [date(year, month, d) for d in range(1, days_in_month + 1)]

    full_idx = pd.MultiIndex.from_product(
        [all_dates, unique_ports], names=["Tanggal", "Pelabuhan"]
    )
    full_df = pd.DataFrame(index=full_idx).reset_index()

    out = full_df.merge(grouped, on=["Tanggal", "Pelabuhan"], how="left")

    for col in ["VIRTUAL ACCOUNT", "E-MONEY", "BCA", "NON BCA"]:
        if col not in out.columns:
            out[col] = 0.0
        else:
            out[col] = out[col].fillna(0.0)

    out = out.sort_values(["Pelabuhan", "Tanggal"]).reset_index(drop=True)
    return out


# =========================== Streamlit UI helpers ===========================

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
        "BCA": "BCA",
        "NON BCA": "NON BCA",
    }
    df_show.rename(columns=col_rename, inplace=True)
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
    month = st.sidebar.selectbox("Bulan", options=list(range(1, 13)), index=today.month - 1, format_func=lambda m: month_names[m])

    up_files = st.sidebar.file_uploader(
        "Upload Payment Report: ZIP / beberapa Excel (.xlsx/.xls/.xlsb) / CSV",
        type=["zip", "xlsx", "xls", "xlsb", "csv"],
        accept_multiple_files=True,
    )

    settlement_files = st.sidebar.file_uploader(
        "Upload Settlement ESPAY (ZIP / .csv)",
        type=["zip", "csv"],
        accept_multiple_files=True,
        key="settlement_espay",
    )

    finnet_files = st.sidebar.file_uploader(
        "Upload Settlement Finnet by Telkom (ZIP / .csv)",
        type=["zip", "csv"],
        accept_multiple_files=True,
        key="settlement_finnet",
    )

    highlight = st.sidebar.checkbox("Highlight kolom Selisih ≠ 0 (Payment Report)", value=True)

    if not up_files:
        st.info("Silakan upload file Payment Report di panel kiri (bisa banyak file atau ZIP).")
        return

    # ===== TABEL UTAMA PAYMENT REPORT =====
    with st.spinner("Memproses file Payment Report secara streaming…"):
        agg = _load_and_aggregate(up_files, year=year, month=month)

    result = _build_result_from_agg(agg)
    if result.empty:
        st.warning(
            "Tidak ada data valid setelah filter periode & kolom wajib.\n"
            "- Pastikan kolom wajib ada dan namanya persis.\n"
            "- Pastikan periode Tahun/Bulan sesuai dengan kolom TANGGAL PEMBAYARAN.\n"
            "- Jika upload .xlsb, pastikan 'pyxlsb' sudah terinstal."
        )
        return

    st.subheader(f"Hasil Rekonsiliasi Payment • Periode: {month_names[month]} {year}")

    ports = list(result["Pelabuhan"].dropna().unique())
    ports.sort()
    tabs = st.tabs(ports if ports else ["(Tidak ada Pelabuhan)"])
    for tab, port in zip(tabs, ports):
        with tab:
            st.markdown(f"**Pelabuhan: {port}**")
            _render_port_table(port, result[result["Pelabuhan"] == port], highlight=highlight)

    # ===== DETAIL SETTLEMENT ESPAY =====
    st.divider()
    st.subheader("DETAIL SETTLEMENT ESPAY")

    if settlement_files:
        with st.spinner("Memproses file Settlement ESPAY (CSV)…"):
            df_settlement_raw = _load_settlement_espay(settlement_files)
            df_espay = _build_espay_settlement_table(df_settlement_raw, year=year, month=month)

        if df_espay.empty:
            st.warning(
                "File Settlement ESPAY tidak memiliki data lengkap, "
                "tidak ada di periode yang dipilih, atau VA NAME tidak dikenali."
            )
        else:
            st.markdown("**Rekap Settlement ESPAY per Pelabuhan (VA NAME) • Tanggal 1–akhir bulan**")
            ports_espay = list(df_espay["Pelabuhan"].dropna().unique())
            ports_espay.sort()
            tabs_espay = st.tabs(ports_espay if ports_espay else ["(Tidak ada Pelabuhan Settlement ESPAY)"])
            for tab, port in zip(tabs_espay, ports_espay):
                with tab:
                    st.markdown(f"**Pelabuhan: {port}**")
                    _render_espay_port_table(df_espay[df_espay["Pelabuhan"] == port])
    else:
        st.info("Belum ada file Settlement ESPAY (CSV) yang di-upload di sidebar.")

    # ===== DETAIL SETTLEMENT FINNET BY TELKOM =====
    st.divider()
    st.subheader("DETAIL SETTLEMENT FINNET BY TELKOM")

    if finnet_files:
        with st.spinner("Memproses file Settlement Finnet (CSV)…"):
            df_finnet_raw = _load_settlement_finnet(finnet_files)
            df_finnet = _build_finnet_settlement_table(df_finnet_raw, year=year, month=month)

        if df_finnet.empty:
            st.warning(
                "File Settlement Finnet tidak memiliki data lengkap "
                "atau tidak ada data untuk periode yang dipilih."
            )
            placeholder = pd.DataFrame(columns=["Tanggal", "Virtual Account", "E-Money", "BCA", "NON BCA"])
            st.markdown("**Struktur kolom Settlement Finnet (data belum terbaca):**")
            st.dataframe(placeholder, use_container_width=True)
        else:
            st.markdown("**Rekap Settlement Finnet per Pelabuhan (Merchant Name) • Tanggal 1–akhir bulan**")
            ports_finnet = list(df_finnet["Pelabuhan"].dropna().unique())
            ports_finnet.sort()
            tabs_finnet = st.tabs(ports_finnet if ports_finnet else ["(Tidak ada Pelabuhan Settlement Finnet)"])
            for tab, port in zip(tabs_finnet, ports_finnet):
                with tab:
                    st.markdown(f"**Pelabuhan: {port}**")
                    _render_finnet_port_table(df_finnet[df_finnet["Pelabuhan"] == port])
    else:
        st.info("Belum ada file Settlement Finnet (ZIP/CSV) yang di-upload di sidebar.")
        placeholder = pd.DataFrame(columns=["Tanggal", "Virtual Account", "E-Money", "BCA", "NON BCA"])
        st.markdown("**Struktur kolom Settlement Finnet:**")
        st.dataframe(placeholder, use_container_width=True)

    # ===== Unduh gabungan Payment Report =====
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
        st.warning(
            "Ekspor Excel dinonaktifkan. Tambahkan `xlsxwriter>=3.1` atau `openpyxl>=3.1` di requirements."
            + (f"\nDetail: {err_msg}" if err_msg else "")
        )

    with st.expander("Aturan, Kolom Wajib & Per-Pelabuhan / Merchant Name"):
        st.markdown(
            f"""
**Kolom Wajib (Payment Report):**  
H=**{COL_H}**, B=**{COL_B}**, AA=**{COL_AA}**, K=**{COL_K}**, X=**{COL_X}**, ASAL=**{COL_ASAL}**.

**Split per Pelabuhan (Payment Report):**  
Tabel dipecah berdasarkan kolom **ASAL**.  
Kolom hasil: kategori (Cash…Finnet), **Total**, **BCA**, **NON BCA**, **NON**, **TOTAL**, **Selisih** (highlight ≠ 0).  
Subtotal di bawah tiap tabel pelabuhan.

**Settlement ESPAY (CSV):**  
Kolom wajib: **{", ".join(SETTLEMENT_REQUIRED_COLS)}**.  
- **VIRTUAL ACCOUNT** : Product Name mengandung `"VA"`.  
- **E-MONEY**         : Product Name yang **tidak** mengandung `"VA"`.  
- **BCA**             : Product Name mengandung `"BCA VA Online"` atau `"blu by BCA Digital"`.  
- **NON BCA**         : Product Name selain dua kriteria BCA di atas.  
Tambahan kolom:
- **TOTAL VA + E-MONEY** = VIRTUAL ACCOUNT + E-MONEY  
- **TOTAL BCA + NON BCA** = BCA + NON BCA  
Pelabuhan dari **VA NAME**: BAKAUHENI, GILIMANUK, KETAPANG, MERAK.

**Settlement Finnet by Telkom (CSV di ZIP):**  
Kolom wajib: **{", ".join(FINNET_REQUIRED_COLS)}**.  
- **Tanggal** : dari **Payment Date Time** (jam diabaikan), difilter sesuai Tahun/Bulan parameter.  
- **Pelabuhan** : diambil dari **Merchant Name**, dipetakan ke:  
  - `"ASDP Bakauheni"`  
  - `"ASDP Gilimanuk"`  
  - `"ASDP Ketapang"`  
  - `"ASDP Merak"`  
- **Virtual Account** : Payment Method mengandung `"VA"`.  
- **E-Money**         : Payment Method **tidak** mengandung `"VA"`.  
- **BCA**             : Payment Method mengandung `"BCA"` atau `"blu"`.  
- **NON BCA**         : Payment Method tidak mengandung `"BCA"` dan tidak mengandung `"blu"`.  

Rekap per **Tanggal & Pelabuhan (Merchant Name)** untuk 1–akhir bulan, dengan baris **Subtotal** di tiap Pelabuhan.  
Jika data Finnet belum terbaca (kolom tidak lengkap atau periode kosong), akan ditampilkan juga daftar kolom asli file Finnet dan kolom yang dianggap kurang.
"""
        )


if __name__ == "__main__":
    main()

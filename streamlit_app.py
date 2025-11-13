# path: streamlit_app.py
import io
import zipfile
from datetime import date
from collections import defaultdict, OrderedDict
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
from openpyxl import load_workbook  # read-only streaming untuk .xlsx/.xls


# =========================== Konfigurasi & Konstanta ===========================

COL_H = "TIPE PEMBAYARAN"                      # H
COL_B = "TANGGAL PEMBAYARAN"                   # B
COL_AA = "REF NO"                              # AA
COL_K = "TOTAL TARIF TANPA BIAYA ADMIN (Rp.)"  # K
COL_X = "SOF ID"                               # X
REQUIRED_COLS = [COL_H, COL_B, COL_AA, COL_K, COL_X]

CAT_COLS = [
    "Cash",
    "Prepaid BRI",
    "Prepaid BNI",
    "Prepaid Mandiri",
    "Prepaid BCA",
    "SKPT",
    "IFCS",
    "Reedem",   # dukung "redeem"/"reedem"
    "ESPAY",    # finpay + AA startswith esp
    "Finnet",   # finpay + AA bukan esp
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
VALID_EXTS = (".xlsx", ".xls", ".csv")


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


# =========================== Agregator streaming ===========================

def _empty_agg():
    return defaultdict(lambda: defaultdict(float))  # date -> {col -> sum}

def _update_agg_series(agg, ser: pd.Series, colname: str) -> None:
    if ser.empty:
        return
    for dt, val in ser.items():
        agg[dt][colname] += float(val)

def _apply_rules_and_update(df_chunk: pd.DataFrame, agg) -> None:
    # normalisasi
    H = df_chunk[COL_H].fillna("").astype(str).str.lower()
    AA = df_chunk[COL_AA].fillna("").astype(str).str.lower()
    X = df_chunk[COL_X].fillna("").astype(str).str.lower()
    amt = pd.to_numeric(df_chunk[COL_K], errors="coerce").fillna(0)
    tgl = df_chunk["Tanggal"]

    def sum_by_date(mask) -> pd.Series:
        if mask.any():
            return amt[mask].groupby(tgl[mask]).sum(min_count=1)
        return pd.Series(dtype="float64")

    # === Kategori utama (FIX: pakai .str.contains, bukan .str_contains) ===
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
        _update_agg_series(agg, sum_by_date(m), name)

    # === BCA / NON BCA (FIX: .str.contains) ===
    is_finpay = H.str.contains("finpay", na=False)
    is_bca_tag = X.str.contains("vabcaespay", na=False) | X.str.contains("bluespay", na=False)
    _update_agg_series(agg, sum_by_date(is_finpay & is_bca_tag), "BCA")
    _update_agg_series(agg, sum_by_date(is_finpay & (~is_bca_tag)), "NON BCA")


# =========================== Pembaca cepat (CSV & Excel) ===========================

def _process_csv_fast(data: bytes, year: int, month: int, agg) -> None:
    itr = pd.read_csv(
        io.BytesIO(data),
        usecols=REQUIRED_COLS,
        chunksize=CSV_CHUNK_ROWS,
        dtype={COL_H: "string", COL_AA: "string", COL_X: "string"},
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

def _flush_xlsx_batch(buf: List[List], year: int, month: int, agg) -> None:
    df = pd.DataFrame(buf, columns=[COL_H, COL_B, COL_AA, COL_K, COL_X])
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
                    else:
                        _process_csv_fast(content, year, month, agg)
        elif name.endswith((".xlsx", ".xls")):
            _process_xlsx_streaming(data, year, month, agg)
        elif name.endswith(".csv"):
            _process_csv_fast(data, year, month, agg)
        else:
            continue
    return agg


# =========================== Build hasil dari aggregator ===========================

def _build_result_from_agg(agg) -> pd.DataFrame:
    if not agg:
        return pd.DataFrame()

    all_dates = sorted(agg.keys())
    all_cols = CAT_COLS + ["Total", "BCA", "NON BCA", "NON", "TOTAL", "Selisih"]

    rows = []
    for dt in all_dates:
        row = {"Tanggal": dt}
        for c in CAT_COLS:
            row[c] = agg[dt].get(c, 0.0)
        row["Total"] = sum(row[c] for c in CAT_COLS)
        bca = agg[dt].get("BCA", 0.0)
        nonbca = agg[dt].get("NON BCA", 0.0)
        row["BCA"] = bca
        row["NON BCA"] = nonbca
        row["NON"] = sum(row[c] for c in NON_COMPONENTS)
        row["TOTAL"] = bca + nonbca + row["NON"]
        row["Selisih"] = row["TOTAL"] - row["Total"]
        rows.append(row)

    return pd.DataFrame(rows, columns=["Tanggal"] + all_cols)


# =========================== Streamlit UI ===========================

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

def main() -> None:
    st.set_page_config(page_title="Rekonsiliasi Payment Report", layout="wide")
    st.title("Rekonsiliasi Payment Report")

    # Sidebar: filter selalu ada + uploader multi-file/ZIP
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
        "Upload ZIP / beberapa Excel (.xlsx/.xls) / CSV",
        type=["zip", "xlsx", "xls", "csv"],
        accept_multiple_files=True,
    )
    highlight = st.sidebar.checkbox("Highlight kolom Selisih ≠ 0", value=True)

    if not up_files:
        st.info("Silakan upload file di panel kiri (bisa banyak file atau ZIP).")
        return

    # Proses streaming semua file (RAM-efisien)
    with st.spinner("Memproses file besar secara streaming…"):
        agg = _load_and_aggregate(up_files, year=year, month=month)

    result = _build_result_from_agg(agg)
    if result.empty:
        st.warning("Tidak ada data valid setelah filter periode & kolom wajib.")
        return

    # Tampilkan + subtotal + format angka bulat
    st.subheader(f"Hasil Rekonsiliasi • Periode: {month_names[month]} {year}")
    result_display = result.copy()
    result_display["Tanggal"] = pd.to_datetime(result_display["Tanggal"]).dt.strftime("%d/%m/%Y")
    result_display = _add_subtotal_row(result_display, label="Subtotal", date_col="Tanggal")

    numeric_cols = result_display.select_dtypes(include="number").columns
    result_display[numeric_cols] = result_display[numeric_cols].round(0).astype("Int64")

    try:
        st.dataframe(_style_table(result_display, highlight=highlight), use_container_width=True)
    except Exception as e:
        st.warning(f"Gagal menerapkan styling. Tampilkan tabel biasa. Detail: {e}")
        st.dataframe(result_display, use_container_width=True)

    # Unduh
    st.divider()
    st.subheader("Unduh Hasil")
    csv_bytes = result_display.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Unduh CSV", data=csv_bytes, file_name=f"rekonsiliasi_payment_{year}_{month:02d}.csv", mime="text/csv")

    excel_bytes, engine_used, err_msg = _to_excel_bytes(result_display, sheet_name="Rekonsiliasi")
    if excel_bytes:
        st.download_button(
            f"Unduh Excel (.xlsx){' • ' + engine_used if engine_used else ''}",
            data=excel_bytes,
            file_name=f"rekonsiliasi_payment_{year}_{month:02d}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.warning(
            "Ekspor Excel dinonaktifkan. Tambahkan `xlsxwriter>=3.1` atau `openpyxl>=3.1` di requirements."
            + (f"\nDetail: {err_msg}" if err_msg else "")
        )

    with st.expander("Tips Performa & Aturan"):
        st.markdown(
            f"""
**Tips Performa:** CSV/ZIP lebih cepat dari Excel. Excel dibaca **streaming** hanya kolom wajib: {", ".join(REQUIRED_COLS)}.
**Perhitungan:** BCA (finpay+SOF `vabcaespay|bluespay`), NON BCA (finpay selain itu), NON (jumlah kategori non-finpay), TOTAL=BCA+NON BCA+NON, Selisih=TOTAL−Total.
"""
        )


if __name__ == "__main__":
    main()

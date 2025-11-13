# path: streamlit_app.py
import io
from collections import OrderedDict
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st


# === Kolom wajib (header harus persis) ===
COL_H = "TIPE PEMBAYARAN"  # H
COL_B = "TANGGAL PEMBAYARAN"  # B
COL_AA = "REF NO"  # AA
COL_K = "TOTAL TARIF TANPA BIAYA ADMIN (Rp.)"  # K
COL_X = "SOF ID"  # X
REQUIRED_COLS = [COL_H, COL_B, COL_AA, COL_K, COL_X]


# === Helpers ===
def _contains_token(series: pd.Series, token: str) -> pd.Series:
    token = (token or "").lower()
    return series.fillna("").astype(str).str.lower().str.contains(token, na=False)


def _startswith_token(series: pd.Series, prefix: str) -> pd.Series:
    prefix = (prefix or "").lower()
    return series.fillna("").astype(str).str.lower().str.startswith(prefix)


# Dukung "redeem" & "reedem"
CATEGORY_RULES = OrderedDict(
    [
        ("Cash", lambda H, AA: _contains_token(H, "cash")),
        ("Prepaid BRI", lambda H, AA: _contains_token(H, "prepaid-bri")),
        ("Prepaid BNI", lambda H, AA: _contains_token(H, "prepaid-bni")),
        ("Prepaid Mandiri", lambda H, AA: _contains_token(H, "prepaid-mandiri")),
        ("Prepaid BCA", lambda H, AA: _contains_token(H, "prepaid-bca")),
        ("SKPT", lambda H, AA: _contains_token(H, "skpt")),
        ("IFCS", lambda H, AA: _contains_token(H, "ifcs")),
        ("Reedem", lambda H, AA: _contains_token(H, "reedem") | _contains_token(H, "redeem")),
        ("ESPAY", lambda H, AA: _contains_token(H, "finpay") & _startswith_token(AA, "esp")),
        ("Finnet", lambda H, AA: _contains_token(H, "finpay") & (~_startswith_token(AA, "esp"))),
    ]
)


def _ensure_required_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError("Kolom wajib tidak ditemukan: " + ", ".join(missing) + ".")


def reconcile(
    df: pd.DataFrame,
    col_h: str,
    col_aa: str,
    amount_col: str,
    group_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Agregasi per kategori; amount dari K; group-by Tanggal."""
    H = df[col_h]
    AA = df[col_aa]
    amount = pd.to_numeric(df[amount_col], errors="coerce").fillna(0)

    pieces = {}
    if group_cols:
        for name, rule in CATEGORY_RULES.items():
            mask = rule(H, AA)
            grp = df.loc[mask, group_cols].copy()
            grp["_amt"] = amount.loc[mask].values  # kenapa: hindari konflik nama
            pieces[name] = grp.groupby(group_cols, dropna=False)["_amt"].sum(min_count=1)
        result = pd.concat(pieces, axis=1).fillna(0)
    else:
        idx = pd.Index(["TOTAL"])
        for name, rule in CATEGORY_RULES.items():
            mask = rule(H, AA)
            pieces[name] = pd.Series([amount.loc[mask].sum()], index=idx)
        result = pd.concat(pieces, axis=1).fillna(0)

    result["Total"] = result.sum(axis=1)
    return result


def compute_bca_nonbca_from_raw(
    df: pd.DataFrame, group_cols: List[str], type_col: str, sof_col: str, amount_col: str
) -> pd.DataFrame:
    """BCA/NON BCA dari finpay + SOF ID; sum amount per group."""
    t = df[type_col].fillna("").astype(str).str.strip().str.lower()
    s = df[sof_col].fillna("").astype(str).str.strip().str.lower()
    amt = pd.to_numeric(df[amount_col], errors="coerce").fillna(0)

    is_finpay = t.str.contains("finpay", na=False)
    is_bca_tag = s.str.contains("vabcaespay", na=False) | s.str.contains("bluespay", na=False)

    df_tmp = df.copy()
    df_tmp["_amt"] = amt

    sub_bca = df_tmp.loc[is_finpay & is_bca_tag, group_cols + ["_amt"]]
    ser_bca = sub_bca.groupby(group_cols, dropna=False)["_amt"].sum(min_count=1) if not sub_bca.empty else pd.Series(dtype="float64")

    sub_nonbca = df_tmp.loc[is_finpay & (~is_bca_tag), group_cols + ["_amt"]]
    ser_nonbca = sub_nonbca.groupby(group_cols, dropna=False)["_amt"].sum(min_count=1) if not sub_nonbca.empty else pd.Series(dtype="float64")

    out = pd.concat({"BCA": ser_bca, "NON BCA": ser_nonbca}, axis=1).fillna(0)
    return out


def _to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Rekonsiliasi") -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
    """Tulis XLSX dengan fallback engine. Nonaktif bila engine tidak ada."""
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


def _add_subtotal_row(df_display: pd.DataFrame, label: str = "Subtotal", date_col: str = "Tanggal") -> pd.DataFrame:
    """Tambahkan baris Subtotal (penjumlahan kolom numerik)."""
    numeric_cols = df_display.select_dtypes(include="number").columns.tolist()
    totals = df_display[numeric_cols].sum()
    subtotal = {c: (totals[c] if c in totals else None) for c in df_display.columns}
    subtotal[date_col] = label
    return pd.concat([df_display, pd.DataFrame([subtotal])], ignore_index=True)


def _style_selisih(df_display: pd.DataFrame) -> "pd.io.formats.style.Styler":
    """Warnai merah jika Selisih ≠ 0."""
    def highlight(series: pd.Series):
        return [
            "background-color:#fdecea; color:#b71c1c; font-weight:600;" if (pd.notna(v) and float(v) != 0) else ""
            for v in series
        ]
    styler = df_display.style
    if "Selisih" in df_display.columns:
        styler = styler.apply(highlight, subset=["Selisih"])
    return styler


def main() -> None:
    st.set_page_config(page_title="Rekonsiliasi Payment Report", layout="wide")
    st.title("Rekonsiliasi Payment Report")

    # Sidebar: uploader + periode
    up = st.sidebar.file_uploader("Upload Excel (.xlsx/.xls) atau CSV", type=["xlsx", "xls", "csv"])
    if not up:
        st.info("Silakan upload file di panel kiri.")
        return

    # Sheet pertama
    if up.name.lower().endswith((".xlsx", ".xls")):
        xls = pd.ExcelFile(up)
        sheet_name = xls.sheet_names[0]
        df = xls.parse(sheet_name)
        st.sidebar.caption(f"Sheet dipakai: **{sheet_name}** (otomatis sheet pertama).")
    else:
        df = pd.read_csv(up)
        st.sidebar.caption("File CSV terbaca.")

    if df.empty:
        st.warning("Data kosong.")
        return

    # Validasi
    try:
        _ensure_required_columns(df)
    except Exception as e:
        st.error(str(e))
        st.stop()

    # Tanggal dari B (date-only)
    tanggal_full = pd.to_datetime(df[COL_B], errors="coerce")
    if "Tanggal" in df.columns:
        df.drop(columns=["Tanggal"], inplace=True)
    df.insert(0, "Tanggal", tanggal_full.dt.date)

    # Sidebar: Tahun & Bulan
    years = sorted({d.year for d in tanggal_full.dropna().dt.date.unique()})
    if not years:
        st.error("Kolom tanggal tidak berisi nilai tanggal yang valid.")
        st.stop()
    default_year = max(years)
    year = st.sidebar.selectbox("Tahun", options=years, index=years.index(default_year))

    month_names = {
        1: "01 - Januari", 2: "02 - Februari", 3: "03 - Maret", 4: "04 - April",
        5: "05 - Mei", 6: "06 - Juni", 7: "07 - Juli", 8: "08 - Agustus",
        9: "09 - September", 10: "10 - Oktober", 11: "11 - November", 12: "12 - Desember",
    }
    months_in_year = sorted({d.month for d in tanggal_full.dropna() if d.year == year})
    default_month = months_in_year[-1] if months_in_year else 1
    month = st.sidebar.selectbox(
        "Bulan",
        options=list(range(1, 13)),
        index=(default_month - 1) if 1 <= default_month <= 12 else 0,
        format_func=lambda m: month_names[m],
    )

    # Filter periode
    periode_mask = (tanggal_full.dt.year == year) & (tanggal_full.dt.month == month)
    df_period = df.loc[periode_mask].copy()
    if df_period.empty:
        st.warning(f"Tidak ada data untuk periode **{month_names[month]} {year}**.")
        st.stop()
    df_period["Tanggal"] = pd.to_datetime(df_period[COL_B], errors="coerce").dt.date

    # Rekon kategori
    with st.spinner("Menghitung rekonsiliasi kategori..."):
        result = reconcile(
            df=df_period,
            col_h=COL_H,
            col_aa=COL_AA,
            amount_col=COL_K,
            group_cols=["Tanggal"],
        )

    # BCA / NON BCA (finpay + SOF ID)
    add_finpay = compute_bca_nonbca_from_raw(
        df=df_period, group_cols=["Tanggal"], type_col=COL_H, sof_col=COL_X, amount_col=COL_K
    )
    result = result.join(add_finpay, how="left").fillna(0)

    # NON = jumlah kategori non-finpay
    non_components = ["Cash", "Prepaid BRI", "Prepaid BNI", "Prepaid Mandiri", "Prepaid BCA", "SKPT", "IFCS", "Reedem"]
    existing = [c for c in non_components if c in result.columns]
    result["NON"] = result[existing].sum(axis=1) if existing else 0

    # TOTAL (baru) = BCA + NON BCA + NON
    for need in ["BCA", "NON BCA", "NON"]:
        if need not in result.columns:
            result[need] = 0
    result["TOTAL"] = result[["BCA", "NON BCA", "NON"]].sum(axis=1)

    # S E L I S I H  = TOTAL − Total (audit)
    result["Selisih"] = result["TOTAL"] - result["Total"]

    # Urut kolom: …, Total, BCA, NON BCA, NON, TOTAL, Selisih
    cols = list(result.columns)
    for c in ["BCA", "NON BCA", "NON", "TOTAL", "Selisih"]:
        if c not in cols:
            cols.append(c)
    if "Total" in cols:
        for c in ["BCA", "NON BCA", "NON", "TOTAL", "Selisih"]:
            if c in cols:
                cols.remove(c)
        insert_pos = cols.index("Total") + 1
        cols[insert_pos:insert_pos] = ["BCA", "NON BCA", "NON", "TOTAL", "Selisih"]
    result = result[cols]

    # Tampilkan + Subtotal + Styling Selisih
    st.subheader(f"Hasil Rekonsiliasi • Periode: {month_names[month]} {year}")
    result_display = result.reset_index()
    if "Tanggal" in result_display.columns:
        result_display["Tanggal"] = pd.to_datetime(result_display["Tanggal"]).dt.strftime("%d/%m/%Y")
        result_display = result_display[["Tanggal"] + [c for c in result_display.columns if c != "Tanggal"]]
    result_display = _add_subtotal_row(result_display, label="Subtotal", date_col="Tanggal")

    styled = _style_selisih(result_display)
    st.dataframe(styled, use_container_width=True)

    # U

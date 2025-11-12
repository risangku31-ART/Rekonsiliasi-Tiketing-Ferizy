# path: streamlit_app.py
import io
from collections import OrderedDict
from typing import List, Optional, Tuple

import pandas as pd
import streamlit as st


# --- Konstanta kolom wajib (fixed) ---
COL_H = "TIPE PEMBAYARAN"  # H
COL_B = "TANGGAL PEMBAYARAN"  # B
COL_AA = "REF NO"  # AA
COL_K = "TOTAL TARIF TANPA BIAYA ADMIN (Rp.)"  # K

REQUIRED_COLS = [COL_H, COL_B, COL_AA, COL_K]


# --- Helpers ---
def _contains_token(series: pd.Series, token: str) -> pd.Series:
    token = (token or "").lower()
    return series.fillna("").astype(str).str.lower().str.contains(token, na=False)


def _startswith_token(series: pd.Series, prefix: str) -> pd.Series:
    prefix = (prefix or "").lower()
    return series.fillna("").astype(str).str.lower().str.startswith(prefix)


CATEGORY_RULES = OrderedDict(
    [
        ("Cash", lambda H, AA: _contains_token(H, "cash")),
        ("Prepaid BRI", lambda H, AA: _contains_token(H, "prepaid-bri")),
        ("Prepaid BNI", lambda H, AA: _contains_token(H, "prepaid-bni")),
        ("Prepaid Mandiri", lambda H, AA: _contains_token(H, "prepaid-mandiri")),
        ("Prepaid BCA", lambda H, AA: _contains_token(H, "prepaid-bca")),
        ("SKPT", lambda H, AA: _contains_token(H, "skpt")),
        ("IFCS", lambda H, AA: _contains_token(H, "ifcs")),
        ("Reedem", lambda H, AA: _contains_token(H, "reedem")),
        ("ESPAY", lambda H, AA: _contains_token(H, "finpay") & _startswith_token(AA, "esp")),
        ("Finnet", lambda H, AA: _contains_token(H, "finpay") & (~_startswith_token(AA, "esp"))),
    ]
)


def _ensure_required_columns(df: pd.DataFrame) -> None:
    """Cek kolom wajib. Gagal cepat bila tidak lengkap."""
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            "Kolom wajib tidak ditemukan: "
            + ", ".join(missing)
            + ". Pastikan header EXACT sesuai yang diminta."
        )


def reconcile(
    df: pd.DataFrame,
    col_h: str,
    col_aa: str,
    amount_col: str,
    group_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Agregasi amount per kategori; 'group_cols' wajib berisi 'Tanggal'."""
    H = df[col_h]
    AA = df[col_aa]
    amount = pd.to_numeric(df[amount_col], errors="coerce").fillna(0)

    pieces = {}
    if group_cols:
        for name, rule in CATEGORY_RULES.items():
            mask = rule(H, AA)
            grp = df.loc[mask, group_cols].copy()
            # Kenapa: menghindari tabrakan nama kolom
            grp["_amt"] = amount.loc[mask].values
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


def _to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Rekonsiliasi") -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
    """Write XLSX dengan fallback engine. Jika engine tak ada → None."""
    for engine in ("xlsxwriter", "openpyxl"):
        try:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine=engine) as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            return buf.getvalue(), engine, None
        except ImportError as e:
            # Kenapa: di Streamlit Cloud kadang salah satu engine belum terpasang.
            continue
        except Exception as e:
            return None, None, f"Gagal menulis Excel dengan {engine}: {e}"
    return None, None, "Tidak ada engine Excel (xlsxwriter/openpyxl). Tambahkan ke requirements."


def main() -> None:
    st.set_page_config(page_title="Rekonsiliasi Payment Report", layout="wide")
    st.title("Rekonsiliasi Payment Report")

    up = st.file_uploader("Upload Excel (.xlsx/.xls) atau CSV", type=["xlsx", "xls", "csv"])
    if not up:
        st.info("Silakan upload file terlebih dahulu.")
        return

    # Load data (sheet picker hanya jika Excel)
    if up.name.lower().endswith((".xlsx", ".xls")):
        xls = pd.ExcelFile(up)
        sheet = st.selectbox("Pilih sheet", xls.sheet_names, index=0)
        df = xls.parse(sheet)
    else:
        df = pd.read_csv(up)

    if df.empty:
        st.warning("Data kosong.")
        return

    # Validasi kolom wajib (fixed, tanpa UI pemilihan kolom)
    try:
        _ensure_required_columns(df)
    except Exception as e:
        st.error(str(e))
        st.stop()

    # Buat kolom 'Tanggal' dari B, abaikan jam; letakkan paling kiri
    tanggal_series = pd.to_datetime(df[COL_B], errors="coerce").dt.date
    if "Tanggal" in df.columns:
        df.drop(columns=["Tanggal"], inplace=True)
    df.insert(0, "Tanggal", tanggal_series)

    # Hitung rekonsiliasi per Tanggal (tanpa UI pengaturan kolom)
    group_cols = ["Tanggal"]
    with st.spinner("Menghitung rekonsiliasi..."):
        try:
            result = reconcile(df, col_h=COL_H, col_aa=COL_AA, amount_col=COL_K, group_cols=group_cols)
        except Exception as e:
            st.error(f"Gagal merekonsiliasi: {e}")
            st.stop()

    # Tampilkan hasil
    st.subheader("Hasil Rekonsiliasi")
    result_display = result.reset_index()
    if "Tanggal" in result_display.columns:
        result_display = result_display[["Tanggal"] + [c for c in result_display.columns if c != "Tanggal"]]
    st.dataframe(result_display, use_container_width=True)

    # Unduhan
    st.divider()
    st.subheader("Unduh Hasil")

    csv_bytes = result_display.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Unduh CSV", data=csv_bytes, file_name="rekonsiliasi_payment.csv", mime="text/csv")

    excel_bytes, engine_used, err_msg = _to_excel_bytes(result_display, sheet_name="Rekonsiliasi")
    if excel_bytes:
        st.download_button(
            f"Unduh Excel (.xlsx){' • ' + engine_used if engine_used else ''}",
            data=excel_bytes,
            file_name="rekonsiliasi_payment.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.warning(
            "Ekspor Excel dinonaktifkan. Tambahkan `xlsxwriter>=3.1` atau `openpyxl>=3.1` di requirements."
            + (f"\nDetail: {err_msg}" if err_msg else "")
        )

    with st.expander("Aturan & Kolom Wajib"):
        st.markdown(
            f"""
**Kolom Wajib (header harus persis):**
- H → **{COL_H}**
- B → **{COL_B}**
- AA → **{COL_AA}**
- K → **{COL_K}**

**Kategori**
- Cash → H `cash`
- Prepaid BRI/BNI/Mandiri/BCA → H `prepaid-...`
- SKPT → H `skpt`
- IFCS → H `ifcs`
- Reedem → H `reedem`
- ESPAY → H `finpay` **dan** AA diawali `esp`
- Finnet → H `finpay` **dan** AA **tidak** diawali `esp`
- **Total** = penjumlahan semua kategori.
"""
        )


if __name__ == "__main__":
    main()

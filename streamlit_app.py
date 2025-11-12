# path: ./streamlit_app.py
import io
from collections import OrderedDict
from typing import List, Optional

import pandas as pd
import streamlit as st


def _default_by_position(cols: List[str], pos0: int) -> Optional[str]:
    """Ambil nama kolom default berdasarkan posisi 0-based (mis. B=1, H=7, K=10, AA=26)."""
    return cols[pos0] if len(cols) > pos0 else None


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
        ("Reedem", lambda H, AA: _contains_token(H, "reedem")),  # sesuai permintaan penamaan
        ("ESPAY", lambda H, AA: _contains_token(H, "finpay") & _startswith_token(AA, "esp")),
        ("Finnet", lambda H, AA: _contains_token(H, "finpay") & (~_startswith_token(AA, "esp"))),
    ]
)


def reconcile(
    df: pd.DataFrame,
    col_h: str,
    col_aa: Optional[str],
    amount_col: str,
    group_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Hitung total per kategori berdasar aturan; group-by bila diminta."""
    if col_h not in df.columns:
        raise ValueError(f"Kolom H tidak ditemukan: {col_h}")
    if col_aa and col_aa not in df.columns:
        raise ValueError(f"Kolom AA tidak ditemukan: {col_aa}")
    if amount_col not in df.columns:
        raise ValueError(f"Kolom nominal tidak ditemukan: {amount_col}")

    H = df[col_h]
    AA = df[col_aa] if col_aa else pd.Series([""] * len(df), index=df.index)
    amount = pd.to_numeric(df[amount_col], errors="coerce").fillna(0)

    pieces = {}
    if group_cols:
        for name, rule in CATEGORY_RULES.items():
            mask = rule(H, AA)
            grp = df.loc[mask, group_cols].copy()
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


def main() -> None:
    st.set_page_config(page_title="Rekonsiliasi Payment Report", layout="wide")
    st.title("Rekonsiliasi Payment Report")
    st.caption("Tanggal dari kolom **B** (jam diabaikan), kategori dari **H**, referensi **AA** (opsional), amount dari **K**.")

    up = st.file_uploader("Upload Excel (.xlsx/.xls) atau CSV", type=["xlsx", "xls", "csv"])
    if not up:
        st.info("Silakan upload file terlebih dahulu.")
        return

    # Load data
    if up.name.lower().endswith((".xlsx", ".xls")):
        xls = pd.ExcelFile(up)
        sheet = st.sidebar.selectbox("Pilih sheet", xls.sheet_names, index=0)
        df = xls.parse(sheet)
    else:
        df = pd.read_csv(up)

    if df.empty:
        st.warning("Data kosong.")
        return

    cols = list(df.columns)

    # Posisi default (0-based): B=1, H=7, K=10, AA=26
    default_b = _default_by_position(cols, 1)
    default_h = _default_by_position(cols, 7)
    default_k = _default_by_position(cols, 10)
    default_aa = _default_by_position(cols, 26)

    st.sidebar.subheader("Pengaturan Kolom")

    # Kolom B → Tanggal (parse date only)
    idx_b = cols.index(default_b) if default_b in cols else 0
    col_b = st.sidebar.selectbox("Kolom B (Tanggal: berisi tanggal+jam)", cols, index=idx_b)
    tanggal_series = pd.to_datetime(df[col_b], errors="coerce").dt.date  # abaikan jam
    if "Tanggal" in df.columns:
        df.drop(columns=["Tanggal"], inplace=True)
    df.insert(0, "Tanggal", tanggal_series)

    # Kolom H (kategori)
    idx_h = list(df.columns).index(default_h) if default_h in df.columns else 1
    col_h = st.sidebar.selectbox("Kolom H (Kategori)", list(df.columns), index=idx_h)

    # Kolom AA (opsional)
    aa_options = ["<tanpa kolom AA>"] + list(df.columns)
    idx_aa = 0 if default_aa not in df.columns else aa_options.index(default_aa)
    sel_aa = st.sidebar.selectbox("Kolom AA (Referensi, opsional)", aa_options, index=idx_aa)
    col_aa = None if sel_aa == "<tanpa kolom AA>" else sel_aa

    # Kolom K (amount) → dikunci jika ada
    if default_k in df.columns:
        amount_col = default_k
        st.sidebar.text_input("Kolom K (Amount, dikunci)", value=amount_col, disabled=True)
    else:
        amount_candidates = list(df.select_dtypes(include="number").columns) or list(df.columns)
        amount_col = st.sidebar.selectbox(
            "Kolom Amount (K tidak ditemukan, pilih manual)", amount_candidates, index=0
        )

    # Group by: wajib Tanggal + tambahan opsional
    extra_group_opts = [c for c in df.columns if c not in {"Tanggal", amount_col}]
    extra_groups = st.sidebar.multiselect("Tambahan Group By (opsional)", options=extra_group_opts, default=[])
    group_cols = ["Tanggal"] + extra_groups

    # ——— HILANGKAN PREVIEW DATA ———

    with st.spinner("Menghitung rekonsiliasi..."):
        try:
            result = reconcile(df, col_h=col_h, col_aa=col_aa, amount_col=amount_col, group_cols=group_cols)
        except Exception as e:
            st.error(f"Gagal merekonsiliasi: {e}")
            return

    st.subheader("Hasil Rekonsiliasi")
    result_display = result.reset_index()

    # Pastikan Tanggal paling kiri
    if "Tanggal" in result_display.columns:
        ordered = ["Tanggal"] + [c for c in result_display.columns if c != "Tanggal"]
        result_display = result_display[ordered]

    st.dataframe(result_display, use_container_width=True)

    st.divider()
    st.subheader("Unduh Hasil")
    csv_bytes = result_display.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Unduh CSV", data=csv_bytes, file_name="rekonsiliasi_payment.csv", mime="text/csv")

    xlsx_buf = io.BytesIO()
    with pd.ExcelWriter(xlsx_buf, engine="xlsxwriter") as writer:
        result_display.to_excel(writer, sheet_name="Rekonsiliasi", index=False)
    st.download_button(
        "Unduh Excel",
        data=xlsx_buf.getvalue(),
        file_name="rekonsiliasi_payment.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    with st.expander("Aturan & Catatan"):
        st.markdown(
            """
**Aturan Kategori**
- Cash → H mengandung `cash`
- Prepaid BRI → H `prepaid-bri`
- Prepaid BNI → H `prepaid-bni`
- Prepaid Mandiri → H `prepaid-mandiri`
- Prepaid BCA → H `prepaid-bca`
- SKPT → H `skpt`
- IFCS → H `ifcs`
- Reedem → H `reedem`
- ESPAY → H `finpay` dan **AA** diawali `esp`
- Finnet → H `finpay` dan **AA** **tidak** diawali `esp`
- **Total** = jumlah semua kategori.

**Catatan**
- Tanggal diparse dari kolom B (jam diabaikan) dan diletakkan di paling kiri.
- Amount default kolom K (dikunci bila ditemukan).
"""
        )


if __name__ == "__main__":
    main()

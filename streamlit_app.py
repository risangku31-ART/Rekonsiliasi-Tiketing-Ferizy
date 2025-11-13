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
COL_X = "SOF ID"  # X (baru, untuk BCA/NON BCA/NON)
REQUIRED_COLS = [COL_H, COL_B, COL_AA, COL_K, COL_X]


# === Helpers kategori ===
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
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            "Kolom wajib tidak ditemukan: " + ", ".join(missing) + ". "
            "Pastikan header persis sesuai permintaan."
        )


def reconcile(
    df: pd.DataFrame,
    col_h: str,
    col_aa: str,
    amount_col: str,
    group_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    H = df[col_h]
    AA = df[col_aa]
    amount = pd.to_numeric(df[amount_col], errors="coerce").fillna(0)

    pieces = {}
    if group_cols:
        for name, rule in CATEGORY_RULES.items():
            mask = rule(H, AA)
            grp = df.loc[mask, group_cols].copy()
            grp["_amt"] = amount.loc[mask].values  # why: hindari bentrok nama
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


def compute_bca_non_columns(
    df: pd.DataFrame,
    group_cols: List[str],
    type_col: str,
    sof_col: str,
    amount_col: str,
) -> pd.DataFrame:
    """
    Hitung kolom BCA / NON BCA / NON:
      - NON      : tipe cash/prepaid/skpt/ifcs/reedem (bukan finpay)
      - BCA      : tipe finpay & SOF ID ∈ {vabcaespay, bluespay}
      - NON BCA  : tipe finpay & SOF ID ∉ {vabcaespay, bluespay}
    """
    t = df[type_col].fillna("").astype(str).str.strip().str.lower()
    s = df[sof_col].fillna("").astype(str).str.strip().str.lower()
    amt = pd.to_numeric(df[amount_col], errors="coerce").fillna(0)

    is_finpay = t.str.contains("finpay", na=False)
    is_bca_tag = s.str.contains("vabcaespay", na=False) | s.str.contains("bluespay", na=False)

    is_non_candidates = (
        t.str.contains("cash", na=False)
        | t.str.contains("prepaid", na=False)
        | t.str.contains("skpt", na=False)
        | t.str.contains("ifcs", na=False)
        | t.str.contains("reedem", na=False)
    )

    mask_non = is_non_candidates & (~is_finpay)
    mask_bca = is_finpay & is_bca_tag
    mask_nonbca = is_finpay & (~is_bca_tag)

    df_tmp = df.copy()
    df_tmp["_amt"] = amt

    agg = {}
    for col_name, mask in [("BCA", mask_bca), ("NON BCA", mask_nonbca), ("NON", mask_non)]:
        sub = df_tmp.loc[mask, group_cols + ["_amt"]]
        if sub.empty:
            agg[col_name] = pd.Series(dtype="float64")
        else:
            agg[col_name] = sub.groupby(group_cols, dropna=False)["_amt"].sum(min_count=1)

    out = pd.concat(agg, axis=1).fillna(0)
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


def main() -> None:
    st.set_page_config(page_title="Rekonsiliasi Payment Report", layout="wide")
    st.title("Rekonsiliasi Payment Report")

    up = st.file_uploader("Upload Excel (.xlsx/.xls) atau CSV", type=["xlsx", "xls", "csv"])
    if not up:
        st.info("Silakan upload file terlebih dahulu.")
        return

    # === Baca data: sheet pertama (Sheet 1) tanpa pilihan ===
    if up.name.lower().endswith((".xlsx", ".xls")):
        xls = pd.ExcelFile(up)
        sheet_name = xls.sheet_names[0]  # Sheet 1
        df = xls.parse(sheet_name)
        st.caption(f"Sheet dipakai: **{sheet_name}** (otomatis sheet pertama).")
    else:
        df = pd.read_csv(up)
        st.caption("File CSV terbaca.")

    if df.empty:
        st.warning("Data kosong.")
        return

    # === Validasi kolom wajib ===
    try:
        _ensure_required_columns(df)
    except Exception as e:
        st.error(str(e))
        st.stop()

    # === Parse tanggal dari kolom B, buat 'Tanggal' paling kiri ===
    tanggal_full = pd.to_datetime(df[COL_B], errors="coerce")
    if "Tanggal" in df.columns:
        df.drop(columns=["Tanggal"], inplace=True)
    df.insert(0, "Tanggal", tanggal_full.dt.date)

    # === Parameter Rekonsiliasi: Tahun & Bulan ===
    years = sorted({d.year for d in tanggal_full.dropna().dt.date.unique()})
    if not years:
        st.error("Kolom tanggal tidak berisi nilai tanggal yang valid.")
        st.stop()
    default_year = max(years)
    year = st.selectbox("Tahun", options=years, index=years.index(default_year))

    month_names = {
        1: "01 - Januari", 2: "02 - Februari", 3: "03 - Maret", 4: "04 - April",
        5: "05 - Mei", 6: "06 - Juni", 7: "07 - Juli", 8: "08 - Agustus",
        9: "09 - September", 10: "10 - Oktober", 11: "11 - November", 12: "12 - Desember",
    }
    month_options = list(month_names.keys())
    months_in_year = sorted({d.month for d in tanggal_full.dropna() if d.year == year})
    default_month = months_in_year[-1] if months_in_year else 1
    month = st.selectbox(
        "Bulan",
        options=month_options,
        index=month_options.index(default_month),
        format_func=lambda m: month_names[m],
    )

    # === Filter data ke periode (tahun, bulan) terpilih ===
    periode_mask = (tanggal_full.dt.year == year) & (tanggal_full.dt.month == month)
    df_period = df.loc[periode_mask].copy()
    if df_period.empty:
        st.warning(f"Tidak ada data untuk periode **{month_names[month]} {year}**.")
        st.stop()

    # Pastikan kolom Tanggal pada df_period adalah date-only
    df_period["Tanggal"] = pd.to_datetime(df_period[COL_B], errors="coerce").dt.date

    # === Rekonsiliasi kategori utama per Tanggal ===
    with st.spinner("Menghitung rekonsiliasi..."):
        try:
            result = reconcile(
                df=df_period,
                col_h=COL_H,
                col_aa=COL_AA,
                amount_col=COL_K,
                group_cols=["Tanggal"],
            )
        except Exception as e:
            st.error(f"Gagal merekonsiliasi: {e}")
            st.stop()

    # === Tambah kolom BCA / NON BCA / NON (berdasar H + X) setelah 'Total' ===
    try:
        add_cols = compute_bca_non_columns(
            df=df_period,
            group_cols=["Tanggal"],
            type_col=COL_H,
            sof_col=COL_X,
            amount_col=COL_K,
        )
        result = result.join(add_cols, how="left").fillna(0)

        # Susun ulang: letakkan BCA, NON BCA, NON tepat setelah 'Total'
        cols_order = list(result.columns)
        insert_after = "Total"
        for c in ["BCA", "NON BCA", "NON"]:
            if c in cols_order:
                cols_order.remove(c)
        if insert_after in cols_order:
            pos = cols_order.index(insert_after) + 1
            cols_order[pos:pos] = ["BCA", "NON BCA", "NON"]
            result = result[cols_order]
        else:
            result = result[[c for c in result.columns if c not in {"BCA", "NON BCA", "NON"}] + ["BCA", "NON BCA", "NON"]]
    except Exception as e:
        st.error(f"Gagal menambahkan kolom BCA/NON BCA/NON: {e}")
        st.stop()

    # === Tampilkan hasil ===
    st.subheader(f"Hasil Rekonsiliasi • Periode: {month_names[month]} {year}")
    result_display = result.reset_index()
    if "Tanggal" in result_display.columns:
        result_display["Tanggal"] = pd.to_datetime(result_display["Tanggal"]).dt.strftime("%d/%m/%Y")
        result_display = result_display[["Tanggal"] + [c for c in result_display.columns if c != "Tanggal"]]
    st.dataframe(result_display, use_container_width=True)

    # === Unduh Hasil ===
    st.divider()
    st.subheader("Unduh Hasil")

    csv_bytes = result_display.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "Unduh CSV",
        data=csv_bytes,
        file_name=f"rekonsiliasi_payment_{year}_{month:02d}.csv",
        mime="text/csv",
    )

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

    with st.expander("Aturan & Kolom Wajib"):
        st.markdown(
            f"""
**Kolom Wajib (header persis):**
- H → **{COL_H}**
- B → **{COL_B}**
- AA → **{COL_AA}**
- K → **{COL_K}**
- X → **{COL_X}**

**Kategori (kolom-kolom rekonsiliasi utama)**
- Cash, Prepaid BRI/BNI/Mandiri/BCA, SKPT, IFCS, Reedem, ESPAY (finpay+AA diawali esp), Finnet (finpay+AA bukan esp)
- **Total** = penjumlahan semua kolom kategori di atas.

**Tambahan kolom (H + X):**
- **NON**: TIPE PEMBAYARAN ∈ `cash|prepaid*|skpt|ifcs|reedem` (bukan `finpay`)
- **BCA**: TIPE PEMBAYARAN `finpay` & SOF ID berisi `vabcaespay` atau `bluespay`
- **NON BCA**: TIPE PEMBAYARAN `finpay` & SOF ID selain `vabcaespay`/`bluespay`
"""
        )


if __name__ == "__main__":
    main()

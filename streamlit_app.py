# file: app.py
import io
import re
from typing import Dict, Optional, Tuple, List

import pandas as pd
import streamlit as st


# =============== IO helpers ===============
def _ext(name: str) -> str:
    return (name or "").lower().rsplit(".", 1)[-1] if "." in (name or "") else ""

def _need_engine_msg(ext: str) -> str:
    if ext == "xls":
        return "Missing 'xlrd'. Install: pip install xlrd>=2.0.1"
    if ext == "xlsx":
        return "Missing 'openpyxl'. Install: pip install openpyxl"
    if ext == "xlsb":
        return "Missing 'pyxlsb'. Install: pip install pyxlsb"
    return "Engine not found."

def _ensure_engine(ext: str) -> Optional[str]:
    try:
        if ext == "xlsx":
            import openpyxl  # noqa: F401
            return "openpyxl"
        if ext == "xls":
            import xlrd  # noqa: F401
            return "xlrd"
        if ext == "xlsb":
            import pyxlsb  # noqa: F401
            return "pyxlsb"
        return None
    except Exception:
        return None

@st.cache_data(show_spinner=False)
def _excel_sheet_names(uploaded) -> List[str]:
    name = getattr(uploaded, "name", "")
    ext = _ext(name)
    engine = _ensure_engine(ext)
    if ext in {"xls", "xlsx", "xlsb"} and engine is None:
        raise ImportError(_need_engine_msg(ext))
    xls = pd.ExcelFile(uploaded, engine=engine)
    return xls.sheet_names

def _read_df(uploaded, header_row: int, sheet_name: Optional[str]) -> pd.DataFrame:
    header_idx = max(header_row - 1, 0)
    name = getattr(uploaded, "name", "")
    ext = _ext(name)
    if ext in {"xls", "xlsx", "xlsb"}:
        engine = _ensure_engine(ext)
        if engine is None:
            raise ImportError(_need_engine_msg(ext))
        df = pd.read_excel(uploaded, sheet_name=sheet_name, header=header_idx, dtype=str, engine=engine)
    else:
        df = pd.read_csv(uploaded, header=header_idx, dtype=str, encoding="utf-8", engine="python")
    df.columns = [str(c).strip() for c in df.columns]
    return df


# =============== Detect & parse ===============
def _detect_cols_date_remark(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    found = {"date": None, "remark": None}
    for c in df.columns:
        if found["date"] is None and re.search(r"\b(date|tanggal|tgl|posting date|value date)\b", str(c), re.I):
            found["date"] = c
        if found["remark"] is None and re.search(r"remark|ket|descrip|uraian|detail|transaksi", str(c), re.I):
            found["remark"] = c
        if found["date"] and found["remark"]:
            break
    if found["date"] is None:
        found["date"] = df.columns[0]
    if found["remark"] is None:
        found["remark"] = df.columns[1] if len(df.columns) > 1 else df.columns[0]
    return found

def _parse_date(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    d1 = pd.to_datetime(s, errors="coerce", dayfirst=True)
    d2 = pd.to_datetime(s, errors="coerce", dayfirst=False)
    return d1.fillna(d2) if d1.notna().sum() >= d2.notna().sum() else d2.fillna(d1)

# --- Parser angka robust ---
_NBSP = "\u00A0"; _NNBS = "\u202F"; _ZWSP = "\u200B"; _MINUS_U = "\u2212"
def _clean_str(x: str) -> str:
    x = (x or "").replace(_NBSP, " ").replace(_NNBS, " ").replace(_ZWSP, "")
    x = x.replace(_MINUS_U, "-")
    return x.strip()

def _parse_one_number(x: str) -> Optional[float]:
    if x is None: return None
    s = _clean_str(str(x))
    if s == "": return None
    up = s.upper(); neg = False
    if "(" in up and ")" in up: neg = True
    if " DR" in f" {up}" or up.endswith("DR"): neg = True
    if re.match(r"^\s*-\s*", s): neg = True
    s = re.sub(r"(CR|DR|IDR|RP)", "", up)
    s = re.sub(r"[^0-9,\.\-]", "", s)
    if s == "" or s in {"-", ".", ",", "-.", "-,"}: return None
    if "." in s and "," in s:
        last_dot, last_com = s.rfind("."), s.rfind(",")
        normalized = s.replace(".", "") if last_dot < last_com else s.replace(",", "")
        if last_com > last_dot: normalized = normalized.replace(",", ".")
    else:
        if "," in s and "." not in s:
            tail = s.split(",")[-1]
            normalized = s.replace(",", ".") if 1 <= len(tail) <= 2 else s.replace(",", "")
        elif "." in s and "," not in s:
            tail = s.split(".")[-1]
            normalized = s if 1 <= len(tail) <= 2 else s.replace(".", "")
        else:
            normalized = s
    try:
        val = float(normalized)
    except Exception:
        return None
    if neg: val = -abs(val)
    return round(val, 2)

def _to_numeric_amount(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce").round(2)
    return series.apply(_parse_one_number)

# --- Formatter UI: ribuan (.) + tanda +/-, koma desimal, tanpa .00 ---
def _fmt_id_signed(x: float) -> str:
    if pd.isna(x): return ""
    val = float(x)
    sign = "+" if val > 0 else "-" if val < 0 else ""
    v = abs(val)
    s = f"{v:.2f}"
    int_part, frac_part = s.split(".")
    # ribuan pakai titik
    int_part = f"{int(int_part):,}".replace(",", ".")
    # buang .00; kalau ada desimal ≠ 00, pakai koma dan trim nol
    if frac_part == "00":
        out = int_part
    else:
        frac_part = frac_part.rstrip("0")
        out = f"{int_part},{frac_part}"
    return f"{sign}{out}"

def _filter_remark(df: pd.DataFrame, remark_col: str, pattern: str) -> pd.DataFrame:
    rgx = re.compile(pattern, re.I)
    return df[df[remark_col].astype(str).str.contains(rgx, na=False)].copy()


# =============== Build outputs ===============
def _get_credit_col_by_neg10(df: pd.DataFrame) -> str:
    if len(df.columns) < 10:
        raise ValueError(f"Jumlah kolom hanya {len(df.columns)} (< 10). Tidak bisa mengambil kolom ke-10 dari belakang.")
    return df.columns[-10]

def _build_outputs(df: pd.DataFrame, date_col: str, remark_col: str, remark_mode: str):
    credit_col = _get_credit_col_by_neg10(df)
    w = df.copy()
    w["Date"] = _parse_date(w[date_col])
    w["Remark"] = w[remark_col].astype(str)
    w["Amount"] = _to_numeric_amount(w[credit_col])   # dari kolom -10

    # Debug sample
    q = pd.DataFrame({"credit_col_name": [credit_col]*10,
                      "raw_credit": w[credit_col].head(10),
                      "parsed_amount": w["Amount"].head(10)})

    w = w[w["Date"].notna() & w["Amount"].notna()]

    if remark_mode == "FINON saja":
        pattern = r"\bFINON\w*"
    elif remark_mode == "FINIF saja":
        pattern = r"\bFINIF\w*"
    else:
        pattern = r"\bFIN(?:ON|IF)\w*"

    wf = _filter_remark(w, "Remark", pattern)

    rows = wf[["Date", "Remark", "Amount"]].copy()
    grouped = (wf.assign(Date=wf["Date"].dt.date)
                 .groupby("Date", as_index=False)["Amount"]
                 .sum()
                 .rename(columns={"Amount": "TotalAmount"}))
    return rows, grouped, q, credit_col


def _to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO(); df.to_csv(buf, index=False); return buf.getvalue().encode("utf-8")


# =============== App ===============
st.set_page_config(page_title="RK → FINON + FINIF → Amount kolom -10 (format ribuan + tanda)", page_icon="📄", layout="wide")
st.title("📄 Rekening Koran → FINON + FINIF → Amount dari kolom ke-10 (tampilan ribuan + tanda)")

with st.sidebar:
    header_row = st.radio("Mulai baca dari baris (header):", options=[13, 14], index=0, horizontal=True)
    remark_mode = st.radio("Filter Remark:", options=["FINON & FINIF", "FINON saja", "FINIF saja"], index=0)
    show_debug = st.checkbox("Tampilkan debug", value=True)

uploaded = st.file_uploader("Unggah Rekening Koran (CSV/XLSX/XLS/XLSB)", type=["csv","xlsx","xls","xlsb"])

sheet_name = None
if uploaded is not None:
    ext = _ext(uploaded.name)
    if ext in {"xls", "xlsx", "xlsb"}:
        try:
            sheets = _excel_sheet_names(uploaded)
            sheet_name = st.selectbox("Pilih Sheet:", options=sheets, index=0)
        except ImportError as e:
            st.error(f"Gagal membaca daftar sheet: {e}")
            st.info("Alternatif: simpan ulang sebagai .xlsx atau .csv, atau install dependency di bawah.")
            st.code("pip install pandas openpyxl xlrd pyxlsb", language="bash")
            st.stop()
        except Exception as e:
            st.error(f"Gagal membaca daftar sheet: {e}")
            st.stop()

if uploaded is not None:
    try:
        df_raw = _read_df(uploaded, header_row=header_row, sheet_name=sheet_name)
        st.success(f"Data terbaca. Baris: {len(df_raw):,} | Kolom: {len(df_raw.columns)}")

        picks = _detect_cols_date_remark(df_raw)
        c1, c2 = st.columns(2)
        with c1:
            date_col = st.selectbox("Kolom Date/Tanggal:", options=list(df_raw.columns),
                                    index=list(df_raw.columns).index(picks["date"]) if picks["date"] in df_raw.columns else 0)
        with c2:
            remark_col = st.selectbox("Kolom Remark:", options=list(df_raw.columns),
                                      index=list(df_raw.columns).index(picks["remark"]) if picks["remark"] in df_raw.columns else 0)

        rows, grouped, qsample, credit_col = _build_outputs(df_raw, date_col, remark_col, remark_mode)

        if not re.search(r"credit", str(credit_col), re.I):
            st.warning(f"Kolom index -10 bernama '{credit_col}', tidak mengandung kata 'Credit'. Pastikan ini benar.")

        st.subheader("🔎 Baris (dengan format ribuan + tanda)")
        st.dataframe(rows.assign(Amount=rows["Amount"].map(_fmt_id_signed)), use_container_width=True, height=380)

        st.subheader("🗓️ Rekap per Tanggal")
        st.dataframe(grouped.assign(TotalAmount=grouped["TotalAmount"].map(_fmt_id_signed)), use_container_width=True, height=300)

        d1, d2, d3 = st.columns(3)
        with d1:
            st.download_button("💾 Unduh Baris (CSV numeric)", data=_to_csv_bytes(rows),
                               file_name="rows_finon_finif_numeric.csv", mime="text/csv")
        with d2:
            st.download_button("💾 Unduh Rekap (CSV numeric)", data=_to_csv_bytes(grouped),
                               file_name="grouped_finon_finif_numeric.csv", mime="text/csv")
        with d3:
            rows_fmt = rows.assign(Amount=rows["Amount"].map(_fmt_id_signed))
            st.download_button("💾 Unduh Baris (CSV tampilan)", data=_to_csv_bytes(rows_fmt),
                               file_name="rows_finon_finif_view.csv", mime="text/csv")

        if show_debug:
            st.divider()
            st.markdown("**Debug**")
            st.write("Kolom sumber Amount (index -10):", credit_col)
            st.dataframe(qsample, use_container_width=True)

    except ValueError as e:
        st.error(str(e))
        st.write("Daftar kolom:", list(df_raw.columns) if 'df_raw' in locals() else "—")
    except ImportError as e:
        st.error(f"Gagal memproses file: {e}")
        st.info("Install dependency sesuai tipe file:")
        st.code("pip install pandas streamlit openpyxl xlrd pyxlsb", language="bash")
    except Exception as e:
        st.error(f"Gagal memproses file: {e}")
else:
    st.info("Unggah file untuk mulai memproses.")

st.caption("Tampilan angka: ribuan (.) dan tanda +/-. Data internal tetap numeric untuk perhitungan & ekspor.")

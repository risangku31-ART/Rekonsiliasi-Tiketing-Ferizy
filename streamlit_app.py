# rekon_tiket_espay_app.py
# -------------------------------------------------------------
# Rekonsiliasi Tiket Detail (Created -> Action) vs Settlement ESPAY
# - Ticket Detail: terima Excel/CSV/ZIP
# - Settlement: terima CSV/ZIP/Excel
# - Ekstrak 'Created' (fixed-width) -> 'Action' (tanggal)
# - Pilih kolom nominal & tanggal, filter periode, rekonsiliasi per tanggal
#
# Cara jalan:
#   pip install streamlit pandas openpyxl xlsxwriter
#   streamlit run rekon_tiket_espay_app.py

import io
import re
import zipfile
from typing import List, Tuple, Optional

import pandas as pd
import streamlit as st

# -------------------------- Helpers --------------------------

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _find_created_col(df: pd.DataFrame) -> Optional[str]:
    cols = [str(c).strip() for c in df.columns]
    for c in cols:
        if c.lower() == "created":
            return c
    for c in cols:
        if "created" in c.lower():
            return c
    for c in cols:
        if re.search(r"(dibuat|tgl.*buat|tanggal.*buat)", c.lower()):
            return c
    return None

def normalize_created_action(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """Pastikan ada:
       - 'Created' string 'dd/mm/YYYY HH:MM:SS'
       - 'Action'  string 'dd/mm/YYYY' (tanggal saja)
    """
    df = _normalize_columns(df.copy())
    created_col = _find_created_col(df)
    if created_col is None:
        return df, "Kolom 'Created' tidak ditemukan; data dibiarkan apa adanya."

    s = df[created_col]
    if pd.api.types.is_datetime64_any_dtype(s):
        created_dt = pd.to_datetime(s, errors="coerce")
        df["Created"] = created_dt.dt.strftime("%d/%m/%Y %H:%M:%S")
        df["Action"]  = created_dt.dt.strftime("%d/%m/%Y")
    else:
        s = s.astype(str).str.strip()
        s = s.str.replace("T", " ", regex=False) \
             .str.replace(r"([+-]\d{2}:?\d{2}|Z)$", "", regex=True)
        date_part = s.str.slice(0, 10)
        time_part = s.str.slice(11, 19)
        d1 = pd.to_datetime(date_part, errors="coerce", dayfirst=True)
        d2 = pd.to_datetime(date_part, errors="coerce", format="%Y-%m-%d")
        dparsed = d1.fillna(d2)
        df["Created"] = dparsed.dt.strftime("%d/%m/%Y").fillna(date_part) + " " + time_part.fillna("00:00:00")
        df["Action"]  = dparsed.dt.strftime("%d/%m/%Y")

    # bersihkan sisa perantara
    for col in list(df.columns):
        if str(col).lower() in ["created_date_str", "created_time_str"]:
            del df[col]
    for col in list(df.columns):
        if str(col).lower() == "action" and col != "Action":
            del df[col]
    return df, "OK"

def guess_numeric_cols(df: pd.DataFrame) -> List[str]:
    preferred = {"amount","nominal","nilai","total","harga","tarif","jumlah","grand total","bayar","payment","gross","net"}
    cols = []
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            cols.append(str(c))
        else:
            sample = df[c].dropna().astype(str).head(20)
            if not sample.empty and sample.str.replace(r"[0-9\.,\-]", "", regex=True).str.len().max() == 0:
                cols.append(str(c))
    return sorted(cols, key=lambda x: (0 if x.strip().lower() in preferred else 1, x))

def guess_date_cols(df: pd.DataFrame) -> List[str]:
    names = [str(c) for c in df.columns]
    cands, seen = [], set()
    for c in names:
        cl = c.strip().lower()
        if any(k in cl for k in ["settlement","posting","tanggal","date","transaksi","paid","created"]) \
           or pd.api.types.is_datetime64_any_dtype(df[c]):
            if c not in seen:
                cands.append(c); seen.add(c)
    return cands

def parse_currency_series(s: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce")
    x = s.astype(str).str.strip().str.replace(r"\s", "", regex=True)
    def _one(v: str):
        if v in ("", "nan", "None", "NaN"):
            return None
        has_dot, has_com = "." in v, "," in v
        if has_dot and has_com:
            last_sep = max(v.rfind("."), v.rfind(","))
            int_digits = re.sub(r"[^\d]", "", v[:last_sep])
            frac_digits = re.sub(r"[^\d]", "", v[last_sep+1:])
            num = int_digits + ("." + frac_digits if frac_digits else "")
            try: return float(num)
            except: return None
        digits = re.sub(r"[^\d\-]", "", v)
        if digits in ("", "-", "--"): return None
        try: return float(digits)
        except: return None
    return x.map(_one)

# -------------------------- Readers --------------------------

def _read_csv_bytes(b) -> pd.DataFrame:
    try:
        return pd.read_csv(b, dtype=object)
    except Exception:
        if hasattr(b, "seek"): b.seek(0)
        return pd.read_csv(b, dtype=object, sep=";")

def _read_excel_bytes(b) -> List[pd.DataFrame]:
    xl = pd.ExcelFile(b)
    return [pd.read_excel(xl, sheet_name=sh, dtype=object) for sh in xl.sheet_names]

def read_ticket_files(files) -> pd.DataFrame:
    """Terima XLSX/XLS/CSV/ZIP (di dalam ZIP: CSV/XLSX/XLS)."""
    frames = []
    for f in files:
        name = getattr(f, "name", "uploaded")
        content = f.read()
        # ZIP
        if name.lower().endswith(".zip"):
            zbuf = io.BytesIO(content)
            with zipfile.ZipFile(zbuf) as z:
                for zi in z.infolist():
                    zname = zi.filename
                    with z.open(zi) as fh:
                        if zname.lower().endswith((".xlsx",".xls")):
                            # baca excel dari bytes
                            data = fh.read()
                            dfs = _read_excel_bytes(io.BytesIO(data))
                            for df in dfs:
                                df2, _ = normalize_created_action(df)
                                df2["source_file"] = f"{name}:{zname}"
                                df2["sheet"] = "XLSX"
                                frames.append(df2)
                        elif zname.lower().endswith(".csv"):
                            df = _read_csv_bytes(fh)
                            df2, _ = normalize_created_action(df)
                            df2["source_file"] = f"{name}:{zname}"
                            df2["sheet"] = "CSV"
                            frames.append(df2)
                        else:
                            continue
            continue

        # Non-ZIP: coba Excel dulu, lalu CSV
        buf = io.BytesIO(content)
        try:
            dfs = _read_excel_bytes(buf)
            for i, df in enumerate(dfs):
                df2, _ = normalize_created_action(df)
                df2["source_file"] = name
                df2["sheet"] = f"Sheet{i+1}"
                frames.append(df2)
        except Exception:
            buf.seek(0)
            df = _read_csv_bytes(buf)
            df2, _ = normalize_created_action(df)
            df2["source_file"] = name
            df2["sheet"] = "CSV"
            frames.append(df2)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def read_settlement_files(files) -> pd.DataFrame:
    """Terima CSV/ZIP/Excel (ZIP berisi CSV)."""
    frames = []
    for f in files:
        name = getattr(f, "name", "uploaded")
        content = f.read()
        if name.lower().endswith(".zip"):
            zbuf = io.BytesIO(content)
            with zipfile.ZipFile(zbuf) as z:
                for zi in z.infolist():
                    if not zi.filename.lower().endswith(".csv"):
                        continue
                    with z.open(zi) as fh:
                        df = _read_csv_bytes(fh)
                    df["source_file"] = f"{name}:{zi.filename}"
                    df["sheet"] = "CSV"
                    frames.append(_normalize_columns(df))
        else:
            buf = io.BytesIO(content)
            # Excel?
            try:
                dfs = _read_excel_bytes(buf)
                for i, df in enumerate(dfs):
                    df["source_file"] = name
                    df["sheet"] = f"Sheet{i+1}"
                    frames.append(_normalize_columns(df))
            except Exception:
                buf.seek(0)
                df = _read_csv_bytes(buf)
                df["source_file"] = name
                df["sheet"] = "CSV"
                frames.append(_normalize_columns(df))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# -------------------------- Aggregations --------------------------

def aggregate_ticket_by_action(df: pd.DataFrame, amount_col: Optional[str]) -> pd.DataFrame:
    tmp = df.copy()
    tmp["Action_dt"] = pd.to_datetime(tmp["Action"], dayfirst=True, errors="coerce").dt.date
    if amount_col is None:
        g = tmp.groupby("Action_dt", dropna=False).size().reset_index(name="Tiket Detail ESPAY")
    else:
        val = parse_currency_series(tmp[amount_col])
        g = tmp.assign(_val=val).groupby("Action_dt", dropna=False)["_val"].sum(min_count=1).reset_index()
        g = g.rename(columns={"_val": "Tiket Detail ESPAY"})
    g = g.rename(columns={"Action_dt": "Tanggal"})
    return g

def aggregate_settlement_by_date(df: pd.DataFrame, date_col: str, amount_col: str) -> pd.DataFrame:
    tmp = df.copy()
    tmp["Tanggal"] = pd.to_datetime(tmp[date_col], dayfirst=True, errors="coerce").dt.date
    val = parse_currency_series(tmp[amount_col])
    g = tmp.assign(_val=val).groupby("Tanggal", dropna=False)["_val"].sum(min_count=1).reset_index()
    g = g.rename(columns={"_val": "Settlement Dana ESPAY"})
    return g

# -------------------------- Streamlit UI --------------------------

st.set_page_config(page_title="Rekonsiliasi Tiket vs Settlement ESPAY", layout="wide")

st.title("Rekonsiliasi Tiket vs Settlement ESPAY")
st.caption("• Ticket: Excel/CSV/ZIP → 'Created'→'Action' • Settlement: CSV/ZIP/Excel.")

with st.expander("1) Upload Ticket Detail (Excel/CSV/ZIP)"):
    ticket_files = st.file_uploader(
        "Upload banyak file Ticket Detail",
        type=["xlsx","xls","csv","zip"],  # <-- ZIP aktif di sini
        accept_multiple_files=True,
        key="ticket")
    if ticket_files:
        tickets_df = read_ticket_files(ticket_files)
        st.write("Preview kolom utama Ticket Detail:")
        keep = [c for c in tickets_df.columns if str(c).lower() in ["created","action","source_file","sheet"]]
        st.dataframe(tickets_df[keep].head(200), use_container_width=True)
        ticket_num_cols = guess_numeric_cols(tickets_df)
        ticket_amt = st.selectbox("Pilih kolom nominal Ticket (opsional; kosong = hitung jumlah tiket)",
                                  options=["(hitung jumlah tiket)"] + ticket_num_cols, index=0)
        ticket_amt_col = None if ticket_amt == "(hitung jumlah tiket)" else ticket_amt
    else:
        tickets_df = pd.DataFrame()
        ticket_amt_col = None

with st.expander("2) Upload Settlement ESPAY (CSV/ZIP/Excel)"):
    settlement_files = st.file_uploader(
        "Upload banyak file Settlement",
        type=["csv","zip","xlsx","xls"],
        accept_multiple_files=True,
        key="settle")
    if settlement_files:
        settle_df = read_settlement_files(settlement_files)
        st.write("Preview Settlement (kolom tersedia):")
        st.dataframe(settle_df.head(200), use_container_width=True)
        date_candidates = guess_date_cols(settle_df)
        num_candidates = guess_numeric_cols(settle_df)
        settle_date_col = st.selectbox("Pilih kolom TANGGAL untuk Settlement", 
                                       options=date_candidates if date_candidates else settle_df.columns.tolist())
        settle_amt_col  = st.selectbox("Pilih kolom NOMINAL untuk Settlement", 
                                       options=num_candidates if num_candidates else settle_df.columns.tolist())
    else:
        settle_df = pd.DataFrame()
        settle_date_col = None
        settle_amt_col = None

st.subheader("3) Periode Rekonsiliasi")
c1, c2 = st.columns(2)
with c1:
    start_date = st.date_input("Tanggal mulai", value=None)
with c2:
    end_date = st.date_input("Tanggal akhir", value=None)

if st.button("Proses"):
    if tickets_df.empty:
        st.error("Ticket Detail belum diupload.")
        st.stop()
    if settle_df.empty:
        st.error("Settlement ESPAY belum diupload.")
        st.stop()

    tiket_agg = aggregate_ticket_by_action(tickets_df, ticket_amt_col)
    settle_agg = aggregate_settlement_by_date(settle_df, settle_date_col, settle_amt_col)

    # Filter
    if start_date:
        tiket_agg = tiket_agg[tiket_agg["Tanggal"] >= start_date]
        settle_agg = settle_agg[settle_agg["Tanggal"] >= start_date]
    if end_date:
        tiket_agg = tiket_agg[tiket_agg["Tanggal"] <= end_date]
        settle_agg = settle_agg[settle_agg["Tanggal"] <= end_date]

    hasil = pd.merge(tiket_agg, settle_agg, on="Tanggal", how="outer").sort_values("Tanggal").reset_index(drop=True)
    for col in ["Tiket Detail ESPAY","Settlement Dana ESPAY"]:
        if col in hasil:
            hasil[col] = pd.to_numeric(hasil[col], errors="coerce").fillna(0)
    hasil["Selisih"] = hasil["Tiket Detail ESPAY"] - hasil["Settlement Dana ESPAY"]

    st.subheader("Hasil Rekonsiliasi per Tanggal")
    st.dataframe(hasil, use_container_width=True)

    # Unduhan Excel
    with io.BytesIO() as buf:
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            tickets_df.to_excel(writer, sheet_name="All Ticket (raw+action)", index=False)
            settle_df.to_excel(writer, sheet_name="All Settlement (raw)", index=False)
            tiket_agg.to_excel(writer, sheet_name="Agg Ticket by Action", index=False)
            settle_agg.to_excel(writer, sheet_name="Agg Settlement", index=False)
            hasil.to_excel(writer, sheet_name="Rekonsiliasi per Tgl", index=False)
        st.download_button("Download Excel Hasil Rekon",
                           data=buf.getvalue(),
                           file_name="Rekon_Tiket_vs_Settlement.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # Grafik ringkas
    try:
        chart_df = hasil.melt(id_vars="Tanggal",
                              value_vars=["Tiket Detail ESPAY","Settlement Dana ESPAY","Selisih"],
                              var_name="Jenis", value_name="Nilai")
        st.line_chart(chart_df.pivot(index="Tanggal", columns="Jenis", values="Nilai"))
    except Exception:
        pass

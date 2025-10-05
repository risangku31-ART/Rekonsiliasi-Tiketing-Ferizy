# -------- Settlement Dana ESPAY (hanya Transaction Date + Settlement Amount) --------
s_txn_date    = _find_col(settle_df, [
    "Transaction Date", "Trans Date", "Tanggal Transaksi", "Tgl Transaksi", "Tanggal Trans", "Tgl Trans"
])
s_amt         = _find_col(settle_df, [
    "Settlement Ammount", "Settlement Amount", "Amount Settlement",
    "Nominal Settlement", "Amount", "Nominal", "Jumlah", "Total Amount",
    "Net Settlement Amount", "Net Settlement"
])
s_settle_date = _find_col(settle_df, ["Settlement Date", "SettlementDate", "Tanggal Settlement", "Tgl Settlement"])
s_prod        = _find_col(settle_df, ["Product Name", "Product", "ProductName", "Nama Produk"])

miss2 = []
if s_txn_date is None: miss2.append("Settlement: Transaction Date")
if s_amt is None:      miss2.append("Settlement: Settlement Amount/Ammount")
if miss2:
    st.error("Kolom wajib tidak ditemukan → " + "; ".join(miss2))
    st.write("Kolom Settlement tersedia:", list(settle_df.columns))
    st.stop()

# Total → group by Transaction Date (TANPA filter bank)
sd_txn = settle_df.copy()
sd_txn[s_txn_date] = _to_datetime_series(sd_txn[s_txn_date]).dt.normalize()
sd_txn = sd_txn[~sd_txn[s_txn_date].isna()]
sd_txn = sd_txn[(sd_txn[s_txn_date] >= month_start) & (sd_txn[s_txn_date] <= month_end)]
sd_txn[s_amt] = _to_num(sd_txn[s_amt])

settle_total = sd_txn.groupby(sd_txn[s_txn_date])[s_amt].sum()
settle_total.index = pd.to_datetime(settle_total.index).date

# (Opsional) BCA/Non-BCA tetap dihitung seperti sebelumnya bila kolomnya ada.
# Ini TIDAK mempengaruhi "Settlement Dana ESPAY" di atas.
if s_settle_date is not None and s_prod is not None:
    sd_settle = settle_df.copy()
    sd_settle[s_settle_date] = _to_datetime_series(sd_settle[s_settle_date]).dt.normalize()
    sd_settle = sd_settle[~sd_settle[s_settle_date].isna()]
    sd_settle = sd_settle[(sd_settle[s_settle_date] >= month_start) & (sd_settle[s_settle_date] <= month_end)]
    sd_settle[s_amt] = _to_num(sd_settle[s_amt])

    target = _norm_label("BCA VA Online")
    prod_norm = sd_settle[s_prod].apply(_norm_label)
    bca_mask = prod_norm.eq(target)

    settle_bca    = sd_settle[bca_mask].groupby(sd_settle[bca_mask][s_settle_date])[s_amt].sum() if bca_mask.any() else pd.Series(dtype=float)
    settle_nonbca = sd_settle[~bca_mask].groupby(sd_settle[~bca_mask][s_settle_date])[s_amt].sum() if (~bca_mask).any() else pd.Series(dtype=float)
else:
    settle_bca = pd.Series(dtype=float)
    settle_nonbca = pd.Series(dtype=float)

# Streamlit viewer for WalterMart Dasmariñas POC data.
#
# Run:
#   streamlit run viewer_streamlit.py

import os, duckdb, pandas as pd, streamlit as st

st.set_page_config(page_title="WalterMart Dasma — PriceCheck PH POC", layout="wide")

st.title("WalterMart Dasmariñas — PriceCheck PH (POC)")
st.caption("Branch-specific, public-page POC. Rows show source URL and timestamp.")

# Read DB path from DATA_DIR env var (mounted by docker-compose) or use local file
DATA_DIR = os.environ.get("DATA_DIR", ".")
db_path = os.path.join(DATA_DIR, "pricecheck.duckdb")
con = duckdb.connect(db_path)

# Load
try:
    df = con.execute("SELECT * FROM waltermart_dasma").df()
except Exception:
    st.warning("No data yet. Run the scraper first to populate 'pricecheck.duckdb'.")
    st.stop()

# Filters
q = st.text_input("Search name contains")
if q:
    df = df[df["name"].str.contains(q, case=False, na=False)]

# Sorting controls
sort_col = st.selectbox("Sort by", ["price", "unit_price", "name", "collected_at"])
ascending = st.checkbox("Ascending", value=True)

df = df.sort_values(by=sort_col, ascending=ascending)

# Display
st.dataframe(df, use_container_width=True)

# Basic stats
st.subheader("Quick stats")
c1, c2, c3 = st.columns(3)
with c1:
    st.metric("Rows", len(df))
with c2:
    st.metric("Median price", f"₱{df['price'].median():,.2f}" if len(df) else "—")
with c3:
    up = df["unit_price"].dropna()
    st.metric("Median unit price", f"₱{up.median():,.2f}" if len(up) else "—")
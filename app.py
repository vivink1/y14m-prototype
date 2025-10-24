import streamlit as st, pandas as pd, hashlib, json, io, time

# ---------- CONFIG ----------
PRODUCT_CODE   = "CCARD"
REPORTING_DATE = "2025-03-31"
GL_CONTROL     = 20_000_000
REQUIRED       = ["MonthlyIncome", "RevolvingUtil", "DPD30_59"]

# ---------- PIPELINE ----------
def run_pipeline(df):
    df["OutstandingBalance"] = (df["MonthlyIncome"] * df["RevolvingUtil"]).round(2)
    df["RowHash"] = df.apply(lambda r: hashlib.sha256(json.dumps(r.to_dict(), sort_keys=True, default=str).encode()).hexdigest()[:8], axis=1)
    df["ReportingDate"] = REPORTING_DATE
    df["ProductCode"]   = PRODUCT_CODE
    return df

def narrative(df):
    bal = df["OutstandingBalance"].sum()
    util = df["RevolvingUtil"].mean()
    p30  = (df["DPD30_59"] > 0).mean()
    var  = abs(GL_CONTROL - bal)
    return (f"Credit-card outstanding balances totaled ${bal:,.0f}. "
            f"Revolving utilization averaged {util:.1%}. "
            f"Delinquency incidence was {p30:.1%} (30–59 DPD). "
            f"Variance to GL was ${var:,.0f}.")

# ---------- Q&A ----------
try:
    from retriever import ask
    QA_OK = True
except Exception:
    QA_OK = False

# ---------- UI ----------
st.set_page_config(page_title="Y-14M Proto", layout="centered")
st.title("📊 Y-14M Instant Pack (Beta)")
st.markdown("*Internal prototype – dummy data only*")

# ----- sample data -----
sample = """MonthlyIncome,RevolvingUtil,DPD30_59
4500,0.35,0
6200,0.62,5
3800,0.28,0
5500,0.45,0
7200,0.71,15"""  # 5 rows

# ----- file upload -----
uploaded = st.file_uploader("Upload CSV (or leave blank to run sample)", type="csv")
if st.button("🚀 Generate Y-14M Report", type="primary", use_container_width=True):
    with st.spinner("Processing…"):
        time.sleep(1.5)  # fake work
        df = pd.read_csv(uploaded if uploaded else io.StringIO(sample))
        # basic validation
        missing = [c for c in REQUIRED if c not in df.columns]
        if missing:
            st.error(f"Missing columns: {', '.join(missing)}")
            st.stop()
        df = run_pipeline(df)
        txt = narrative(df)

    st.success("✅ Done!")
    col1, col2 = st.columns(2)
    with col1:
        st.download_button("⬇️ CSV", data=df.to_csv(index=False), file_name="Y14M_clean.csv", use_container_width=True)
    with col2:
        st.download_button("⬇️ Narrative", data=txt, file_name="Y14M_narrative.txt", use_container_width=True)
    st.text(txt)

# ----- Q&A -----
if QA_OK:
    q = st.text_input("Ask something (e.g. What is GL control?)")
    if q:
        st.info(ask(q))
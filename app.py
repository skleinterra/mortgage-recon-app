import os, tempfile
import streamlit as st
import pandas as pd
from pipeline_inmemory import run_pipeline_in_memory

st.set_page_config(page_title="Mortgage Statement Consolidation", page_icon="📊", layout="centered")

# --- simple passcode gate (upgrade to SSO later) ---
PASS = os.environ.get("APP_PASSCODE", "")
if PASS:
    with st.sidebar:
        st.header("Access")
        code = st.text_input("Enter passcode", type="password")
    if code != PASS:
        st.info("Enter passcode to use the app.")
        st.stop()

st.title("📊 Mortgage Statement Consolidation")
st.caption("Uploads are processed in memory and discarded after the Excel file is generated.")

# OCR provider selector (env-driven; keep UI simple)
prov = os.environ.get("OCR_PROVIDER","gcv").lower()
st.write(f"**OCR Provider:** `{prov}`")

pdf_files = st.file_uploader("Upload mortgage PDFs (text or scanned)", type=["pdf"], accept_multiple_files=True)
dg_file = st.file_uploader("Upload DataGridExport.csv", type=["csv"])
vendor_file = st.file_uploader("Upload VendorInformationLog.csv (optional)", type=["csv"])
tpl_file = st.file_uploader("Upload Mortgage_Template.xlsx (optional)", type=["xlsx"])

if st.button("Process"):

    if not pdf_files or not dg_file:
        st.error("Please upload at least PDFs and DataGridExport.csv"); st.stop()

    with st.spinner("Processing…"):
        try:
            datagrid_df = pd.read_csv(dg_file)
            vendor_df = pd.read_csv(vendor_file) if vendor_file is not None else None
            template_bytes = tpl_file.read() if tpl_file is not None else None
            pdf_blobs = [(f.name, f.read()) for f in pdf_files]

            # If using Google Vision, we may need to materialize the SA JSON for the client
            if os.environ.get("OCR_PROVIDER","gcv").lower() == "gcv":
                sa_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON","")
                if sa_json:
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
                    tmp.write(sa_json.encode("utf-8")); tmp.flush()
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp.name

            out_bytes = run_pipeline_in_memory(pdf_blobs, datagrid_df, vendor_df, template_bytes)

        except Exception as e:
            st.error(f"Action needed: {e}")
            st.stop()

    st.success("Done. Download your workbook below.")
    st.download_button(
        "Download Mortgage_Consolidated.xlsx",
        data=out_bytes.getvalue(),
        file_name="Mortgage_Consolidated.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

st.markdown("---")
st.caption("Upgrade-ready: passcode via env var, OCR via env-selected provider, no file persistence.")

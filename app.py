import os, tempfile
from pathlib import Path
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

BASE = Path(__file__).parent
DEFAULTS_DIR = BASE / "defaults"

# NOTE: use your uploaded filenames here
DEFAULT_VENDOR_PATH   = DEFAULTS_DIR / "Vendor Information Log v2.csv"   # <— your file name
DEFAULT_TEMPLATE_PATH = DEFAULTS_DIR / "Mortgage_Template.xlsx"          # <— your template

st.title("📊 Mortgage Statement Consolidation")
st.caption("Uploads are processed in memory and discarded after the Excel is generated.")
st.write(f"**OCR Provider:** `{os.environ.get('OCR_PROVIDER','gcv').lower()}`")

# Required each run
pdf_files = st.file_uploader("Upload mortgage PDFs (text or scanned)", type=["pdf"], accept_multiple_files=True)
dg_file   = st.file_uploader("Upload DataGridExport.xlsx", type=["xlsx"])

# Optional overrides (if you want to temporarily use different files than the defaults)
vendor_up = st.file_uploader("Upload VendorInformationLog.csv (optional, overrides default)", type=["csv"])
tpl_up    = st.file_uploader("Upload Mortgage_Template.xlsx (optional, overrides default)", type=["xlsx"])

if st.button("Process"):
    if not pdf_files or not dg_file:
        st.error("Please upload at least PDFs and DataGridExport.xslx")
        st.stop()

    with st.spinner("Processing…"):
        try:
            # Required input
            datagrid_df = pd.read_xlsx(dg_file)

            # Vendor rules: uploaded OR default from repo
            if vendor_up is not None:
                vendor_df = pd.read_csv(vendor_up)
                used_vendor = f"(override: {vendor_up.name})"
            else:
                if not DEFAULT_VENDOR_PATH.exists():
                    st.error("Default vendor log not found in /defaults. Upload a CSV or add it to the repo.")
                    st.stop()
                vendor_df = pd.read_csv(DEFAULT_VENDOR_PATH)
                used_vendor = f"(default: {DEFAULT_VENDOR_PATH.name})"

            # Template: uploaded OR default from repo (bytes)
            if tpl_up is not None:
                template_bytes = tpl_up.read()
                used_tpl = f"(override: {tpl_up.name})"
            else:
                if DEFAULT_TEMPLATE_PATH.exists():
                    template_bytes = DEFAULT_TEMPLATE_PATH.read_bytes()
                    used_tpl = f"(default: {DEFAULT_TEMPLATE_PATH.name})"
                else:
                    template_bytes = None
                    used_tpl = "(auto-create new template)"

            pdf_blobs = [(f.name, f.read()) for f in pdf_files]

            # Google Vision service-account JSON (if used)
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

    st.success(f"Done. Using vendor log {used_vendor} and template {used_tpl}. Download your workbook below.")
    st.download_button(
        "Download Mortgage_Consolidated.xlsx",
        data=out_bytes.getvalue(),
        file_name="Mortgage_Consolidated.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

st.markdown("---")
st.caption("Defaults live in /defaults. Uploads (if provided) override them for that run only.")


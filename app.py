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

# We support either filename to match your repo right now
VENDOR_CANDIDATES = [
    DEFAULTS_DIR / "VendorInformationLog.csv",
    DEFAULTS_DIR / "Vendor Information Log v2.csv",
]
DEFAULT_TEMPLATE_PATH = DEFAULTS_DIR / "Mortgage_Template.xlsx"

st.title("📊 Mortgage Statement Consolidation")
st.caption("Uploads are processed in memory and discarded after the Excel is generated.")
st.write(f"**OCR Provider:** `{os.environ.get('OCR_PROVIDER','gcv').lower()}`")

# Required each run
pdf_files = st.file_uploader("Upload mortgage PDFs (text or scanned)", type=["pdf"], accept_multiple_files=True)
dg_file   = st.file_uploader("Upload DataGridExport.xlsx", type=["xlsx"])

# Optional overrides (to temporarily replace defaults)
vendor_up = st.file_uploader("Upload VendorInformationLog.csv (optional, overrides default)", type=["csv"])
tpl_up    = st.file_uploader("Upload Mortgage_Template.xlsx (optional, overrides default)", type=["xlsx"])

def _load_default_vendor_df():
    for p in VENDOR_CANDIDATES:
        if p.exists():
            return pd.read_csv(p), f"(default: {p.name})"
    raise FileNotFoundError("Default vendor log not found in /defaults (expected one of: "
                            "VendorInformationLog.csv, 'Vendor Information Log v2.csv').")

if st.button("Process"):
    if not pdf_files or not dg_file:
        st.error("Please upload at least PDFs and DataGridExport.xlsx")
        st.stop()

    with st.spinner("Processing…"):
        try:
            # ---- Required input (Excel) ----
            dg_df_raw = pd.read_excel(dg_file, engine="openpyxl")

            # Map your columns -> pipeline expectation
            # Your Excel: Column A='Property' (code), Column B='Description' (name)
            # Pipeline expects: PropertyCode, PropertyName
            cols_lower = {c.lower(): c for c in dg_df_raw.columns}
            if "property" in cols_lower and "description" in cols_lower:
                datagrid_df = dg_df_raw.rename(columns={
                    cols_lower["property"]: "PropertyCode",
                    cols_lower["description"]: "PropertyName"
                })
            else:
                raise ValueError("DataGridExport.xlsx must include columns named 'Property' and 'Description'.")

            # ---- Vendor rules: uploaded OR default from repo ----
            if vendor_up is not None:
                vendor_df = pd.read_csv(vendor_up)
                used_vendor = f"(override: {vendor_up.name})"
            else:
                vendor_df, used_vendor = _load_default_vendor_df()

            # ---- Template: uploaded OR default from repo (bytes) ----
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

            # ---- Google Vision service-account JSON (if used) ----
            if os.environ.get("OCR_PROVIDER","gcv").lower() == "gcv":
                sa_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON","")
                if sa_json:
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
                    tmp.write(sa_json.encode("utf-8")); tmp.flush()
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp.name

            # ---- Run pipeline ----
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
st.caption("Defaults live in /defaults. Uploads (if provided) override them for that run only. DataGrid uses columns: Property (code), Description (name).")

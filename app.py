import os, tempfile
from pathlib import Path
import streamlit as st
import pandas as pd
from pipeline_inmemory import run_pipeline_in_memory

st.set_page_config(page_title="Mortgage Statement Consolidation", page_icon="📊", layout="centered")

# --- passcode gate ---
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

# Support either filename to match your repo
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
dg_file   = st.file_uploader("Upload DataGridExport.xlsx (columns: Property, Description)", type=["xlsx"])

# Optional overrides
vendor_up = st.file_uploader("Upload VendorInformationLog.csv (optional, overrides default)", type=["csv"])
tpl_up    = st.file_uploader("Upload Mortgage_Template.xlsx (optional, overrides default)", type=["xlsx"])

# --- Helpers ---
def _normalize_cols(cols):
    return {c: "".join(str(c).strip().lower().replace("-", " ").replace("_", " ").split()) for c in cols}

def _pick(colmap, *cands):
    """Return the original column name matching any normalized candidate."""
    for orig, norm in colmap.items():
        if norm in cands:
            return orig
    return None

def _load_default_vendor_df():
    for p in VENDOR_CANDIDATES:
        if p.exists():
            return pd.read_csv(p), f"(default: {p.name})"
    raise FileNotFoundError(
        "Default vendor log not found in /defaults (expected one of: "
        "VendorInformationLog.csv, 'Vendor Information Log v2.csv')."
    )

def _normalize_vendor_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Accepts a vendor CSV with flexible headers and returns a DataFrame
    with required columns named exactly: Vendor, Pattern, MappedHeader, DetectPattern (DetectPattern optional).
    """
    if df_raw is None or df_raw.empty:
        raise ValueError("Vendor log is empty.")
    colmap = _normalize_cols(df_raw.columns)

    # Required
    vendor_col = _pick(colmap, "vendor", "servicer", "lender")
    pattern_col = _pick(colmap, "pattern", "field", "label", "line", "item", "keyword", "match", "matchtext", "description")
    mapped_col = _pick(colmap, "mappedheader", "header", "mapto", "mapped", "column", "destination", "templateheader")

    # Optional
    detect_col = _pick(colmap, "detectpattern", "detect", "vendordetect", "identifier", "regex")

    missing = []
    if not vendor_col:  missing.append("Vendor (e.g., Vendor/Servicer/Lender)")
    if not pattern_col: missing.append("Pattern (e.g., Pattern/Field/Label/Line/Item/Keyword/Description)")
    if not mapped_col:  missing.append("MappedHeader (e.g., MappedHeader/Header/MapTo/Column/Destination)")
    if missing:
        raise ValueError(
            "Vendor log is missing required columns:\n - " + "\n - ".join(missing) +
            f"\n\nFound columns: {list(df_raw.columns)}"
        )

    df = df_raw.rename(columns={
        vendor_col:  "Vendor",
        pattern_col: "Pattern",
        mapped_col:  "MappedHeader",
        **({detect_col: "DetectPattern"} if detect_col else {})
    }).copy()

    # Ensure required columns exist
    for req in ["Vendor", "Pattern", "MappedHeader"]:
        if req not in df.columns:
            raise ValueError(f"Vendor log normalization failed: missing '{req}' after rename.")

    # Fill optional DetectPattern if absent
    if "DetectPattern" not in df.columns:
        df["DetectPattern"] = ""

    # Trim whitespace
    for c in ["Vendor", "Pattern", "MappedHeader", "DetectPattern"]:
        df[c] = df[c].astype(str).fillna("").str.strip()

    return df

# --- Main button ---
if st.button("Process"):
    if not pdf_files or not dg_file:
        st.error("Please upload at least PDFs and DataGridExport.xlsx")
        st.stop()

    with st.spinner("Processing…"):
        try:
            # ---- DataGrid (Excel) -> expects columns: Property (code), Description (name)
            dg_df_raw = pd.read_excel(dg_file, engine="openpyxl")
            cols_lower = {c.lower(): c for c in dg_df_raw.columns}
            if "property" in cols_lower and "description" in cols_lower:
                datagrid_df = dg_df_raw.rename(columns={
                    cols_lower["property"]: "PropertyCode",
                    cols_lower["description"]: "PropertyName"
                })
            else:
                raise ValueError("DataGridExport.xlsx must include columns named 'Property' and 'Description'.")

            # ---- Vendor rules: uploaded OR default; normalize columns flexibly
            if vendor_up is not None:
                raw_vendor_df = pd.read_csv(vendor_up)
                vendor_df = _normalize_vendor_df(raw_vendor_df)
                used_vendor = f"(override: {vendor_up.name})"
            else:
                raw_vendor_df, used_vendor = _load_default_vendor_df()
                vendor_df = _normalize_vendor_df(raw_vendor_df)

            # ---- Template: uploaded OR default (bytes)
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

            # ---- Google Vision service-account JSON (if used)
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
st.caption("Defaults live in /defaults. Vendor log columns are auto-detected (Vendor, Pattern, MappedHeader, DetectPattern). DataGrid uses columns: Property (code), Description (name).")

import os, tempfile, re
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
dg_file   = st.file_uploader("Upload DataGridExport.xlsx (Property code + Description name)", type=["xlsx"])

# Optional overrides
vendor_up = st.file_uploader("Upload VendorInformationLog (CSV) (optional, overrides default)", type=["csv"])
tpl_up    = st.file_uploader("Upload Mortgage_Template.xlsx (optional, overrides default)", type=["xlsx"])

# ---- Helpers ----
EXPECTED_HEADERS = [
 "Property","Mortgage 1st","Mortgage 2nd","Interest Mortgage 1st","Interest Mortgage 2nd",
 "Tax Escrow","Escrow-Insurance","Escrow-Interest Reserve","Escrow-Debt Service Reserve",
 "Escrow-Immediate Replacement Reserve","Escrow-Replacement Reserve","Escrow-Renovation Reserve","Other Escrows"
]

def _norm(s):
    return "".join(str(s).strip().lower().replace("-", " ").replace("_", " ").split())

def _normalize_cols(cols):
    return {c: _norm(c) for c in cols}

def _pick(colmap, *cands):
    for orig, normed in colmap.items():
        if normed in cands:
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

# map friendly/wide column names to exact template headers
WIDE_TO_TEMPLATE = {
    "principalbalance": "Mortgage 1st",
    "principalbalance1st": "Mortgage 1st",
    "principalbalancefirst": "Mortgage 1st",
    "principalbalance2nd": "Mortgage 2nd",
    "principalbalancesecond": "Mortgage 2nd",
    "interestmortgage1st": "Interest Mortgage 1st",
    "interestmortgagefirst": "Interest Mortgage 1st",
    "interestmortgage2nd": "Interest Mortgage 2nd",
    "interestmortgagesecond": "Interest Mortgage 2nd",
    "taxescrow": "Tax Escrow",
    "escrowinsurance": "Escrow-Insurance",
    "escrowinterestreserve": "Escrow-Interest Reserve",
    "escrowdebtservicereserve": "Escrow-Debt Service Reserve",
    "escrowimmediatereplacementreserve": "Escrow-Immediate Replacement Reserve",
    "escrowreplacementreserve": "Escrow-Replacement Reserve",
    "escrowrenovationreserve": "Escrow-Renovation Reserve",
    "otherescrows": "Other Escrows",
    # also allow exact header matches
    **{_norm(h): h for h in EXPECTED_HEADERS}
}

SPLIT_RX = re.compile(r"[;\|\n,]")

def _explode_wide_vendor(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a wide vendor table (Vendor + many header columns)
    into long rows: Vendor, Pattern, MappedHeader, DetectPattern(optional).
    Any non-empty cell becomes one or more Pattern rows (split on ; , | newline).
    """
    if df_raw is None or df_raw.empty:
        raise ValueError("Vendor log is empty.")
    colmap = _normalize_cols(df_raw.columns)

    vendor_col = _pick(colmap, "vendor", "servicer", "lender")
    detect_col = _pick(colmap, "detectpattern", "detect", "vendordetect", "identifier", "regex")  # optional

    if not vendor_col:
        raise ValueError("Vendor log is missing a 'Vendor' column (e.g., Vendor/Servicer/Lender).")

    # find all header columns we can map
    header_cols = []
    for orig, normed in colmap.items():
        if orig == vendor_col or (detect_col and orig == detect_col):
            continue
        if normed in WIDE_TO_TEMPLATE:
            header_cols.append((orig, WIDE_TO_TEMPLATE[normed]))

    if not header_cols:
        raise ValueError(
            "No recognizable header columns found in vendor log. "
            f"Expected something like: {', '.join(sorted(set(WIDE_TO_TEMPLATE.values())))}"
        )

    rows = []
    for _, r in df_raw.iterrows():
        vendor = str(r[vendor_col]).strip()
        detect = str(r[detect_col]).strip() if detect_col else ""
        for orig, mapped in header_cols:
            cell = r.get(orig, "")
            if pd.isna(cell):
                continue
            text = str(cell).strip()
            if not text:
                continue
            parts = [p.strip() for p in SPLIT_RX.split(text) if p.strip()]
            for pat in parts:
                rows.append({"Vendor": vendor, "Pattern": pat, "MappedHeader": mapped, "DetectPattern": detect})

    if not rows:
        raise ValueError("Vendor log has no non-empty pattern cells to use.")
    return pd.DataFrame(rows, columns=["Vendor","Pattern","MappedHeader","DetectPattern"])

def _normalize_vendor_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Accept either:
    - Long format: columns ~ Vendor, Pattern, MappedHeader, (DetectPattern optional)
    - Wide format: Vendor + one column per header with pattern lists
    """
    colmap = _normalize_cols(df_raw.columns)

    # detect "long" format quickly
    maybe_vendor = _pick(colmap, "vendor", "servicer", "lender")
    has_pattern = _pick(colmap, "pattern", "field", "label", "line", "item", "keyword", "match", "matchtext", "description")
    has_mapped  = _pick(colmap, "mappedheader", "header", "mapto", "mapped", "column", "destination", "templateheader")

    if maybe_vendor and has_pattern and has_mapped:
        detect_col = _pick(colmap, "detectpattern", "detect", "vendordetect", "identifier", "regex")
        df = df_raw.rename(columns={
            maybe_vendor: "Vendor",
            has_pattern:  "Pattern",
            has_mapped:   "MappedHeader",
            **({detect_col: "DetectPattern"} if detect_col else {})
        }).copy()
        if "DetectPattern" not in df.columns:
            df["DetectPattern"] = ""
        for c in ["Vendor","Pattern","MappedHeader","DetectPattern"]:
            df[c] = df[c].astype(str).fillna("").str.strip()
        return df

    # otherwise treat as wide
    return _explode_wide_vendor(df_raw)

def _normalize_datagrid(dg_df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Robustly map DataGridExport.xlsx columns to:
    - PropertyCode (from Property/Property Code/Prop/PropID/etc.)
    - PropertyName (from Description/PropertyName/Name/etc.)
    """
    # Normalize headers to compare
    normed = {str(c).strip().lower().replace(" ", "").replace("_",""): c for c in dg_df_raw.columns}

    prop_col = None
    desc_col = None
    for key, orig in normed.items():
        if key in ["property", "propertycode", "prop", "propid", "propertyid", "property_code"]:
            prop_col = orig
        if key in ["description", "propertyname", "name", "propname", "property_description", "propertydesc"]:
            desc_col = orig

    if not prop_col or not desc_col:
        raise ValueError(f"DataGridExport.xlsx must include columns for Property (code) and Description (name). Found: {list(dg_df_raw.columns)}")

    return dg_df_raw.rename(columns={prop_col: "PropertyCode", desc_col: "PropertyName"})

# ---- Main button ----
if st.button("Process"):
    if not pdf_files or not dg_file:
        st.error("Please upload at least PDFs and DataGridExport.xlsx")
        st.stop()

    with st.spinner("Processing…"):
        try:
            # ---- DataGrid (Excel) -> robust column mapping
            dg_df_raw = pd.read_excel(dg_file, engine="openpyxl")
            datagrid_df = _normalize_datagrid(dg_df_raw)

            # ---- Vendor rules: uploaded OR default; accept wide or long formats
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
st.caption("Vendor log: accepts LONG (Vendor, Pattern, MappedHeader[, DetectPattern]) or WIDE (one column per target header; patterns split by ; , | newline). DataGrid: Property(code), Description(name) with robust header matching.")

from fastapi import FastAPI, UploadFile, File, BackgroundTasks, Form
from fastapi.responses import FileResponse, JSONResponse
import pandas as pd
import numpy as np
import tempfile
import os
import logging
from datetime import datetime
from typing import Optional
import requests  # <-- needed

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("enrolment-report")

# ---------------------------
# Constants / Mappings
# ---------------------------
PRE_SESSIONAL_PROGRAM_CODES = [4287, 4291, 8383, 8384, 8454, 8802, 8809, 8810, 8811, 8332]
SUMMER_SCHOOL_PROGRAM_CODES = [9541, 9544, 9546, 9547]

# Final column rename mapping (original -> final)
# column_mapping = {
#     "Agent Code": "AGENT_CODE",
#     "Agent Source": "AGENT_SOURCE",
#     "Agent Name": "AGENT_NAME",
#     "Student ID": "APPLICANT_NO",

#     "FORENAME": "FORENAME",
#     "MIDDLE_NAMES": "MIDDLE_NAMES",
#     "SURNAME": "SURNAME",
#     "PATHWAY_1": "PATHWAY_1",
#     "PATHWAY_2": "PATHWAY_2",
#     "SCHOOL_NAME": "SCHOOL_NAME",
#     "ENQUIRY_DETAIL": "ENQUIRY_DETAIL",

#     "ENTRY TERM": "ENTRY_TERM",
#     "DOMICILE DESC": "COUNTRY_OF_DOMICILE",
#     "Residence_Description": "RESIDENCY_DESCRIPTION",
#     "LEVL_CODE": "LEVEL",
#     "Faculty": "FACULTY",
#     "PROGRAM": "PROGRAMME_NAME",
#     "PROGRAM DESCRIPTION": "PROGRAMME_DESCRIPTION",
#     "OnCampus": "MODE",
#     "Latest Decision": "DECISION",
#     "Decision_Description": "DECISION_DESCRIPTION",
#     "APPLICATION DATE": "APPLICATION_DATE",
#     "Application_Year": "APPLICATION_YEAR",
#     "PresessionalCourse": "PRES_SESSIONAL_COURSE",
#     "Summer_School": "SUMMER_SCHOOL",
#     "Pathway": "PATHWAY",
#     "Agent_Code_Post_App": "AGENT_CODE_POST_APP",
#     "Post_App_Agent": "POST_APP_AGENT",
#     "Tuition_Fees": "TUITION_FEE",
#     "Scholarship_Discount": "SCHOLARSHIP",
#     "Commissionable_Amount": "COMMISSIONABLE_AMOUNT",
#     "Presessional_Fee": "PRES_SESSIONAL_FEE",
#     "DECISION DATE": "DECISION_DATE",
#     "Last Institution Code": "LAST_INSTITUTION_CODE",
#     "ESTS CODE": "ESTS_CODE",
#     "ESTS DESC": "ESTS_DESC",
# }

column_mapping = {
    "Agent Code": "AGENT_CODE",
    "Agent Source": "AGENT_SOURCE",
    "Agent Name": "AGENT_NAME",
    "Student ID": "APPLICANT_NO",

    "FORENAME": "FORENAME",
    "MIDDLE_NAMES": "MIDDLE_NAMES",
    "SURNAME": "SURNAME",
    "PATHWAY_1": "PATHWAY_1",
    "PATHWAY_2": "PATHWAY_2",
    "SCHOOL_NAME": "SCHOOL_NAME",
    "ENQUIRY_DETAIL": "ENQUIRY_DETAIL",

    "ENTRY TERM": "ENTRY_TERM",
    "DOMICILE DESC": "COUNTRY_OF_DOMICILE",
    "Residence_Description": "RESIDENCY_DESCRIPTION",
    "LEVL_CODE": "LEVEL",
    "Faculty": "FACULTY",
    "PROGRAM": "PROGRAMME_NAME",
    "PROGRAM DESCRIPTION": "PROGRAMME_DESCRIPTION",
    "OnCampus": "MODE",
    "Latest Decision": "DECISION",
    "Decision_Description": "DECISION_DESCRIPTION",
    "APPLICATION DATE": "APPLICATION_DATE",
    "Application_Year": "APPLICATION_YEAR",
    "PresessionalCourse": "PRES_SESSIONAL_COURSE",
    "Summer_School": "SUMMER_SCHOOL",
    "Pathway": "PATHWAY",
    "Agent_Code_Post_App": "AGENT_CODE_POST_APP",
    "Post_App_Agent": "POST_APP_AGENT",
    "Tuition_Fees": "TUITION_FEE",
    "Scholarship_Discount": "SCHOLARSHIP",
    "Commissionable_Amount": "COMMISSIONABLE_AMOUNT",
    "Presessional_Fee": "PRES_SESSIONAL_FEE",
    "DECISION DATE": "DECISION_DATE",
    "Last Institution Code": "LAST_INSTITUTION_CODE",
    "ESTS CODE": "ESTS_CODE",
    "ESTS DESC": "ESTS_DESC",
}

# ---------------------------
# Utils
# ---------------------------
def reduce_memory_usage(df: pd.DataFrame) -> pd.DataFrame:
    start_mem = df.memory_usage(deep=True).sum() / 1024**2
    for col in df.columns:
        col_type = df[col].dtype
        if pd.api.types.is_integer_dtype(col_type):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")
        elif pd.api.types.is_float_dtype(col_type):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
        elif pd.api.types.is_object_dtype(col_type):
            nunique = df[col].nunique(dropna=True)
            if nunique > 0 and nunique / max(len(df), 1) < 0.5:
                df[col] = df[col].astype("category")
    end_mem = df.memory_usage(deep=True).sum() / 1024**2
    logger.info(f"Memory reduced by {start_mem - end_mem:.2f} MB ({(1 - end_mem / start_mem):.1%})")
    return df

def extract_academic_year(dt_series: pd.Series) -> pd.Series:
    s = pd.to_datetime(dt_series, errors="coerce")
    year = s.dt.year
    month = s.dt.month
    start = np.where(month >= 8, year, year - 1)
    end = start + 1
    return pd.Series([f"{int(a)}-{str(int(b))[-2:]}" if not pd.isna(a) else np.nan for a, b in zip(start, end)], index=dt_series.index)

def load_large_excel(file_path: str, usecols: list, dtype_map: dict | None = None) -> pd.DataFrame:
    logger.info(f"Loading {os.path.basename(file_path)}")
    head = pd.read_excel(file_path, engine="openpyxl", nrows=5)
    available = head.columns.tolist()
    kept_cols = [c for c in usecols if c in available]
    df = pd.read_excel(file_path, engine="openpyxl", usecols=kept_cols)
    if dtype_map:
        for col, dt in dtype_map.items():
            if col in df.columns:
                try:
                    if dt == "Int32":
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")
                    elif dt == "int32":
                        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int32")
                    elif dt == "float32":
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
                    elif dt == "category":
                        df[col] = df[col].astype("category")
                    else:
                        df[col] = df[col].astype(dt)
                except Exception:
                    if "int" in str(dt).lower():
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")
                    elif "float" in str(dt).lower():
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
    missing = set(usecols) - set(df.columns)
    for c in missing:
        df[c] = pd.Series(["--"] * len(df))
    return reduce_memory_usage(df)

def clean_final_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize the merged enrolment DataFrame:
      - Ensure stable copies to avoid SettingWithCopy warnings
      - Create/normalize Student ID
      - Unify Agent Code / Name / Source
      - Add placeholder text columns if missing
      - Map COUNTRY_OF_DOMICILE and LEVEL (PC->PGT, PR->PGR) safely
      - Drop helper columns
      - Reorder important columns first
    """
    df = df.copy()
    if 'logger' in globals():
        logger.info("Cleaning final report…")

    # --- Remove any pre-existing duplicates of key columns to rebuild them cleanly
    df.drop(["Agent Code", "Agent Source", "Agent Name", "Student ID"], axis=1, errors="ignore", inplace=True)

    # --- Student ID (from ID if needed), keep nullable integer for robustness
    if "ID" in df.columns:
        df.rename(columns={"ID": "Student ID"}, inplace=True)
    base_sid = pd.Series([np.nan] * len(df), index=df.index)
    df.loc[:, "Student ID"] = pd.to_numeric(df.get("Student ID", base_sid), errors="coerce").astype("Int32")

    # --- Agent Code (prefer Banner code, else Dynamics assisting application code)
    acb = df.get("Agency Code (Banner)", pd.Series([np.nan] * len(df), index=df.index))
    aca = df.get("Agent_Code_Agency_Assisting_Application", pd.Series([""] * len(df), index=df.index))

    df.loc[:, "Agent Code"] = np.where(
        acb.notna() & (acb.astype(str).str.len() > 0),
        acb,
        np.where(
            aca.astype(str).str.len() > 0,
            aca,
            np.nan
        )
    )

    # --- Agent Source (use as-is if present, else NaN)
    if "Agent Source" in df.columns:
        df.loc[:, "Agent Source"] = df["Agent Source"]
    else:
        df.loc[:, "Agent Source"] = np.nan

    # --- Agent Name (prefer Banner agency name, else Dynamics)
    anb = df.get("Agency Name (Banner)", pd.Series([np.nan] * len(df), index=df.index))
    ana = df.get("Agency_Assisting_Application", pd.Series([np.nan] * len(df), index=df.index))
    df.loc[:, "Agent Name"] = np.where(
        anb.notna() & (anb.astype(str).str.len() > 0),
        anb,
        np.where(
            ana.notna() & (ana.astype(str).str.len() > 0),
            ana,
            ""
        )
    )

    # --- Cast agent fields to category to save memory
    for c in ["Agent Code", "Agent Source", "Agent Name"]:
        df.loc[:, c] = df[c].astype("category")

    # --- Put key identity columns first
    front = ["Agent Code", "Agent Source", "Agent Name", "Student ID"]
    rest = [c for c in df.columns if c not in front]
    df = df.loc[:, front + rest]

    # --- Ensure placeholder text columns exist
    for col in ["FORENAME", "MIDDLE_NAMES", "SURNAME", "PATHWAY_1", "PATHWAY_2", "SCHOOL_NAME", "ENQUIRY_DETAIL"]:
        if col not in df.columns:
            df.loc[:, col] = "--"

    # --- COUNTRY_OF_DOMICILE fallback from DOMICILE DESC
    if "COUNTRY_OF_DOMICILE" not in df.columns and "DOMICILE DESC" in df.columns:
        df.loc[:, "COUNTRY_OF_DOMICILE"] = df["DOMICILE DESC"]

    # --- LEVEL fallback from LEVL_CODE, then normalize values
    if "LEVEL" not in df.columns and "LEVL_CODE" in df.columns:
        df.loc[:, "LEVEL"] = df["LEVL_CODE"]

    if "LEVEL" in df.columns:
        # Avoid FutureWarning on categoricals by operating on object dtype
        df.loc[:, "LEVEL"] = df["LEVEL"].astype(object).replace({"PC": "PGT", "PR": "PGR"})
        # Optional: cast back to category for memory optimization
        df.loc[:, "LEVEL"] = df["LEVEL"].astype("category")

    # --- Drop helper columns that shouldn't go to output
    df.drop(
        ["Agency Code (Banner)", "Agency Name (Banner)",
         "Agent_Code_Agency_Assisting_Application", "Agency_Assisting_Application"],
        axis=1, errors="ignore", inplace=True
    )

    if 'logger' in globals():
        logger.info("Final report cleaned.")
    return df

def process_banner(banner_path: str) -> pd.DataFrame:
    logger.info("Processing Banner…")
    usecols = [
        "Agency Code (Banner)", "Agency Name (Banner)", "ID",
        "APPLICATION DATE", "ENTRY TERM", "DOMICILE DESC",
        "Residence_Description", "LEVL_CODE", "Faculty", "PROGRAM",
        "PROGRAM DESCRIPTION", "OnCampus", "Latest Decision",
        "Decision_Description", "DECISION DATE", "ESTS CODE", "ESTS DESC",
        "Last Institution Code"
    ]
    dtype_map = {
        "ID": "Int32",
        "ENTRY TERM": "category",
        "DOMICILE DESC": "category",
        "Residence_Description": "category",
        "LEVL_CODE": "category",
        "Faculty": "category",
        "PROGRAM": "Int32",
        "PROGRAM DESCRIPTION": "category",
        "OnCampus": "category",
        "Latest Decision": "category",
        "Decision_Description": "category",
        "ESTS CODE": "category",
        "ESTS DESC": "category",
        "Last Institution Code": "category",
        "Agency Code (Banner)": "category",
        "Agency Name (Banner)": "category",
    }
    banner_df = load_large_excel(banner_path, usecols, dtype_map)
    for dcol in ["APPLICATION DATE", "DECISION DATE"]:
        if dcol in banner_df.columns:
            banner_df[dcol] = pd.to_datetime(banner_df[dcol], errors="coerce")
    if "APPLICATION DATE" in banner_df.columns:
        banner_df["Application_Year"] = extract_academic_year(banner_df["APPLICATION DATE"])
    else:
        banner_df["Application_Year"] = np.nan
    if "PROGRAM" in banner_df.columns:
        presessional_mask = banner_df["PROGRAM"].isin(PRE_SESSIONAL_PROGRAM_CODES)
        summer_mask = banner_df["PROGRAM"].isin(SUMMER_SCHOOL_PROGRAM_CODES)
        banner_df["PresessionalCourse"] = np.where(presessional_mask, "Y", "N")
        banner_df["Summer_School"] = np.where(summer_mask, "Y", "N")
    else:
        banner_df["PresessionalCourse"] = "--"
        banner_df["Summer_School"] = "--"
    if "OnCampus" in banner_df.columns:
        banner_df["Pathway"] = np.where(banner_df["OnCampus"].astype(str) == "Y", "CEG", "")
    else:
        banner_df["Pathway"] = ""
    logger.info(f"Banner records loaded: {len(banner_df)}")
    return banner_df.reset_index(drop=True)

def process_dynamics(dynamics_path: str) -> pd.DataFrame:
    logger.info("Processing Dynamics…")
    usecols = [
        "Banner ID", "Agent_Code_Agency_Assisting_Application",
        "Agency_Assisting_Application", "Agent_Code_Post_App", "Post_App_Agent"
    ]
    dtype_map = {
        "Banner ID": "Int32",
        "Agent_Code_Agency_Assisting_Application": "category",
        "Agency_Assisting_Application": "category",
        "Agent_Code_Post_App": "category",
        "Post_App_Agent": "category",
    }
    dynamics_df = load_large_excel(dynamics_path, usecols, dtype_map)
    if "Banner ID" in dynamics_df.columns:
        dynamics_df = dynamics_df.drop_duplicates(subset=["Banner ID"])
    logger.info(f"Dynamics records after dedupe: {len(dynamics_df)}")
    return dynamics_df.reset_index(drop=True)

def calculate_fee_metrics(group: pd.DataFrame) -> pd.Series:
    tuition_mask = (
        (group["Fee Type(T)"] == "Tuition Fees") &
        (group["Sponsor Code"] == "SELF") &
        (group["Sponsor Code(T)"] == "Self")
    )
    tuition = group.loc[tuition_mask, "Original Transaction Value"].max()

    scholarship_mask = (
        (group["Fee Type(T)"] == "Tuition Fees") &
        (group["Sponsor Code"] == "DET") &
        (group["Sponsor Code(T)"] == "Tuition Fee Reduction")
    )
    scholarship = group.loc[scholarship_mask, "Original Transaction Value"].max()

    scholarship_abs = 0.0 if pd.isna(scholarship) else abs(float(scholarship))
    tuition_val = float(tuition) if pd.notna(tuition) else 0.0
    commissionable = max(tuition_val - scholarship_abs, 0.0)

    pres_mask = (
        group["Programme"].isin(PRE_SESSIONAL_PROGRAM_CODES) &
        (group["Fee Type(T)"] == "Pre-Sessional Fee Deposit") &
        (group["Sponsor Code"] == "SELF")
    )
    pres_fee = group.loc[pres_mask, "Original Transaction Value"].max()

    return pd.Series({
        "Tuition_Fees": tuition_val if tuition_val != 0 else np.nan,
        "Scholarship_Discount": scholarship_abs if scholarship_abs != 0 else np.nan,
        "Commissionable_Amount": commissionable if commissionable != 0 else np.nan,
        "Presessional_Fee": float(pres_fee) if pd.notna(pres_fee) else np.nan
    })

def process_fee04(fee04_path: str) -> pd.DataFrame:
    logger.info("Processing Fee04…")
    usecols = [
        "Student ID", "Transaction Type", "Sponsor Code",
        "Enrolment Status", "Original Transaction Value",
        "Fee Type(T)", "Sponsor Code(T)", "Programme", "Study Level(T)"
    ]
    dtype_map = {
        "Student ID": "Int32",
        "Transaction Type": "category",
        "Sponsor Code": "category",
        "Enrolment Status": "category",
        "Original Transaction Value": "float32",
        "Fee Type(T)": "category",
        "Sponsor Code(T)": "category",
        "Programme": "Int32",
        "Study Level(T)": "category",
    }
    fee = load_large_excel(fee04_path, usecols, dtype_map)
    if "Enrolment Status" in fee.columns:
        fee = fee[fee["Enrolment Status"] == "EN"]
    logger.info(f"Filtered Fee04 rows: {len(fee)}")
    if fee.empty or "Student ID" not in fee.columns:
        logger.warning("No Fee04 records after filtering or missing Student ID.")
        return pd.DataFrame(columns=["Student ID", "Tuition_Fees", "Scholarship_Discount", "Commissionable_Amount", "Presessional_Fee"])
    grouped = fee.groupby("Student ID", group_keys=False).apply(calculate_fee_metrics).reset_index()
    logger.info(f"Processed fee metrics for {len(grouped)} students")
    return grouped

def merge_datasets(banner: pd.DataFrame, dynamics: pd.DataFrame, fee04: pd.DataFrame) -> pd.DataFrame:
    logger.info("Merging datasets…")
    merged = pd.merge(banner, dynamics, left_on="ID", right_on="Banner ID", how="left")
    final = pd.merge(merged, fee04, left_on="ID", right_on="Student ID", how="left")
    final.drop(["Banner ID", "Student ID"], axis=1, errors="ignore", inplace=True)
    for col in ["Tuition_Fees", "Scholarship_Discount", "Commissionable_Amount", "Presessional_Fee"]:
        if col in final.columns:
            final[col] = final[col].fillna(0)
    logger.info(f"Final merged records: {len(final)}")
    return final


def apply_column_mapping_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns per column_mapping while avoiding duplicate-name collisions.
    If a destination name already exists, we keep the destination and drop the source.
    """
    # normalize headers first
    df.columns = df.columns.astype(str).str.strip()

    safe_map = {}
    for src, dst in column_mapping.items():
        if src not in df.columns:
            continue
        if src == dst:
            continue
        if dst in df.columns:
            # If both exist, prefer the destination and drop the source to avoid duplicates
            try:
                # if they hold identical values, dropping src is definitely safe
                if df[src].equals(df[dst]):
                    df.drop(columns=[src], inplace=True)
                    continue
            except Exception:
                # even if not comparable, still prefer existing dst to avoid duplicate header
                df.drop(columns=[src], inplace=True)
                continue
            # default: prefer existing dst
            df.drop(columns=[src], inplace=True)
            continue
        # No collision -> we can rename
        safe_map[src] = dst

    if safe_map:
        df.rename(columns=safe_map, inplace=True)
    return df

# ---------------------------
# FastAPI App
# ---------------------------
app = FastAPI(title="Enrolment Report API")

def _do_generate_and_callback(
    banner_path: str,
    dynamics_path: str,
    fee04_path: str,
    callback_url: str,
    callback_token: Optional[str],
    passthrough: dict,
):
    try:
        banner = process_banner(banner_path)
        dynamics = process_dynamics(dynamics_path)
        fee04   = process_fee04(fee04_path)

        final_report = merge_datasets(banner, dynamics, fee04)
        final_report = clean_final_report(final_report)
        final_report = apply_column_mapping_safe(final_report)  # rename headings before export

        out = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        out_path = out.name
        out.close()
        final_report.to_excel(out_path, index=False)

        # headers + files
        headers = {}
        if callback_token:
            headers["X-Callback-Token"] = callback_token

        # Use context manager so the file handle is closed even if requests errors
        with open(out_path, "rb") as fh:
            files = {
                "file": (
                    "final_enrolment_report.xlsx",
                    fh,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            }

            # Short timeouts: (connect, read). Callback should return 202 quickly.
            resp = requests.post(
                callback_url,
                files=files,
                data=passthrough,
                headers=headers,
                timeout=(5, 20),
            )
            logger.info("Callback POST -> %s %s", resp.status_code, resp.text[:300])

    except requests.exceptions.ReadTimeout:
        # If your callback enqueues work and returns immediately, you won't see this.
        logger.warning("Callback timed out waiting for response; continuing.")
    except Exception:
        logger.exception("Background processing failed")
    finally:
        # cleanup temp files
        for p in (banner_path, dynamics_path, fee04_path, locals().get("out_path")):
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

@app.get("/")
async def root():
    return {"message": "Enrolment Report API is running."}

@app.post("/generate-report-async")
async def generate_report_async(
    background_tasks: BackgroundTasks,
    banner_file: UploadFile = File(...),
    dynamics_file: UploadFile = File(...),
    fee04_file: UploadFile = File(...),
    callback_url: str = Form(...),
    callback_token: Optional[str] = Form(None),
    intake_id: Optional[str] = Form(None),
    uni_id: Optional[str] = Form(None),
    bi_log_hint: Optional[str] = Form(None),
    requested_by: Optional[str] = Form(None),
):
    def save_tmp(uf: UploadFile) -> str:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        tmp.write(uf.file.read())
        tmp.flush(); tmp.close()
        return tmp.name

    b_path = save_tmp(banner_file)
    d_path = save_tmp(dynamics_file)
    f_path = save_tmp(fee04_file)

    passthrough = {
        "intake_id": intake_id or "",
        "uni_id": uni_id or "",
        "bi_log_hint": bi_log_hint or "",
        "bi_log_hint": bi_log_hint or "",
        "requested_by": requested_by or "",
    }

    background_tasks.add_task(
        _do_generate_and_callback,
        b_path, d_path, f_path, callback_url, callback_token, passthrough
    )
    return JSONResponse(status_code=202, content={"status": "accepted"})

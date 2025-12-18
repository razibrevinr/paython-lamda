from fastapi import FastAPI, UploadFile, File, BackgroundTasks, Form
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
import tempfile
import os
import logging
from typing import Optional
import requests
import re
from pandas.api.types import (
    is_categorical_dtype,
    is_integer_dtype,
    is_float_dtype,
    is_object_dtype,
    is_string_dtype,  # 👈 add this
)

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
# i need more a PRE_SESSIONAL_PROGRAM_CODES
SUMMER_SCHOOL_PROGRAM_CODES = [9541, 9544, 9546, 9547]

# Old -> Final headers
column_mapping = {
    "AGENT_CODE": "AGENT_CODE",
    "AGENT_SOURCE": "AGENT_SOURCE",
    "AGENT_NAME": "AGENT_NAME",
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
    "RESD_DESC": "RESIDENCY_DESCRIPTION",
    "LEVL_CODE": "LEVEL",
    "FACULTY NAME": "FACULTY",
    "PROGRAM": "PROGRAMME_CODE",
    "PROGRAM DESCRIPTION": "PROGRAMME_NAME",
    "OnCampus": "MODE",
    "LATEST DECISION": "DECISION",
    "APDC DESC2": "DECISION_DESCRIPTION",
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
    "UCAS_ID": "UCAS_ID"
}

# Final columns that must never end as blank placeholders unintentionally
NEVER_PLACEHOLDER_COLS = ["AGENT_CODE", "AGENT_SOURCE", "AGENT_NAME", "RESIDENCY_DESCRIPTION"]

# ---------------------------
# Helpers: robust string casting
# ---------------------------
def as_string(s: pd.Series) -> pd.Series:
    """
    Cast a Series to a string-like dtype. Uses pandas 'string' if available,
    otherwise falls back to Python str (object dtype). Never raises TypeError.
    """
    try:
        return s.astype("string")
    except (TypeError, ValueError):
        return s.astype(str)

def to_string_series(values, index=None) -> pd.Series:
    ser = pd.Series(values, index=index)
    return as_string(ser)

# ---------------------------
# Utils
# ---------------------------
def reduce_memory_usage(df: pd.DataFrame) -> pd.DataFrame:
    start_mem = df.memory_usage(deep=True).sum() / 1024**2
    for col in df.columns:
        col_type = df[col].dtype
        try:
            if is_integer_dtype(col_type):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")
            elif is_float_dtype(col_type):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
            elif is_object_dtype(col_type):
                # Defer category; do not auto-shrink to category yet
                pass
        except Exception:
            pass
    end_mem = df.memory_usage(deep=True).sum() / 1024**2
    if start_mem > 0:
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
    logger.info(f"Columns in file: {head.columns.tolist()}")

    available = head.columns.tolist()
    kept_cols = [c for c in usecols if c in available]

    logger.info(f"Columns being used for loading: {kept_cols}")
    df = pd.read_excel(file_path, engine="openpyxl", usecols=kept_cols)

    logger.info(f"First few rows of the loaded file: \n{df.head()}")

    if dtype_map:
        for col, dt in dtype_map.items():
            if col not in df.columns:
                continue
            try:
                if dt == "Int32":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")
                elif dt == "int32":
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int32")
                elif dt == "float32":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
                elif dt in ("string", "str", "object"):
                    df[col] = as_string(df[col])
                elif dt == "category":
                    df[col] = as_string(df[col])
                else:
                    df[col] = df[col].astype(dt)
            except Exception as e:
                logger.error(f"Error converting {col} to {dt}: {e}")
                if "int" in str(dt).lower():
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")
                elif "float" in str(dt).lower():
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
                else:
                    df[col] = as_string(df[col])

    # Ensure requested columns exist, but keep them EMPTY (no "--")
    missing = set(usecols) - set(df.columns)
    for c in missing:
        df[c] = pd.Series([""] * len(df))

    logger.info(f"Data after all operations: \n{df.head()}")
    return reduce_memory_usage(df)


def remove_placeholder_dashes(df, placeholder="--") -> pd.DataFrame:
    df = df.copy()
    for c in df.columns:
        s = df[c]
        if is_object_dtype(s) or is_string_dtype(s) or is_categorical_dtype(s):
            s = as_string(s)
            s = s.mask(s.str.strip() == placeholder, "")  # turn "--" into empty
            df[c] = s
    return df

# ---------------------------
# Cleaning / Processing
# ---------------------------


def clean_final_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build clean enrolment fields on string dtype (no early categoricals).
    """
    df = df.copy()
    logger.info("Cleaning final report…")

    # ---- Student ID (nullable Int32)
    if "ID" in df.columns and "Student ID" not in df.columns:
        df.rename(columns={"ID": "Student ID"}, inplace=True)

    base_sid = pd.Series([np.nan] * len(df), index=df.index)
    df.loc[:, "Student ID"] = pd.to_numeric(
        df.get("Student ID", base_sid), errors="coerce"
    ).astype("Int32")

    # ---- AGENT_CODE
    ac_banner = as_string(
        df.get("AGENCY CODE", pd.Series([pd.NA] * len(df), index=df.index))
    )
    ac_dyn = as_string(
        df.get(
            "Agent_Code_Agency_Assisting_Application",
            pd.Series([""] * len(df), index=df.index),
        )
    )

    agent_code = np.where(
        ac_banner.str.len().fillna(0) > 0,
        ac_banner,
        np.where(ac_dyn.str.len().fillna(0) > 0, ac_dyn, pd.NA),
    )
    df.loc[:, "AGENT_CODE"] = to_string_series(agent_code, index=df.index)

    # ---- AGENT_SOURCE
    agent_source = as_string(
        df.get("Agent Source", pd.Series([pd.NA] * len(df), index=df.index))
    )
    df.loc[:, "AGENT_SOURCE"] = agent_source

    # ---- AGENT_NAME
    an_banner = as_string(
        df.get("AGENCY NAME", pd.Series([pd.NA] * len(df), index=df.index))
    )
    an_dyn = as_string(
        df.get(
            "Agency_Assisting_Application",
            pd.Series([pd.NA] * len(df), index=df.index),
        )
    )

    agent_name = np.where(
        an_banner.str.len().fillna(0) > 0,
        an_banner,
        np.where(an_dyn.str.len().fillna(0) > 0, an_dyn, ""),
    )
    df.loc[:, "AGENT_NAME"] = to_string_series(agent_name, index=df.index)

    # ---- Ensure existence of common text columns
    for col in [
        "FORENAME",
        "MIDDLE_NAMES",
        "SURNAME",
        "PATHWAY_1",
        "PATHWAY_2",
        "SCHOOL_NAME",
        "ENQUIRY_DETAIL",
    ]:
        if col not in df.columns:
            df.loc[:, col] = ""

    # ---- COUNTRY_OF_DOMICILE
    if "COUNTRY_OF_DOMICILE" not in df.columns and "DOMICILE DESC" in df.columns:
        df.loc[:, "COUNTRY_OF_DOMICILE"] = as_string(df["DOMICILE DESC"])

    # ---- RESIDENCY_DESCRIPTION
    if "RESIDENCY_DESCRIPTION" not in df.columns:
        if "RESD_DESC" in df.columns:
            df.loc[:, "RESIDENCY_DESCRIPTION"] = as_string(df["RESD_DESC"])
        elif "Residence_Description" in df.columns:
            df.loc[:, "RESIDENCY_DESCRIPTION"] = as_string(
                df["Residence_Description"]
            )

    # ---- LEVEL normalisation
    if "LEVEL" not in df.columns and "LEVL_CODE" in df.columns:
        df.loc[:, "LEVEL"] = df["LEVL_CODE"]

    if "LEVEL" in df.columns:
        lvl = as_string(df["LEVEL"]).replace({"PC": "PGT", "PR": "PGR"})
        df.loc[:, "LEVEL"] = lvl

    # ---- Reorder (final names)
    front = ["AGENT_CODE", "AGENT_SOURCE", "AGENT_NAME", "Student ID"]
    rest = [c for c in df.columns if c not in front]
    df = df.loc[:, front + rest]

    # ---- Drop helpers
    df.drop(
        [
            "AGENCY CODE",
            "AGENCY NAME",
            "Agent Source",
            "Agent_Code_Agency_Assisting_Application",
            "Agency_Assisting_Application",
            "Residence_Description",
        ],
        axis=1,
        errors="ignore",
        inplace=True,
    )

    logger.info("Final report cleaned.")
    return df



def process_banner(banner_path: str) -> pd.DataFrame:
    logger.info("Processing Banner…")

    usecols = [
        "AGENCY CODE", "AGENCY NAME", "ID",
        "APPLICATION DATE", "ENTRY TERM", "DOMICILE DESC",
        "Residence_Description", "LEVL_CODE", "FACULTY NAME", "PROGRAM",
        "PROGRAM DESCRIPTION", "OnCampus", "LATEST DECISION",
        "APDC DESC2", "DECISION DATE", "ESTS CODE", "ESTS DESC",
        "Last Institution Code", "RESD_DESC", "UCAS_ID"
    ]

    dtype_map = {
        "ID": "Int32",
        "ENTRY TERM": "string",
        "DOMICILE DESC": "string",
        "Residence_Description": "string",
        "LEVL_CODE": "string",
        "FACULTY NAME": "string",
        "PROGRAM": "Int32",
        "PROGRAM DESCRIPTION": "string",
        "OnCampus": "string",
        "LATEST DECISION": "string",
        "APDC DESC2": "string",
        "ESTS CODE": "string",
        "ESTS DESC": "string",
        "Last Institution Code": "string",
        "AGENCY CODE": "string",
        "AGENCY NAME": "string",
        "RESD_DESC": "string",
        "UCAS_ID": "string"
    }

    # Load
    banner_df = load_large_excel(banner_path, usecols, dtype_map)

    # ---------------------------------------------------------
    # 1️⃣ Filter rows where ESTS CODE is EN / EL / EC / EP ONLY
    # ---------------------------------------------------------
    valid_ests = ["EN", "EL", "EC", "EP"]

    if "ESTS CODE" in banner_df.columns:
        banner_df["ESTS CODE"] = banner_df["ESTS CODE"].astype(str).str.strip().str.upper()
        banner_df = banner_df[banner_df["ESTS CODE"].isin(valid_ests)]
    else:
        logger.warning("ESTS CODE column not found — returning empty dataframe.")
        return pd.DataFrame()

    # ---------------------------------------------------------
    # 2️⃣ Filter rows where LEVL_CODE = PC
    # ---------------------------------------------------------
    # if "LEVL_CODE" in banner_df.columns:
    #     banner_df["LEVL_CODE"] = banner_df["LEVL_CODE"].astype(str).str.strip().str.upper()
    #     banner_df = banner_df[banner_df["LEVL_CODE"] == "PC"]
    # else:
    #     logger.warning("LEVL_CODE column missing — skipping LEVL_CODE filter.")

    # ---------------------------------------------------------
    # 3️⃣ Convert Dates
    # ---------------------------------------------------------
    for dcol in ["APPLICATION DATE", "DECISION DATE"]:
        if dcol in banner_df.columns:
            banner_df[dcol] = pd.to_datetime(banner_df[dcol], errors="coerce")

    # ---------------------------------------------------------
    # 4️⃣ Academic Year Extraction
    # ---------------------------------------------------------
    if "APPLICATION DATE" in banner_df.columns:
        banner_df["Application_Year"] = extract_academic_year(
            banner_df["APPLICATION DATE"]
        )
    else:
        banner_df["Application_Year"] = np.nan

    # ---------------------------------------------------------
    # 5️⃣ Program mapping
    # ---------------------------------------------------------
    if "PROGRAM" in banner_df.columns:
        banner_df["PresessionalCourse"] = np.where(
            banner_df["PROGRAM"].isin(PRE_SESSIONAL_PROGRAM_CODES), "Y", "N"
        )
        banner_df["Summer_School"] = np.where(
            banner_df["PROGRAM"].isin(SUMMER_SCHOOL_PROGRAM_CODES), "Y", "N"
        )
    else:
        banner_df["PresessionalCourse"] = ""
        banner_df["Summer_School"] = ""

    # ---------------------------------------------------------
    # 6️⃣ Pathway mapping
    # ---------------------------------------------------------
    if "OnCampus" in banner_df.columns:
        banner_df["Pathway"] = np.where(
            banner_df["OnCampus"].astype(str).str.upper() == "Y", "CEG", ""
        )
    else:
        banner_df["Pathway"] = ""

    logger.info(f"Banner records loaded after filters: {len(banner_df)}")

    return banner_df.reset_index(drop=True)


def process_dynamics(dynamics_path: str) -> pd.DataFrame:
    logger.info("Processing Dynamics…")
    usecols = [
        "Banner ID", "Agent_Code_Agency_Assisting_Application",
        "Agency_Assisting_Application", "Agent_Code_Post_App", "Post_App_Agent", "UCAS_ID"
    ]
    dtype_map = {
        "Banner ID": "Int32",
        "Agent_Code_Agency_Assisting_Application": "string",
        "Agency_Assisting_Application": "string",
        "Agent_Code_Post_App": "string",
        "Post_App_Agent": "string",
        "UCAS_ID": "string"
    }

    dynamics_df = load_large_excel(dynamics_path, usecols, dtype_map)
    if "Banner ID" in dynamics_df.columns:
        dynamics_df = dynamics_df.drop_duplicates(subset=["Banner ID"])

    # Drop UCAS_ID to avoid _x/_y in merge
    if "UCAS_ID" in dynamics_df.columns:
        dynamics_df = dynamics_df.drop(columns=["UCAS_ID"])

    logger.info(f"Dynamics records after dedupe: {len(dynamics_df)}")
    return dynamics_df.reset_index(drop=True)

# ---------------------------
# New robust text normalizer & constants for Fee04 logic
# ---------------------------
def _norm_txt(s: pd.Series) -> pd.Series:
    return as_string(s).str.strip().str.casefold().fillna("")

SELF_CODES = {"self", "self-funded", "self funded", "selffunded"}
TUITION_KEY = "tuition"
SCHOLARSHIP_KEY = "tuition fee reduction"

def calculate_fee_metrics(group: pd.DataFrame) -> pd.Series:
    # Normalize text once
    fee_type = _norm_txt(group["Fee Type(T)"])  # e.g., "tuition fees"
    sponsor_code = _norm_txt(group["Sponsor Code"])  # e.g., "self"
    sponsor_t = _norm_txt(group["Sponsor Code(T)"])  # e.g., "self-funded"

    # Numeric value
    val = pd.to_numeric(group["Original Transaction Value"], errors="coerce")

    # ------- TUITION -------
    tuition_mask = fee_type.str.contains(TUITION_KEY)
    self_mask = sponsor_code.isin(SELF_CODES) | sponsor_t.isin(SELF_CODES)

    tu_mask_primary = tuition_mask & self_mask
    tu_mask_fallback = tuition_mask

    tuition_candidates = group.loc[tu_mask_primary, "Original Transaction Value"].astype(float)
    if tuition_candidates.empty:
        tuition_candidates = group.loc[tu_mask_fallback, "Original Transaction Value"].astype(float)

    tuition_val = np.nan
    if not tuition_candidates.empty:
        pos = tuition_candidates[tuition_candidates > 0]
        tuition_val = pos.max() if not pos.empty else tuition_candidates.abs().max()

    # ------- SCHOLARSHIP (discount) -------
    scholarship_mask = (sponsor_code == "det") & (sponsor_t.str.contains(SCHOLARSHIP_KEY))
    scholarship_vals = group.loc[scholarship_mask, "Original Transaction Value"].astype(float)
    scholarship_abs = scholarship_vals.abs().max() if not scholarship_vals.empty else np.nan

    # ------- COMMISSIONABLE -------
    if pd.notna(tuition_val):
        if pd.notna(scholarship_abs):
            commissionable = max(float(tuition_val) - float(scholarship_abs), 0.0)
        else:
            commissionable = float(tuition_val)
    else:
        commissionable = np.nan

    # ------- PRE-SESSIONAL FEE -------
    prog_mask = group["Programme"].isin(PRE_SESSIONAL_PROGRAM_CODES)
    pres_type = _norm_txt(group["Fee Type(T)"]).eq("pre-sessional fee deposit")
    pres_self = sponsor_code.isin(SELF_CODES)
    pres_mask = prog_mask & pres_type & pres_self

    # Check which rows are selected by pres_mask
    pres_vals = group.loc[pres_mask, "Original Transaction Value"]

    # Calculate the sum of absolute values
    pres_vals_abs_sum = pres_vals.abs().sum() if not pres_vals.empty else np.nan

    # Adjust Tuition Fees based on pre-sessional condition
    if pres_type.any():  # Check if there are any "pre-sessional fee deposit" records
        tuition_val = pres_vals_abs_sum if pd.notna(pres_vals_abs_sum) else tuition_val

    # ------- FIXED TUITION FEE BASED ON PROGRAM CODE -------
    fixed_program_codes = {
        8809: 3510, 8383: 3510, 4291: 3510,
        8810: 5775, 8384: 5775, 4287: 5775,
        8802: 7920, 8811: 7920
    }

    # Check if the program code exists in fixed_program_codes, and set the fixed tuition value
    prog = pd.to_numeric(group.get("Programme", np.nan), errors="coerce")
    
    if prog.isin(fixed_program_codes.keys()).any():
        # Assign the fixed tuition value for matching program codes
        tuition_val = prog.map(fixed_program_codes).iloc[0]  # Map and get the fixed value for the first row

    return pd.Series({
        "Tuition_Fees": tuition_val if pd.notna(tuition_val) else np.nan,
        "Scholarship_Discount": scholarship_abs if pd.notna(scholarship_abs) else np.nan,
        "Commissionable_Amount": commissionable if pd.notna(commissionable) else np.nan,
        "Presessional_Fee": pres_vals_abs_sum if pd.notna(pres_vals_abs_sum) else np.nan,
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
        "Transaction Type": "string",
        "Sponsor Code": "string",
        "Enrolment Status": "string",
        "Original Transaction Value": "float32",
        "Fee Type(T)": "string",
        "Sponsor Code(T)": "string",
        "Programme": "Int32",
        "Study Level(T)": "string",
    }
    fee = load_large_excel(fee04_path, usecols, dtype_map)

    # Keep only enrolled
    # if "Enrolment Status" in fee.columns:
    #     fee = fee[_norm_txt(fee["Enrolment Status"]).eq("en")]

    logger.info(f"Filtered Fee04 rows: {len(fee)}")
    if fee.empty or "Student ID" not in fee.columns:
        logger.warning("No Fee04 records after filtering or missing Student ID.")
        return pd.DataFrame(columns=["Student ID", "Tuition_Fees", "Scholarship_Discount", "Commissionable_Amount", "Presessional_Fee"])

    needed_cols = ["Original Transaction Value", "Fee Type(T)", "Sponsor Code", "Sponsor Code(T)", "Programme", "Transaction Type"]
    grouped = fee.groupby("Student ID", group_keys=False)[needed_cols].apply(calculate_fee_metrics).reset_index()
    logger.info(f"Processed fee metrics for {len(grouped)} students")
    return grouped

# def merge_datasets(banner: pd.DataFrame, dynamics: pd.DataFrame, fee04: pd.DataFrame) -> pd.DataFrame:
#     logger.info("Merging datasets…")
#     merged = pd.merge(banner, dynamics, left_on="ID", right_on="Banner ID", how="left")
#     final = pd.merge(merged, fee04, left_on="ID", right_on="Student ID", how="left")
#     final.drop(["Banner ID", "Student ID"], axis=1, errors="ignore", inplace=True)

#     # ⚠️ Do NOT fill with 0 here; keep NaN to detect truly missing matches
#     # for col in ["Tuition_Fees", "Scholarship_Discount", "Commissionable_Amount", "Presessional_Fee"]:
#     #     if col in final.columns:
#     #         final[col] = final[col].fillna(0)

#     logger.info(f"Final merged records: {len(final)}")
#     return final

def merge_datasets(banner: pd.DataFrame, dynamics: pd.DataFrame, fee04: pd.DataFrame) -> pd.DataFrame:
    logger.info("Merging datasets…")
    merged = pd.merge(
        banner,
        dynamics,
        left_on="ID",
        right_on="Banner ID",
        how="left",
        suffixes=("", "_dyn"),  # suffix only affects dynamics cols
    )

    final = pd.merge(
        merged,
        fee04,
        left_on="ID",
        right_on="Student ID",
        how="left",
        suffixes=("", "_fee"),
    )

    final.drop(["Banner ID", "Student ID"], axis=1, errors="ignore", inplace=True)
    logger.info(f"Final merged records: {len(final)}")
    return final

# ---------------------------
# Column mapping + placeholders (categorical-safe)
# ---------------------------
def _norm(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r'[\s_]+', ' ', s)
    return s.casefold()

def _is_blank_series(s: pd.Series) -> pd.Series:
    s2 = as_string(s)
    return s2.isna() | s2.str.strip().eq("") | s2.str.strip().eq("--")

def apply_column_mapping_safe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.astype(str).str.strip()

    col_by_norm = {_norm(c): c for c in df.columns}

    for src, dst in column_mapping.items():
        src_norm, dst_norm = _norm(src), _norm(dst)
        src_col = col_by_norm.get(src_norm)
        dst_col = col_by_norm.get(dst_norm)

        if not src_col and not dst_col:
            continue

        if src_col and dst_col:
            if is_categorical_dtype(df[dst_col]):
                df[dst_col] = as_string(df[dst_col])
            mask = _is_blank_series(df[dst_col])
            df[src_col] = as_string(df[src_col])
            df.loc[mask, dst_col] = df.loc[mask, src_col]
            if src_col != dst_col:
                df.drop(columns=[src_col], inplace=True)
                col_by_norm = {_norm(c): c for c in df.columns}
            if dst_col != dst:
                df.rename(columns={dst_col: dst}, inplace=True)
                col_by_norm = {_norm(c): c for c in df.columns}
            continue

        if src_col and not dst_col:
            df.rename(columns={src_col: dst}, inplace=True)
            col_by_norm = {_norm(c): c for c in df.columns}
            continue

        if dst_col and not src_col:
            if dst_col != dst:
                df.rename(columns={dst_col: dst}, inplace=True)
                col_by_norm = {_norm(c): c for c in df.columns}

    return df

def enforce_no_placeholder(df: pd.DataFrame, cols: list[str], placeholder: str="--", recategorize: bool=False) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            continue
        s = df[c]
        was_cat = is_categorical_dtype(s)
        s = as_string(s)
        s = s.str.strip()
        s = s.mask(s.eq(""), pd.NA)
        s = s.mask(s.eq("--"), pd.NA)
        s = s.fillna(placeholder)
        df[c] = s
        if was_cat and recategorize:
            df[c] = df[c].astype("category")
    return df

# ---------------------------
# FastAPI
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
    """
    Background worker:
      1) Load and process Banner / Dynamics / Fee04
      2) Merge, clean, map columns, enforce placeholders
      3) Sanity-log key columns
      4) Export and POST to callback
    """
    out_path = None
    try:
        # 1) Load sources
        banner = process_banner(banner_path)
        dynamics = process_dynamics(dynamics_path)
        fee04   = process_fee04(fee04_path)

        # 2) Merge + clean
        final_report = merge_datasets(banner, dynamics, fee04)
        final_report = clean_final_report(final_report)

        # 3) Map old->new (safe coalescing) then enforce placeholders on key columns
        final_report = apply_column_mapping_safe(final_report)

        final_report = enforce_no_placeholder(
            final_report,
            NEVER_PLACEHOLDER_COLS,
            placeholder="--",
            recategorize=False,
        )

        # and optionally keep this (it turns any existing "--" into empty)
        final_report = remove_placeholder_dashes(final_report)

        # 3.5) Quick integrity log for key columns
        for col in ["AGENT_CODE", "AGENT_SOURCE", "AGENT_NAME"]:
            if col in final_report.columns:
                nonblank = as_string(final_report[col]).str.strip().ne("").sum()
                total = len(final_report)
                pct = (nonblank / total * 100.0) if total else 0.0
                logger.info(f"[sanity] {col}: {nonblank}/{total} populated ({pct:.1f}%)")
            else:
                logger.error(f"[sanity] {col} missing from final report!")

        # 4) Export to temp file
        out = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        out_path = out.name
        out.close()
        final_report.to_excel(out_path, index=False)

        # 5) Prepare callback
        headers = {}
        if callback_token:
            headers["X-Callback-Token"] = callback_token

        with open(out_path, "rb") as fh:
            files = {
                "file": (
                    "final_enrolment_report.xlsx",
                    fh,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            }
            resp = requests.post(
                callback_url,
                files=files,
                data=passthrough,
                headers=headers,
                timeout=(5, 20),
            )
            logger.info("Callback POST -> %s %s", resp.status_code, str(resp.text)[:300])

    except requests.exceptions.ReadTimeout:
        logger.warning("Callback timed out waiting for response; continuing.")
    except Exception:
        logger.exception("Background processing failed")
    finally:
        # cleanup temp files
        for p in (banner_path, dynamics_path, fee04_path, out_path):
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
        "requested_by": requested_by or "",
    }

    background_tasks.add_task(
        _do_generate_and_callback,
        b_path, d_path, f_path, callback_url, callback_token, passthrough
    )
    return JSONResponse(status_code=202, content={"status": "accepted"})

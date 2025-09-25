from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
import pandas as pd
import tempfile
import os

from utils import (
    process_banner,
    process_dynamics,
    process_fee04,
    merge_datasets,
    clean_final_report,
    logger,
)

app = FastAPI(title="Enrolment Report API")

# Define the new column names mapping
column_mapping = {
    'Agent Code': 'AGENT_CODE',
    'Agent Source': 'AGENT_SOURCE',
    'Agent Name': 'AGENT_NAME',
    'Student ID': 'APPLICANT_NO',
    'First Name': 'FORENAME',
    'MIDDLE_NAMES':'MIDDLE_NAMES',
    'SURNAME': 'SURNAME',
    'PATHWAY_1': 'PATHWAY_1',
    'PATHWAY_2': 'PATHWAY_2',
    'SCHOOL_NAME': 'SCHOOL_NAME',
    'ENQUIRY_DETAIL': 'ENQUIRY_DETAIL',

    'ENTRY TERM': 'ENTRY_TERM',
    'Domicile': 'COUNTRY_OF_DOMICILE',
    'Residence_Description': 'RESIDENCY_DESCRIPTION',
    'Level_Code': 'LEVEL',
    'Faculty': 'FACULTY',
    'Program_Code': 'PROGRAMME_NAME',
    'Program_Description': 'PROGRAMME_DESCRIPTION',
    'OnCampus': 'MODE',
    'Latest Decision': 'DECISION',
    'Decision_Description': 'DECISION_DESCRIPTION',
    'Application Date': 'APPLICATION_DATE',
    'Registration_Code': 'REGISTRATION_CODE',
    'Application_Year': 'APPLICATION_YEAR',
    'PresessionalCourse': 'PRES_SESSIONAL_COURSE',
    'Summer_School': 'SUMMER_SCHOOL',
    'Pathway': 'PATHWAY',
    'Agent_Code_Post_App': 'AGENT_CODE_POST_APP',
    'Post_App_Agent': 'POST_APP_AGENT',
    'Tuition_Fees': 'TUITION_FEE',
    'Scholarship_Discount': 'SCHOLARSHIP',
    'Commissionable_Amount': 'COMMISSIONABLE_AMOUNT',
    'Presessional_Fee': 'PRES_SESSIONAL_FEE',
    'DECISION DATE': 'DECISION_DATE',
    'Last Institution Code': 'LAST_INSTITUTION_CODE',
    'ESTS CODE': 'ESTS_CODE',
    'ESTS DESC': 'ESTS_DESC',
}

@app.post("/generate-report/")
async def generate_report():
    """
    Generate enrolment report from existing Banner, Dynamics, and Fee04 Excel files.
    """
    try:
        # 1️⃣ Paths to your existing files
        banner_path = os.path.join(os.getcwd(), "banner_document.xlsx")
        dynamics_path = os.path.join(os.getcwd(), "dynamics_document.xlsx")
        fee04_path = os.path.join(os.getcwd(), "fee04_document.xlsx")

        # 2️⃣ Process datasets
        banner = process_banner(banner_path)
        dynamics = process_dynamics(dynamics_path)
        fee04 = process_fee04(fee04_path)

        # 3️⃣ Merge and clean
        final_report = merge_datasets(banner, dynamics, fee04)
        final_report = clean_final_report(final_report)

        # 4️⃣ Check if 'First Name', 'MIDDLE_NAMES', 'SURNAME' columns are missing, add them with default value '--'
        if 'FORENAME' not in final_report.columns:
            final_report['FORENAME'] = '--'
        
        if 'MIDDLE_NAMES' not in final_report.columns:
            final_report['MIDDLE_NAMES'] = '--'
        
        if 'SURNAME' not in final_report.columns:
            final_report['SURNAME'] = '--'

        if 'PATHWAY_1' not in final_report.columns:
            final_report['PATHWAY_1'] = '--'

        if 'PATHWAY_2' not in final_report.columns:
            final_report['PATHWAY_2'] = '--'

        if 'SCHOOL_NAME' not in final_report.columns:
            final_report['SCHOOL_NAME'] = '--'

        if 'ENQUIRY_DETAIL' not in final_report.columns:
            final_report['ENQUIRY_DETAIL'] = '--'



        # 5️⃣ Rename the columns based on the mapping
        final_report.rename(columns=column_mapping, inplace=True)

        # 6️⃣ Save output to temp file
        output_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        final_report.to_excel(output_file.name, index=False)

        logger.info(f"Report generated with {len(final_report)} rows")

        # 7️⃣ Return the generated file as a download
        return FileResponse(
            output_file.name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename="final_enrolment_report.xlsx",
        )

    except Exception as e:
        logger.error(f"Processing failed: {str(e)}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/")
async def root():
    return {"message": "Enrolment Report API is running."}

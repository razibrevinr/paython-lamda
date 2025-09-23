from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, JSONResponse
import os
import pandas as pd
import tempfile
from utils import process_banner, process_dynamics, process_fee04, merge_datasets, clean_final_report, logger

app = FastAPI(title="Enrolment Report API")

@app.post("/generate-report/")
async def generate_report(
    banner_file: UploadFile = File(...),
    dynamics_file: UploadFile = File(...),
    fee04_file: UploadFile = File(...)
):
    """
    Generate enrolment report from Banner, Dynamics, and Fee04 Excel files.
    """
    try:
        # Save uploaded files to temporary directory
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as banner_tmp:
            banner_path = banner_tmp.name
            content = await banner_file.read()
            banner_tmp.write(content)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as dynamics_tmp:
            dynamics_path = dynamics_tmp.name
            content = await dynamics_file.read()
            dynamics_tmp.write(content)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as fee04_tmp:
            fee04_path = fee04_tmp.name
            content = await fee04_file.read()
            fee04_tmp.write(content)

        # Process datasets
        banner = process_banner(banner_path)
        dynamics = process_dynamics(dynamics_path)
        fee04 = process_fee04(fee04_path)

        # Merge and clean
        final_report = merge_datasets(banner, dynamics, fee04)
        final_report = clean_final_report(final_report)

        # Save output to temp file
        output_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        final_report.to_excel(output_file.name, index=False)

        logger.info(f"Report generated with {len(final_report)} rows")

        return FileResponse(output_file.name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            filename="final_enrolment_report.xlsx")

    except Exception as e:
        logger.error(f"Processing failed: {str(e)}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/")
async def root():
    return {"message": "Enrolment Report API is running."}

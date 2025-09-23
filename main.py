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

@app.get("/generate-report/")
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

        # 4️⃣ Save output to temp file
        output_file = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        final_report.to_excel(output_file.name, index=False)

        logger.info(f"Report generated with {len(final_report)} rows")

        # 5️⃣ Return the generated file as a download
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

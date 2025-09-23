# Soton Enrolment ETL Script

This project processes university enrolment, agent, and fee data from Excel files, merges them, calculates financial metrics, and outputs a final report.  
It is designed for efficient memory usage and robust error handling.

## Features

- Loads large Excel files with optimized memory usage
- Cleans and merges Banner, Dynamics, and Fee04 datasets
- Calculates tuition, scholarship, commissionable, and presessional fee metrics
- Handles locked output files by versioning
- Outputs a final enrolment report in Excel format

## Usage

1. Place your input files in the project folder:
   - `banner_document.xlsx`
   - `dynamics_document.xlsx`
   - `fee04_document.xlsx`

2. Run the script:
   ```bash
   python app.py
   ```

3. The output will be saved as `final_enrolment_report.xlsx`.  
   If the file is open, it will be renamed with a version number and a new file will be created.

## Requirements

See [requirements.txt](requirements.txt) for dependencies.

## Logging

The script logs progress, memory usage, and sample output metrics for validation.

## File Structure

- `app.py` — Main ETL script
- `README.md` — Project documentation
- `requirements.txt` — Python dependencies

## License

MIT License

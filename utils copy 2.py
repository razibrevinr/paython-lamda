import pandas as pd
import numpy as np
import os
import logging
import time
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PRE_SESSIONAL_PROGRAM_CODES = [4287, 4291, 8383, 8384, 8454, 8802, 8809, 8810, 8811, 8332]
SUMMER_SCHOOL_PROGRAM_CODES = [9541, 9544, 9546, 9547]

# Function to reduce memory usage by downcasting numeric types
def reduce_memory_usage(df):
    """Optimize DataFrame memory usage by downcasting numeric types."""
    start_mem = df.memory_usage().sum() / 1024**2
    for col in df.columns:
        col_type = df[col].dtype
        
        # Convert integers
        if col_type in ['int64', 'int32']:
            min_val = df[col].min()
            max_val = df[col].max()
            if min_val > np.iinfo(np.int8).min and max_val < np.iinfo(np.int8).max:
                df[col] = df[col].astype(np.int8)
            elif min_val > np.iinfo(np.int16).min and max_val < np.iinfo(np.int16).max:
                df[col] = df[col].astype(np.int16)
            elif min_val > np.iinfo(np.int32).min and max_val < np.iinfo(np.int32).max:
                df[col] = df[col].astype(np.int32)
        
        # Convert floats
        elif col_type in ['float64', 'float32']:
            df[col] = df[col].astype(np.float32)
        
        # Convert objects to category if low cardinality
        elif col_type == 'object':
            num_unique = df[col].nunique()
            num_total = len(df[col])
            if num_unique / num_total < 0.5:
                df[col] = df[col].astype('category')
    
    end_mem = df.memory_usage().sum() / 1024**2
    logger.info(f"Memory reduced by {start_mem - end_mem:.2f} MB ({1 - end_mem/start_mem:.1%})")
    return df

def clean_final_report(df):
    """Clean and enhance final enrolment report with unified agent details."""
    # Your cleaning logic here
    return df
# Function to load large Excel file with optimized memory usage
def load_large_excel(file_path, usecols, dtype_map=None):
    """Load large Excel files with optimized settings."""
    logger.info(f"Loading {file_path} with optimized settings")

    # Read the first few rows to get the columns
    df = pd.read_excel(file_path, engine='openpyxl', nrows=5)
    available_columns = df.columns.tolist()
    logger.info(f"Columns found: {available_columns}")

    # Dynamically map columns based on available columns in the file
    valid_columns = [col for col in usecols if col in available_columns]

    # Read the full file with valid columns
    df = pd.read_excel(file_path, usecols=valid_columns, engine='openpyxl')

    # Apply data type conversions if specified
    if dtype_map:
        for col, dtype in dtype_map.items():
            if col in df.columns:
                if dtype == 'int32':
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int32')
                elif dtype == 'float32':
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('float32')
                elif dtype == 'category':
                    df[col] = df[col].astype('category')

    return reduce_memory_usage(df)

# Function to extract academic year based on the application date
def extract_academic_year(dt_series):
    """Vectorized extraction of academic year from datetime series."""
    years = np.full(len(dt_series), np.nan, dtype=object)
    valid_mask = dt_series.notnull()

    # Process valid dates only
    valid_dates = dt_series[valid_mask]
    year_vals = valid_dates.dt.year.values
    month_vals = valid_dates.dt.month.values

    def format_years(start, end):
        return f"{start}-{str(end)[-2:]}"
    
    academic_years = np.array([
        format_years(y, y + 1) if m >= 8 else format_years(y - 1, y)
        for y, m in zip(year_vals, month_vals)
    ])

    years[valid_mask] = academic_years
    return years

# Process the Banner document
def process_banner(banner_path):
    """Process Banner document with memory-efficient operations."""
    logger.info("Processing Banner document...")

    # Define column mappings based on expected columns
    column_mapping = {
        'Agency Code (Banner)': 'Agent Code',
        'Agency Name (Banner)': 'Agent Name',
        'Program Code': 'Program Code',
        'Program Description': 'Program Description',
        'ID': 'Student ID',
        'Faculty': 'Faculty',
        'Level_Code': 'Level Code',
        'Application Date': 'Application Date'
    }

    usecols = list(column_mapping.keys())

    dtype_map = {
        'ID': 'int32',
        'Program Code': 'category',
        'Level_Code': 'category',
        'Faculty': 'category'
    }

    # Load data
    banner_df = load_large_excel(banner_path, usecols, dtype_map)

    # Rename columns according to the mapping
    banner_df.rename(columns=column_mapping, inplace=True)
    logger.info(f"Banner records loaded: {len(banner_df)} rows after renaming")

    return banner_df

# Process the Dynamics document
def process_dynamics(dynamics_path):
    """Process Dynamics document with memory-efficient operations."""
    logger.info("Processing Dynamics document...")

    # Define your column mapping for Dynamics file
    column_mapping = {
        'Banner ID': 'Banner ID',
        'Agent_Code_Agency_Assisting_Application': 'Agent Code',
        'Agency_Assisting_Application': 'Agent Name'
    }

    usecols = list(column_mapping.keys())

    dtype_map = {
        'Banner ID': 'int32',
        'Agent_Code_Agency_Assisting_Application': 'category',
        'Agency_Assisting_Application': 'category'
    }

    dynamics_df = load_large_excel(dynamics_path, usecols, dtype_map)

    # Rename columns to match expected names
    dynamics_df.rename(columns=column_mapping, inplace=True)
    dynamics_df = dynamics_df.drop_duplicates('Banner ID')
    logger.info(f"Dynamics records loaded: {len(dynamics_df)} rows after renaming")

    return dynamics_df

# Process Fee04 data
def process_fee04(fee04_path):
    """Process Fee04 document with optimized operations and fee logic."""
    logger.info("Processing Fee04 document...")

    # Define column mapping for Fee04 document
    column_mapping = {
        'Student ID': 'Student ID',
        'Transaction Type': 'Transaction Type',
        'Sponsor Code': 'Sponsor Code',
        'Enrolment Status': 'Enrolment Status',
        'Original Transaction Value': 'Original Transaction Value'
    }

    usecols = list(column_mapping.keys())

    dtype_map = {
        'Student ID': 'int32',
        'Transaction Type': 'category',
        'Sponsor Code': 'category',
        'Enrolment Status': 'category',
        'Original Transaction Value': 'float32'
    }

    fee04_df = load_large_excel(fee04_path, usecols, dtype_map)

    # Filter records
    fee04_filtered = fee04_df[fee04_df['Enrolment Status'] == 'EN']
    logger.info(f"Fee04 records loaded: {len(fee04_filtered)} rows after renaming")

    return fee04_filtered

# Merge datasets
def merge_datasets(banner_df, dynamics_df, fee04_df):
    """Merge datasets efficiently."""
    logger.info("Merging datasets...")

    # Merge Banner with Dynamics
    merged_df = pd.merge(
        banner_df, dynamics_df, left_on='Student ID', right_on='Banner ID', how='left'
    )

    # Merge with Fee04 data
    final_df = pd.merge(
        merged_df, fee04_df, left_on='Student ID', right_on='Student ID', how='left'
    )

    # Clean up merged DataFrame
    final_df.drop(['Banner ID'], axis=1, errors='ignore', inplace=True)

    logger.info(f"Final merged records: {len(final_df)}")
    return final_df
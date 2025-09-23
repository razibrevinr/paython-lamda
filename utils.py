import pandas as pd
import numpy as np
import os
import logging
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PRE_SESSIONAL_PROGRAM_CODES = [4287, 4291, 8383, 8384, 8454, 8802, 8809, 8810, 8811, 8332]
SUMMER_SCHOOL_PROGRAM_CODES = [9541, 9544, 9546, 9547]

def reduce_memory_usage(df):
    """Optimize DataFrame memory usage by downcasting numeric types"""
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

def extract_academic_year(dt_series):
    """Vectorized extraction of academic year from datetime series"""
    # Academic year: August-July (adjust month threshold as needed)
    years = np.full(len(dt_series), np.nan, dtype=object)
    valid_mask = dt_series.notnull()

    # Process valid dates only
    valid_dates = dt_series[valid_mask]
    year_vals = valid_dates.dt.year.values
    month_vals = valid_dates.dt.month.values

    # Format years as strings with last two digits for academic year
    def format_years(start, end):
        return f"{start}-{str(end)[-2:]}"

    academic_years = np.array([
        format_years(y, y+1) if m >= 8 else format_years(y-1, y)
        for y, m in zip(year_vals, month_vals)
    ])

    years[valid_mask] = academic_years
    return years

def load_large_excel(file_path, usecols, dtype_map=None):
    """Load large Excel files with optimized memory usage"""
    logger.info(f"Loading {file_path} with optimized settings")
    
    # Read entire file with specified columns
    df = pd.read_excel(
        file_path,
        usecols=usecols,
        engine='openpyxl'
    )
    
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

# Clean and enhance final report
def clean_final_report(df):
    """Clean and enhance final enrolment report with unified agent details"""
    logger.info("Cleaning final enrolment report...")

    # Remove any existing Agent Code, Agent Source, Agent Name columns to avoid duplication
    df.drop(['Agent Code', 'Agent Source', 'Agent Name', 'Student ID'], axis=1, errors='ignore', inplace=True)
    # rename ID to Student ID and drop ID column after creating Student ID column. set type to int32
    df.rename(columns={'ID': 'Student ID'}, inplace=True)
    df['Student ID'] = df['Student ID'].astype('int32')

    # Create unified Agent Code column with source tracking
    conditions = [
        df['Agency Code (Banner)'].notna() & (df['Agency Code (Banner)'] != ''),
        df['Agent_Code_Agency_Assisting_Application'].notna() & (df['Agent_Code_Agency_Assisting_Application'] != '')
    ]
    choices = [
        df['Agency Code (Banner)'],
        df['Agent_Code_Agency_Assisting_Application']
    ]
    df['Agent Code'] = np.select(conditions, choices, default=np.nan)

    # Create Agent Source column
    df['Agent Source'] = np.select(
        conditions,
        ['Banner', 'Dynamics'],
        default=None
    )

    # Create Agent Name column
    df['Agent Name'] = np.where(
        df.get('Agency Name (Banner)', pd.Series([np.nan]*len(df))).notna() & (df.get('Agency Name (Banner)', pd.Series([np.nan]*len(df))) != ''),
        df.get('Agency Name (Banner)', pd.Series([np.nan]*len(df))),
        np.where(
            df.get('Agency_Assisting_Application', pd.Series([np.nan]*len(df))).notna() & (df.get('Agency_Assisting_Application', pd.Series([np.nan]*len(df))) != ''),
            df.get('Agency_Assisting_Application', pd.Series([np.nan]*len(df))),
            ''
        )
    )

    # make the new three columns categorical to save memory and create in first three columns
    df['Agent Code'] = df['Agent Code'].astype('category')
    df['Agent Name'] = df['Agent Name'].astype('category')
    df['Agent Source'] = df['Agent Source'].astype('category')
    df = df[['Agent Code', 'Agent Source', 'Agent Name', 'Student ID'] + [col for col in df.columns if col not in ['Agent Code', 'Agent Source', 'Agent Name', 'Student ID']]]

    # rename Level_Code to following PC to PGT, PR to PGR, UG remains UG
    if 'Level_Code' in df.columns and df['Level_Code'].notna().any():
        if isinstance(df['Level_Code'].dtype, pd.CategoricalDtype):
            df['Level_Code'] = df['Level_Code'].cat.rename_categories({'PC': 'PGT', 'PR': 'PGR', 'UG': 'UG'})
        else:
            df['Level_Code'] = df['Level_Code'].replace({'PC': 'PGT', 'PR': 'PGR'})

    #Drop intermediate columns if present
    df.drop([
        'Agency Code (Banner)',
        'Agency Name (Banner)',
        'Agent_Code_Agency_Assisting_Application',
        'Agency_Assisting_Application'
    ], axis=1, errors='ignore', inplace=True)

    logger.info("Final report cleaned with unified agent details")
    return df

def process_banner(banner_path):
    """Process Banner document with memory-efficient operations"""
    logger.info("Processing Banner document...")
    
    # Define necessary columns
    usecols = ['Agency Code (Banner)', 'Agency Name (Banner)', 'ID', 'Registration_Code', 'Application Date', 'ENTRY TERM', 'Domicile', 
               'Residence_Description', 'Level_Code', 'Faculty', 'Program_Code', 'Program_Description', 'OnCampus', 'Latest Decision',
               'Decision_Description']
    
    # Define data types
    dtype_map = {
        'ID': 'int32',
        'Registration_Code': 'category',
        'ENTRY TERM': 'category',
        'Domicile': 'category',
        'Residence_Description': 'category',
        'Level_Code': 'category',
        'Faculty': 'category',
        'Program_Code': 'category',
        'Agency Code (Banner)': 'category',
        'Agency Name (Banner)': 'category',
        'latest Decision': 'category',
        'Decision_Description': 'category',
    }
    
    # Load data
    banner_df = load_large_excel(banner_path, usecols, dtype_map)
    
    # Extract academic year
    banner_df['Application Date'] = pd.to_datetime(
        banner_df['Application Date'], 
        errors='coerce'
    )
    banner_df['Application_Year'] = extract_academic_year(banner_df['Application Date'])

    # if Program_Code values are in 4287, 4291, 8383, 8384, 8454, 8802, 8809, 8810, 8811 then set PressessionalCourse to Y otherwise N
    presessional_mask = banner_df['Program_Code'].isin(PRE_SESSIONAL_PROGRAM_CODES)
    banner_df['PresessionalCourse'] = np.where(presessional_mask, 'Y', 'N')

    # if Program_Code values are in 9541, 9544, 9546, 9547 then set SummerSchool to Y otherwise N
    summer_school_mask = banner_df['Program_Code'].isin(SUMMER_SCHOOL_PROGRAM_CODES)
    banner_df['Summer_School'] = np.where(summer_school_mask, 'Y', 'N')

    # Convert 'OnCampus' Y value to Pathway column with vlaue CEG and N value to Pathway column with empty
    banner_df['Pathway'] = np.where(banner_df['OnCampus'] == 'Y', 'CEG', '')

    # Log record count
    logger.info(f"Banner records loaded: {len(banner_df)}") 
    
    # Filter records
    banner_df = banner_df[
        (banner_df['Registration_Code'] == 'EN') &
        (banner_df['Application_Year'] == '2023-24')
    ]
    
    logger.info(f"Filtered Banner records with EN and Application Year: {len(banner_df)} rows")
    return banner_df.reset_index(drop=True)

def process_dynamics(dynamics_path):
    """Process Dynamics document with minimal memory footprint"""
    logger.info("Processing Dynamics document...")
    
    # Define columns
    usecols = ['Banner ID', 'Agent_Code_Agency_Assisting_Application', 
               'Agency_Assisting_Application', 'Agent_Code_Post_App', 'Post_App_Agent']
    
    # Define data types
    dtype_map = {
        'Banner ID': 'int32',
        'Agent_Code_Agency_Assisting_Application': 'category',
        'Agency_Assisting_Application': 'category',
        'Agent_Code_Post_App': 'category',
        'Post_App_Agent': 'category'
    }
    
    # Load data
    dynamics_df = load_large_excel(dynamics_path, usecols, dtype_map)
    
    # Process data
    dynamics_df = dynamics_df.drop_duplicates('Banner ID')
    logger.info(f"Dynamics records after deduplication: {len(dynamics_df)}")
    return dynamics_df.reset_index(drop=True)

def calculate_fee_metrics(group):
    """Calculate fee metrics according to business rules"""
    # Regular tuition fees (UG, PG, PR, PC)
    tuition_mask = (
        (group['Fee Type(T)'] == 'Tuition Fees') &
        (group['Sponsor Code'] == 'SELF') &
        (group['Sponsor Code(T)'] == 'Self')
    )
    tuition_fees = group.loc[tuition_mask, 'Original Transaction Value'].max()
    
    # Scholarship discount
    scholarship_mask = (
        (group['Fee Type(T)'] == 'Tuition Fees') &
        (group['Sponsor Code'] == 'DET') &
        (group['Sponsor Code(T)'] == 'Tuition Fee Reduction')
    )
    scholarship_discount = group.loc[scholarship_mask, 'Original Transaction Value'].max()

    # make negetive scholarship discount positive
    scholarship_discount = abs(scholarship_discount) if pd.notna(scholarship_discount) else 0
    
    # Commissionable amount with ternary logic if scholarship discount is less than tuition fees then substract scholarship discount from tuition fees else use tuition fees
    if pd.isna(scholarship_discount):
        commissionable_amount = tuition_fees
    commissionable_amount = tuition_fees - scholarship_discount
    
    # Presessional course fees (PS)
    presessional_mask = (
        (group['Programme'].isin(PRE_SESSIONAL_PROGRAM_CODES)) &
        (group['Fee Type(T)'] == 'Pre-Sessional Fee Deposit') &
        (group['Sponsor Code'] == 'SELF')
    )
    presessional_fee = group.loc[presessional_mask, 'Original Transaction Value'].max()
    
    return pd.Series({
        'Tuition_Fees': tuition_fees,
        'Scholarship_Discount': scholarship_discount,
        'Commissionable_Amount': commissionable_amount,
        'Presessional_Fee': presessional_fee
    })

def process_fee04(fee04_path):
    """Process Fee04 document with optimized operations and fee logic"""
    logger.info("Processing Fee04 document...")
    
    # Define columns
    usecols = ['Student ID', 'Transaction Type', 'Sponsor Code', 
               'Enrolment Status', 'Original Transaction Value',
               'Fee Type(T)', 'Sponsor Code(T)', 'Programme', 'Study Level(T)']
    
    # Define data types
    dtype_map = {
        'Student ID': 'int32',
        'Transaction Type': 'category',
        'Sponsor Code': 'category',
        'Enrolment Status': 'category',
        'Original Transaction Value': 'float32',
        'Fee Type(T)': 'category',
        'Sponsor Code(T)': 'category',
        'Programme': 'int32',
        'Study Level(T)': 'category'
    }
    
    # Load data
    fee04_df = load_large_excel(fee04_path, usecols, dtype_map)
    
    # Filter records
    fee04_filtered = fee04_df[
        (fee04_df['Enrolment Status'] == 'EN')
    ]
    logger.info(f"Filtered Fee04 transactions: {len(fee04_filtered)}")
    
    # Calculate metrics
    logger.info("Calculating fee metrics with business rules...")
    fee04_grouped = fee04_filtered.groupby('Student ID').apply(calculate_fee_metrics).reset_index()
    
    logger.info(f"Processed fee metrics for {len(fee04_grouped)} students")
    return fee04_grouped

def merge_datasets(banner, dynamics, fee04):
    """Merge datasets efficiently using optimized join operations"""
    logger.info("Merging datasets...")
    
    # Merge Banner with Dynamics
    merged = pd.merge(
        banner,
        dynamics,
        left_on='ID',
        right_on='Banner ID',
        how='left'
    )
    
    # Merge with Fee04
    final = pd.merge(
        merged,
        fee04,
        left_on='ID',
        right_on='Student ID',
        how='left'
    )
    
    # Cleanup
    final.drop(['Banner ID', 'Student ID'], axis=1, errors='ignore', inplace=True)
    
    # Fill missing financials
    financial_cols = ['Tuition_Fees', 'Scholarship_Discount', 'Commissionable_Amount',
                     'Presessional_Fee']
    for col in financial_cols:
        if col in final.columns:
            final[col] = final[col].fillna(0)
    
    logger.info(f"Final merged records: {len(final)}")
    return final


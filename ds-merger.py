import pandas as pd
import os
from datetime import datetime

def print_progress(percentage, message):
    print(f"[{percentage}%] {message}")

def main():
    print("Starting enrolment report processing...")
    
    # Load datasets
    try:
        print_progress(10, "Loading Banner document...")
        banner_df = pd.read_excel('banner_document.xlsx')
        
        print_progress(20, "Loading Dynamics document...")
        dynamics_df = pd.read_excel('dynamics_document.xlsx')
        
        print_progress(30, "Loading Fee04 document...")
        fee04_df = pd.read_excel('fee04_document.xlsx')
    except Exception as e:
        print(f"Error loading files: {e}")
        return

    # Process Banner Document
    try:
        print_progress(40, "Processing Banner data...")
        # Convert Application Date to datetime
        banner_df['Application Date'] = pd.to_datetime(banner_df['Application Date'], errors='coerce')
        
        # Extract Application Year in 'YY-YY' format
        banner_df['Application_Year'] = banner_df['Application Date'].apply(
            lambda x: f"{x.year % 100:02d}-{(x.year + 1) % 100:02d}" 
            if not pd.isnull(x) and x.month >= 7  # Academic year starts July/August
            else f"{(x.year - 1) % 100:02d}-{x.year % 100:02d}" 
            if not pd.isnull(x) 
            else None
        )
        
        # Filter data
        banner_filtered = banner_df[
            (banner_df['Registration_Code'] == 'EN') &
            (banner_df['Application_Year'] == '23-24')
        ].copy()
        
        print(f"  Found {len(banner_filtered)} eligible students in Banner")
    except KeyError as e:
        print(f"Missing column in Banner data: {e}")
        return
    except Exception as e:
        print(f"Error processing Banner data: {e}")
        return

    # Process Dynamics Document
    try:
        print_progress(50, "Processing Dynamics data...")
        dynamics_processed = dynamics_df[[
            'Banner ID',
            'Agent_Code_Agency_Assisting_Application',
            'Agency_Assisting_Application',
            'Agent_Code_Post_App',
            'Post_App_Agent'
        ]].drop_duplicates(subset=['Banner ID'])
        
        print(f"  Extracted agent info for {len(dynamics_processed)} students")
    except KeyError as e:
        print(f"Missing column in Dynamics data: {e}")
        return

    # Process Fee04 Document
    try:
        print_progress(60, "Processing Fee04 data...")
        fee04_filtered = fee04_df[
            (fee04_df['Transaction Type'] == 'T') &
            (fee04_df['Sponsor Code'] == 'DET') &
            (fee04_df['Enrolment Status'] == 'EN')
        ].copy()
        
        print_progress(70, "Calculating fee metrics...")
        # Group by student and calculate metrics
        fee04_grouped = fee04_filtered.groupby('Student ID').agg(
            Tuition_Fees=('Original Transaction Value', 'max'),
            Fees_Deductions=('Original Transaction Value', lambda x: x[x < 0].sum())
        )
        fee04_grouped['Net_Amount'] = fee04_grouped['Tuition_Fees'] + fee04_grouped['Fees_Deductions']
        fee04_grouped.reset_index(inplace=True)
        
        print(f"  Processed fees for {len(fee04_grouped)} students")
    except KeyError as e:
        print(f"Missing column in Fee04 data: {e}")
        return

    # Merge datasets
    try:
        print_progress(80, "Merging Banner and Dynamics data...")
        # Merge Banner with Dynamics
        merged_df = pd.merge(
            banner_filtered,
            dynamics_processed,
            left_on='ID',
            right_on='Banner ID',
            how='left'
        )
        
        print_progress(90, "Merging with Fee04 data...")
        # Merge with Fee04 data
        final_report = pd.merge(
            merged_df,
            fee04_grouped,
            left_on='ID',
            right_on='Student ID',
            how='left'
        )
        
        # Cleanup
        final_report.drop(['Banner ID', 'Student ID'], 
                         axis=1, errors='ignore', inplace=True)
        final_report.fillna({
            'Tuition_Fees': 0,
            'Fees_Deductions': 0,
            'Net_Amount': 0
        }, inplace=True)
    except Exception as e:
        print(f"Error during merge: {e}")
        return

    # Save output
    try:
        print_progress(95, "Saving final report...")
        final_report.to_excel('final_enrolment_report.xlsx', index=False)
        
        print_progress(100, "Process completed successfully!")
        print("\nReport Summary:")
        print(f"- Total students: {len(final_report)}")
        print(f"- Application Date range: {final_report['Application Date'].min().date()} to {final_report['Application Date'].max().date()}")
        print(f"- Students with agent info: {final_report['Agent_Code_Agency_Assisting_Application'].count()}")
        print(f"- Students with fee records: {final_report[final_report['Tuition_Fees'] > 0].shape[0]}")
        print(f"Output saved as 'final_enrolment_report.xlsx'")
    except Exception as e:
        print(f"Error saving report: {e}")

if __name__ == "__main__":
    main()
import json
import tempfile
import boto3
from utils import process_banner, process_dynamics, process_fee04, merge_datasets, clean_final_report

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # 1️⃣ Get S3 file paths from event
        banner_s3 = event['banner_s3']
        dynamics_s3 = event['dynamics_s3']
        fee04_s3 = event['fee04_s3']
        bucket = event['bucket']

        # 2️⃣ Download files to /tmp (Lambda temp folder)
        banner_file = '/tmp/banner.xlsx'
        dynamics_file = '/tmp/dynamics.xlsx'
        fee04_file = '/tmp/fee04.xlsx'

        s3_client.download_file(bucket, banner_s3, banner_file)
        s3_client.download_file(bucket, dynamics_s3, dynamics_file)
        s3_client.download_file(bucket, fee04_s3, fee04_file)

        # 3️⃣ Process data
        banner = process_banner(banner_file)
        dynamics = process_dynamics(dynamics_file)
        fee04 = process_fee04(fee04_file)

        final_report = merge_datasets(banner, dynamics, fee04)
        final_report = clean_final_report(final_report)

        # 4️⃣ Save result to /tmp
        output_file = '/tmp/final_report.xlsx'
        final_report.to_excel(output_file, index=False)

        # 5️⃣ Upload to S3
        result_key = 'final_reports/final_report.xlsx'
        s3_client.upload_file(output_file, bucket, result_key)

        # 6️⃣ Return result S3 URL
        result_url = f"https://{bucket}.s3.amazonaws.com/{result_key}"
        return {
            'statusCode': 200,
            'body': json.dumps({'result_url': result_url})
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

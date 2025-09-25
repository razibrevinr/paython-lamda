import json
import boto3
import base64
import io
import os
from typing import List, Dict, Any

# Initialize AWS clients
s3_client = boto3.client('s3')

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda function to merge 3 files and upload to S3
    
    Expected event structure:
    {
        "files": [
            {"name": "file1.txt", "content": "base64_encoded_content"},
            {"name": "file2.txt", "content": "base64_encoded_content"},
            {"name": "file3.txt", "content": "base64_encoded_content"}
        ],
        "merge_type": "concatenate|json_merge|csv_merge",
        "output_filename": "merged_file.txt",
        "s3_bucket": "your-bucket-name",
        "s3_key": "path/to/merged_file.txt"
    }
    """
    
    try:
        # Extract parameters from event
        files = event.get('files', [])
        merge_type = event.get('merge_type', 'concatenate')
        output_filename = event.get('output_filename', 'merged_file.txt')
        s3_bucket = event.get('s3_bucket', os.environ.get('S3_BUCKET'))
        s3_key = event.get('s3_key', f'merged_files/{output_filename}')
        
        # Validate input
        if len(files) != 3:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Exactly 3 files are required'})
            }
        
        if not s3_bucket:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'S3 bucket not specified'})
            }
        
        # Decode files
        decoded_files = []
        for file_info in files:
            try:
                content = base64.b64decode(file_info['content']).decode('utf-8')
                decoded_files.append({
                    'name': file_info['name'],
                    'content': content
                })
            except Exception as e:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': f'Error decoding file {file_info["name"]}: {str(e)}'})
                }
        
        # Merge files based on type
        merged_content = merge_files(decoded_files, merge_type)
        
        # Upload to S3
        s3_response = upload_to_s3(merged_content, s3_bucket, s3_key)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Files merged and uploaded successfully',
                's3_location': f's3://{s3_bucket}/{s3_key}',
                's3_url': f'https://{s3_bucket}.s3.amazonaws.com/{s3_key}',
                'file_size': len(merged_content.encode('utf-8'))
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Internal server error: {str(e)}'})
        }

def merge_files(files: List[Dict[str, str]], merge_type: str) -> str:
    """Merge files based on the specified type"""
    if merge_type == 'concatenate':
        return concatenate_files(files)
    elif merge_type == 'json_merge':
        return merge_json_files(files)
    elif merge_type == 'csv_merge':
        return merge_csv_files(files)
    else:
        return concatenate_files(files)

def concatenate_files(files: List[Dict[str, str]]) -> str:
    """Simple concatenation of files with separators"""
    merged_content = []
    for i, file_info in enumerate(files, 1):
        merged_content.append(f"=== FILE {i}: {file_info['name']} ===")
        merged_content.append(file_info['content'])
        merged_content.append("")
    return "\n".join(merged_content)

def merge_json_files(files: List[Dict[str, str]]) -> str:
    """Merge JSON files into a single JSON structure"""
    merged_data = {}
    for file_info in files:
        try:
            file_data = json.loads(file_info['content'])
            key = os.path.splitext(file_info['name'])[0]
            merged_data[key] = file_data
        except json.JSONDecodeError:
            key = os.path.splitext(file_info['name'])[0]
            merged_data[key] = file_info['content']
    return json.dumps(merged_data, indent=2)

def merge_csv_files(files: List[Dict[str, str]]) -> str:
    """Merge CSV files"""
    merged_lines = []
    header_added = False
    for file_info in files:
        lines = file_info['content'].strip().split('\n')
        if not header_added and lines:
            merged_lines.append(lines[0])
            header_added = True
            merged_lines.extend(lines[1:])
        else:
            if len(lines) > 1:
                merged_lines.extend(lines[1:])
    return '\n'.join(merged_lines)

def upload_to_s3(content: str, bucket: str, key: str) -> Dict[str, Any]:
    """Upload content to S3"""
    try:
        response = s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content.encode('utf-8'),
            ContentType='text/plain'
        )
        return response
    except Exception as e:
        raise Exception(f"Failed to upload to S3: {str(e)}")
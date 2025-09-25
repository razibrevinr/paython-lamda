import json
import base64
import requests

def test_merger():
    """Test the file merger functionality"""
    
    # Sample files content
    files = [
        {
            "name": "file1.txt",
            "content": base64.b64encode("Hello from file 1\nLine 2 of file 1".encode()).decode()
        },
        {
            "name": "file2.txt", 
            "content": base64.b64encode("Hello from file 2\nLine 2 of file 2".encode()).decode()
        },
        {
            "name": "file3.txt",
            "content": base64.b64encode("Hello from file 3\nLine 2 of file 3".encode()).decode()
        }
    ]
    
    payload = {
        "files": files,
        "merge_type": "concatenate",
        "output_filename": "merged_test.txt",
        "s3_bucket": "your-test-bucket",
        "s3_key": "test/merged_test.txt"
    }
    
    # For local testing, you can import and call the function directly
    from lambda_function import lambda_handler
    
    class MockContext:
        pass
    
    result = lambda_handler(payload, MockContext())
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    test_merger()
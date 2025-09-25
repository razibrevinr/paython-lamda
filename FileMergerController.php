<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Illuminate\Http\JsonResponse;

class FileMergerController extends Controller
{
    private $lambdaEndpoint;
    private $s3Bucket;

    public function __construct()
    {
        $this->lambdaEndpoint = config('services.aws.lambda_endpoint');
        $this->s3Bucket = config('services.aws.s3_bucket');
    }

    /**
     * Merge 3 files using AWS Lambda
     */
    public function mergeFiles(Request $request): JsonResponse
    {
        try {
            // Validate request
            $request->validate([
                'files' => 'required|array|size:3',
                'files.*.name' => 'required|string',
                'files.*.content' => 'required|file',
                'merge_type' => 'required|in:concatenate,json_merge,csv_merge',
                'output_filename' => 'required|string'
            ]);

            // Process files
            $files = [];
            foreach ($request->file('files') as $index => $file) {
                $content = file_get_contents($file->getPathname());
                $files[] = [
                    'name' => $request->input("files.{$index}.name") ?? $file->getClientOriginalName(),
                    'content' => base64_encode($content)
                ];
            }

            // Prepare Lambda payload
            $payload = [
                'files' => $files,
                'merge_type' => $request->input('merge_type'),
                'output_filename' => $request->input('output_filename'),
                's3_bucket' => $this->s3Bucket,
                's3_key' => 'merged_files/' . $request->input('output_filename')
            ];

            // Call Lambda function
            $response = Http::timeout(300)
                ->post($this->lambdaEndpoint, $payload);

            if ($response->successful()) {
                $result = $response->json();

                // Log success
                Log::info('Files merged successfully', [
                    'output_filename' => $request->input('output_filename'),
                    's3_location' => $result['body']['s3_location'] ?? null
                ]);

                return response()->json([
                    'success' => true,
                    'message' => 'Files merged successfully',
                    'data' => json_decode($result['body'], true)
                ]);
            } else {
                Log::error('Lambda function failed', [
                    'status' => $response->status(),
                    'response' => $response->body()
                ]);

                return response()->json([
                    'success' => false,
                    'message' => 'Failed to merge files',
                    'error' => $response->body()
                ], 500);
            }
        } catch (\Exception $e) {
            Log::error('File merge error', [
                'message' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);

            return response()->json([
                'success' => false,
                'message' => 'An error occurred while merging files',
                'error' => $e->getMessage()
            ], 500);
        }
    }

    /**
     * Get merged file from S3
     */
    public function getFile(Request $request): JsonResponse
    {
        try {
            $request->validate([
                's3_key' => 'required|string'
            ]);

            $s3Url = "https://{$this->s3Bucket}.s3.amazonaws.com/" . $request->input('s3_key');

            return response()->json([
                'success' => true,
                'download_url' => $s3Url
            ]);
        } catch (\Exception $e) {
            return response()->json([
                'success' => false,
                'message' => 'Error retrieving file',
                'error' => $e->getMessage()
            ], 500);
        }
    }
}

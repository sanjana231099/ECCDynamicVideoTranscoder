# import boto3
# import subprocess
# import os
# import time
# import sys
# import uuid
# from botocore.exceptions import ClientError
# from pyspark.sql import SparkSession

# # AWS Clients
# dynamodb = boto3.resource('dynamodb')
# s3 = boto3.client('s3')

# # Your DynamoDB Table Name
# TABLE_NAME = 'TranscodeJobs'  # TODO: change this
# table = dynamodb.Table(TABLE_NAME)

# # Polling Interval (seconds)
# POLL_INTERVAL = 5

# # Parse transcoding settings from command line arguments
# if len(sys.argv) != 4:
#     print("Usage: python transcoding_worker.py <output_format> <output_resolution> <output_codec>")
#     sys.exit(1)

# output_format = sys.argv[1]    # Example: mp4
# output_resolution = sys.argv[2]  # Example: 1280x720
# output_codec = sys.argv[3]    # Example: libx264

# def poll_jobs():
#     while True:
#         print("Polling DynamoDB for PENDING jobs...")

#         try:
#             # Find jobs that are PENDING
#             response = table.scan(
#                 FilterExpression="Status = :pending",
#                 ExpressionAttributeValues={":pending": "PENDING"}
#             )
#             jobs = response.get('Items', [])

#             if not jobs:
#                 print("No pending jobs. Sleeping...")
#                 time.sleep(POLL_INTERVAL)
#                 continue

#             for job in jobs:
#                 try:
#                     job_id = job['job_id']
#                     s3_key = job['s3_key']
#                     bucket_name = job['video-transcoder-input1']  # Assuming this is stored in the job
#                     print(f"Found job {job_id}: {s3_key}")

#                     # Try to "lock" the job by marking it PROCESSING
#                     success = lock_job(job_id)
#                     if not success:
#                         print(f"Could not lock job {job_id}, maybe another worker picked it. Skipping.")
#                         continue

#                     # Download from S3
#                     local_input_file = f"/tmp/{uuid.uuid4()}_{os.path.basename(s3_key)}"
#                     s3.download_file(bucket_name, s3_key, local_input_file)
#                     print(f"Downloaded {s3_key} to {local_input_file}")

#                     # Prepare output file
#                     output_file = f"/tmp/transcoded_{os.path.basename(local_input_file)}"
#                     ffmpeg_cmd = [
#                         'ffmpeg', '-y', '-i', local_input_file,
#                         '-vf', f'scale={output_resolution}',
#                         '-c:v', output_codec,
#                         output_file
#                     ]
#                     print(f"Starting transcoding: {output_file}")
#                     subprocess.run(ffmpeg_cmd, check=True)

#                     # Upload back to S3
#                     output_s3_key = f"transcoded/{os.path.basename(output_file)}"
#                     s3.upload_file(output_file, bucket_name, output_s3_key)
#                     print(f"Uploaded transcoded file to {output_s3_key}")

#                     # Update job to COMPLETED
#                     table.update_item(
#                         Key={'job_id': job_id},
#                         UpdateExpression="SET job_status = :completed, output_s3_key = :out",
#                         ExpressionAttributeValues={
#                             ":completed": "COMPLETED",
#                             ":out": output_s3_key
#                         }
#                     )

#                     print(f"Job {job_id} marked as COMPLETED!")

#                     # Cleanup temp files
#                     os.remove(local_input_file)
#                     os.remove(output_file)

#                 except Exception as e:
#                     print(f"Error processing job {job_id}: {e}")

#         except ClientError as e:
#             print(f"AWS ClientError: {e}")
#         except Exception as e:
#             print(f"Error polling jobs: {e}")

#         time.sleep(POLL_INTERVAL)

# def lock_job(job_id):
#     """ Try to mark a job as PROCESSING atomically to avoid double processing """
#     try:
#         response = table.update_item(
#             Key={'job_id': job_id},
#             ConditionExpression="job_status = :pending",
#             UpdateExpression="SET job_status = :processing",
#             ExpressionAttributeValues={
#                 ":pending": "PENDING",
#                 ":processing": "PROCESSING"
#             }
#         )
#         return True
#     except ClientError as e:
#         if e.response['Error']['Code'] == "ConditionalCheckFailedException":
#             return False  # Someone else already locked it
#         else:
#             raise

# if _name_ == "_main_":
#     poll_jobs()


import boto3
import subprocess
import os
import time
import sys
import uuid
from botocore.exceptions import ClientError

# AWS Clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Your DynamoDB Table Name
TABLE_NAME = 'TranscodeJobs'  # TODO: change this
table = dynamodb.Table(TABLE_NAME)

# Your S3 Bucket Name
BUCKET_NAME = 'video-transcoder-input1'  # TODO: change this

# Polling Interval (seconds)
POLL_INTERVAL = 5

# Parse transcoding settings from command line arguments
if len(sys.argv) != 4:
    print("Usage: python v2_transcode_program.py <output_format> <output_resolution> <output_codec>")
    sys.exit(1)

output_format = sys.argv[1]      # Example: mp4
output_resolution = sys.argv[2]  # Example: 1280x720
output_codec = sys.argv[3]       # Example: libx264

def poll_jobs():
    while True:
        print("Polling DynamoDB for PENDING jobs...")

        try:
            # Find jobs that are PENDING
            response = table.scan(
                FilterExpression="#s = :pending",
                ExpressionAttributeNames={"#s": "Status"},
                ExpressionAttributeValues={":pending": "PENDING"}
            )
            jobs = response.get('Items', [])

            if not jobs:
                print("No pending jobs. Sleeping...")
                time.sleep(POLL_INTERVAL)
                continue

            for job in jobs:
                try:
                    job_id = job['JobId']
                    s3_key = job['InputKey']
                    print(f"Found job {job_id}: {s3_key}")

                    # Lock the job
                    success = lock_job(job_id)
                    if not success:
                        print(f"Could not lock job {job_id}, maybe another worker picked it. Skipping.")
                        continue

                    # Download from S3
                    local_input_file = f"/tmp/{uuid.uuid4()}_{os.path.basename(s3_key)}"
                    s3.download_file(BUCKET_NAME, s3_key, local_input_file)
                    print(f"Downloaded {s3_key} to {local_input_file}")

                    # Prepare output file
                    output_file = f"/tmp/transcoded_{os.path.basename(local_input_file)}"
                    ffmpeg_cmd = [
                        'ffmpeg', '-y', '-i', local_input_file,
                        '-vf', f'scale={output_resolution}',
                        '-c:v', output_codec,
                        output_file
                    ]
                    print(f"Starting transcoding: {output_file}")
                    subprocess.run(ffmpeg_cmd, check=True)

                    # Upload back to S3
                    output_s3_key = f"transcoded/{os.path.basename(output_file)}"
                    s3.upload_file(output_file, BUCKET_NAME, output_s3_key)
                    print(f"Uploaded transcoded file to {output_s3_key}")

                    # Update job to COMPLETED
                    table.update_item(
                        Key={'JobId': job_id},
                        UpdateExpression="SET #s = :completed",
                        ExpressionAttributeNames={"#s": "Status"},
                        ExpressionAttributeValues={
                            ":completed": "COMPLETED"
                        }
                    )

                    print(f"Job {job_id} marked as COMPLETED!")

                    # Cleanup temp files
                    os.remove(local_input_file)
                    os.remove(output_file)

                except Exception as e:
                    print(f"Error processing job {job_id}: {e}")

        except ClientError as e:
            print(f"AWS ClientError: {e}")
        except Exception as e:
            print(f"Error polling jobs: {e}")

        time.sleep(POLL_INTERVAL)

def lock_job(job_id):
    """ Try to mark a job as PROCESSING atomically to avoid double processing """
    try:
        response = table.update_item(
            Key={'JobId': job_id},
            ConditionExpression="#s = :pending",
            UpdateExpression="SET #s = :processing",
            ExpressionAttributeNames={"#s": "Status"},
            ExpressionAttributeValues={
                ":pending": "PENDING",
                ":processing": "PROCESSING"
            }
        )
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            return False  # Someone else already locked it
        else:
            raise

if __name__ == "__main__":
    try:
        poll_jobs()
    except KeyboardInterrupt:
        print("\nExiting transcoding worker gracefully...")
        sys.exit(0)
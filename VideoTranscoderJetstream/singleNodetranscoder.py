import os
import sys
import time
import tempfile
import subprocess

from decimal import Decimal
import boto3
from botocore.exceptions import ClientError

# === CONFIGURATION ===
DYNAMODB_TABLE    = 'TranscodeJobs'
S3_BUCKET         = 'video-transcoder-input1'
S3_INPUT_PREFIX   = 'videos/'
S3_OUTPUT_PREFIX  = 'transcoded/'

dynamodb = boto3.resource('dynamodb')
table    = dynamodb.Table(DYNAMODB_TABLE)
s3       = boto3.client('s3')


def list_pending_jobs():
    try:
        resp = table.scan(
            FilterExpression="#s = :pending",
            ExpressionAttributeNames={"#s": "Status"},
            ExpressionAttributeValues={":pending": "PENDING"}
        )
        return resp.get('Items', [])
    except ClientError as e:
        print("Error scanning for pending jobs:", e)
        return []


def lock_job(job_id):
    try:
        table.update_item(
            Key={'JobId': job_id},
            ConditionExpression="#s = :pending",
            UpdateExpression="SET #s = :processing",
            ExpressionAttributeNames={"#s": "Status"},
            ExpressionAttributeValues={
                ":pending":    "PENDING",
                ":processing": "PROCESSING"
            }
        )
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            return False
        print("Error locking job", job_id, e)
        return False


def update_job_status(job_id, status, output_key=None, duration=None):
    """
    Update Status (and optionally OutputKey, DurationSeconds, and Mode)
    on a DynamoDB item. Converts duration to Decimal if provided.
    """
    expr_parts  = ["#s = :s"]
    expr_names  = {"#s": "Status"}
    expr_attrs  = {":s": status}

    if output_key:
        expr_parts.append("OutputKey = :o")
        expr_attrs[":o"] = output_key

    if duration is not None:
        # convert float → Decimal
        expr_parts.append("DurationSeconds = :d")
        expr_attrs[":d"] = Decimal(str(duration))
        # alias the reserved word Mode
        expr_names["#mo"] = "Mode"
        expr_parts.append("#mo = :m")
        expr_attrs[":m"] = "Single"

    update_expr = "SET " + ", ".join(expr_parts)

    try:
        table.update_item(
            Key={'JobId': job_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_attrs
        )
    except ClientError as e:
        print(f"Error updating job {job_id} to {status}: {e}")


def transcode_video(job, fmt, resolution, codec):
    job_id    = job['JobId']
    input_key = job['InputKey']

    with tempfile.TemporaryDirectory() as tmp:
        local_in  = os.path.join(tmp, os.path.basename(input_key))
        try:
            s3.download_file(S3_BUCKET, input_key, local_in)

            base       = os.path.splitext(os.path.basename(input_key))[0]
            local_out  = os.path.join(tmp, f"{base}_transcoded.{fmt}")
            output_key = f"{S3_OUTPUT_PREFIX}{os.path.basename(local_out)}"

            cmd = [
                "ffmpeg", "-y",
                "-analyzeduration", "10M", "-probesize", "20M",
                "-i", local_in,
                "-vf", f"scale={resolution}",
                "-c:v", codec,
                local_out
            ]

            start    = time.time()
            subprocess.run(cmd, check=True)
            duration = time.time() - start
            print(f"Job {job_id} transcoded in {duration:.2f}s")

            s3.upload_file(local_out, S3_BUCKET, output_key)

            # Now passes a Decimal-wrapped duration and Mode
            update_job_status(job_id, "COMPLETED", output_key, duration)

        except subprocess.CalledProcessError as e:
            update_job_status(job_id, "FAILED")
            print(f"[ffmpeg error] Job {job_id} failed: {e}")
        except ClientError as e:
            update_job_status(job_id, "FAILED")
            print(f"[AWS error] Job {job_id} failed: {e}")
        except Exception as e:
            update_job_status(job_id, "FAILED")
            print(f"[Unexpected error] Job {job_id} failed: {e}")


def main(fmt, resolution, codec, poll_interval=30):
    print("Starting transcoder loop…")
    while True:
        jobs = list_pending_jobs()
        if not jobs:
            print(f"No pending jobs; sleeping {poll_interval}s")
            time.sleep(poll_interval)
            continue

        for job in jobs:
            if lock_job(job['JobId']):
                transcode_video(job, fmt, resolution, codec)

        time.sleep(5)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script.py <format> <resolution> <codec>")
        sys.exit(1)

    out_fmt   = sys.argv[1]
    out_res   = sys.argv[2]
    out_codec = sys.argv[3]

    main(out_fmt, out_res, out_codec)
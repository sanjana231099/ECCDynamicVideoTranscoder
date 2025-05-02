import boto3
import subprocess
import os
import sys
import uuid
import time
from decimal import Decimal  # For recording durations in DynamoDB
from pyspark.sql import SparkSession
from botocore.exceptions import ClientError
import shutil
import logging
import psutil  # For disk space checking

# AWS Configuration
DYNAMODB_TABLE   = 'TranscodeJobs'
S3_BUCKET        = 'video-transcoder-input1'
S3_INPUT_PREFIX  = 'videos/'       # Prefix for original videos in S3
S3_OUTPUT_PREFIX = 'transcoded/'   # Prefix for all transcoded outputs and intermediate segments

# Initialize AWS clients/resources
dynamodb = boto3.resource('dynamodb')
table    = dynamodb.Table(DYNAMODB_TABLE)

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_disk_space(path):
    """Check available disk space at the given path."""
    try:
        disk = psutil.disk_usage(path)
        free_gb = disk.free / (1024 ** 3)
        if free_gb < 1:
            logger.warning(f"Low disk space at {path}: {free_gb:.2f} GB free")
        return free_gb
    except Exception as e:
        logger.error(f"Error checking disk space at {path}: {e}")
        return 0


def create_spark_session():
    spark = SparkSession.builder \
        .appName("S3VideoTranscoder") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.executorEnv.AWS_ACCESS_KEY_ID", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])
    hadoop_conf.set("fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])
    return spark


def list_pending_jobs():
    """List all pending jobs from DynamoDB"""
    try:
        resp = table.scan(
            FilterExpression="#s = :pending",
            ExpressionAttributeNames={"#s": "Status"},
            ExpressionAttributeValues={":pending": "PENDING"}
        )
        return resp.get('Items', [])
    except ClientError as e:
        logger.error(f"Error listing pending jobs: {e}")
        return []


def lock_job(job_id):
    """Atomically lock a job to prevent duplicate processing"""
    try:
        table.update_item(
            Key={'JobId': job_id},
            ConditionExpression="#s = :pending",
            UpdateExpression="SET #s = :processing",
            ExpressionAttributeNames={"#s": "Status"},
            ExpressionAttributeValues={":pending": "PENDING", ":processing": "PROCESSING"}
        )
        return True
    except ClientError as e:
        # Someone else locked or status != PENDING
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return False
        logger.error(f"Error locking job {job_id}: {e}")
        return False


def update_job_status(job_id, status, output_key=None, hls_output_key=None, duration=None, mode=None):
    """Update job status in DynamoDB with optional output keys, duration, and mode"""
    expr_parts = ["#s = :status"]
    names      = {"#s": "Status"}
    vals       = {":status": status}

    if output_key:
        expr_parts.append("OutputKey = :output")
        vals[":output"] = output_key

    if hls_output_key:
        expr_parts.append("HLSOutputKey = :hls")
        vals[":hls"] = hls_output_key

    if duration is not None:
        expr_parts.append("DurationSeconds = :d")
        vals[":d"] = Decimal(str(duration))

    # Use a placeholder for "Mode", since it's a reserved word in DynamoDB
    if mode is not None:
        expr_parts.append("#m = :m")
        names["#m"] = "Mode"
        vals[":m"]  = mode

    update_expr = "SET " + ", ".join(expr_parts)

    try:
        table.update_item(
            Key={'JobId': job_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=vals
        )
    except ClientError as e:
        logger.error(f"Error updating job {job_id}: {e}")


def segment_video(input_file, temp_dir, job_id):
    """
    Segment video into 2-minute chunks using FFmpeg,
    upload segments to S3, and return their S3 keys.
    """
    if check_disk_space(temp_dir) < 1:
        raise RuntimeError(f"Insufficient disk space in {temp_dir}")

    seg_pattern = os.path.join(temp_dir, 'segment%03d.ts')
    cmd = [
        'ffmpeg', '-y', '-i', input_file,
        '-c', 'copy',
        '-segment_time', '120',
        '-f', 'segment',
        '-reset_timestamps', '1',
        seg_pattern
    ]
    subprocess.run(cmd, check=True)

    files = sorted(f for f in os.listdir(temp_dir) if f.startswith('segment') and f.endswith('.ts'))
    if not files:
        raise FileNotFoundError("No segments generated; check FFmpeg logs.")

    s3 = boto3.client('s3')
    s3_prefix = f"{S3_OUTPUT_PREFIX}segments/{job_id}/"
    segment_keys = []
    for seg in files:
        local_path = os.path.join(temp_dir, seg)
        key = s3_prefix + seg
        s3.upload_file(local_path, S3_BUCKET, key)
        segment_keys.append(key)
        logger.info(f"Uploaded segment {seg} to s3://{S3_BUCKET}/{key}")

    return segment_keys


def transcode_segment(s3_key, output_dir, output_format, output_resolution, output_codec):
    os.makedirs(output_dir, exist_ok=True)
    session = boto3.Session(
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
    )
    s3 = session.client('s3')
    name = os.path.basename(s3_key)
    local_in = os.path.join(output_dir, name)

    try:
        s3.download_file(S3_BUCKET, s3_key, local_in)
    except ClientError as e:
        raise RuntimeError(f"Failed to download {s3_key}: {e}")

    out_name = f"transcoded_{name}"
    local_out = os.path.join(output_dir, out_name)
    cmd = [
        'ffmpeg', '-y',
        '-analyzeduration', '10M',
        '-probesize', '20M',
        '-i', local_in,
        '-vf', f'scale={output_resolution}',
        '-c:v', output_codec,
        '-c:a', 'aac',
        '-f', 'mpegts',
        local_out
    ]
    subprocess.run(cmd, check=True)

    transcoded_key = f"{S3_OUTPUT_PREFIX}{out_name}"
    try:
        s3.upload_file(local_out, S3_BUCKET, transcoded_key)
    except ClientError as e:
        raise RuntimeError(f"Failed to upload {local_out}: {e}")

    return transcoded_key


def merge_segments(temp_dir, output_format, output_resolution, base_name, transcoded_keys):
    """
    Download all transcoded segments from S3, concatenate them,
    create HLS, and return local paths for upload.
    """
    s3 = boto3.client('s3')

    for key in transcoded_keys:
        local_path = os.path.join(temp_dir, os.path.basename(key))
        s3.download_file(S3_BUCKET, key, local_path)

    segments = sorted(f for f in os.listdir(temp_dir) if f.startswith('transcoded_') and f.endswith('.ts'))
    list_txt = os.path.join(temp_dir, 'files_list.txt')
    with open(list_txt, 'w') as lf:
        for seg in segments:
            lf.write(f"file '{seg}'\n")

    output_file = os.path.join(temp_dir, f"transcoded_{base_name}_{output_resolution}.{output_format}")
    concat_cmd = [
        'ffmpeg', '-y', '-f', 'concat', '-safe', '0',
        '-i', list_txt,
        '-c', 'copy',
        output_file
    ]
    subprocess.run(concat_cmd, check=True)

    hls_playlist = os.path.join(temp_dir, f"hls_{base_name}.m3u8")
    hls_cmd = [
        'ffmpeg', '-y',
        '-i', output_file,
        '-c:v', 'copy',
        '-c:a', 'copy',
        '-f', 'hls',
        '-hls_time', '10',
        '-hls_list_size', '0',
        '-hls_segment_filename', os.path.join(temp_dir, f"hls_{base_name}_%03d.ts"),
        hls_playlist
    ]
    subprocess.run(hls_cmd, check=True)

    return output_file, hls_playlist


def upload_and_update(job_id, output_file, hls_playlist, base_name):
    """
    Upload final video and HLS files to S3, update DynamoDB, and return status
    """
    s3 = boto3.client('s3')

    video_key     = f"{S3_OUTPUT_PREFIX}{os.path.basename(output_file)}"
    s3.upload_file(output_file, S3_BUCKET, video_key)

    playlist_key = f"{S3_OUTPUT_PREFIX}{os.path.basename(hls_playlist)}"
    for fname in os.listdir(os.path.dirname(hls_playlist)):
        if fname.startswith(f"hls_{base_name}"):
            local = os.path.join(os.path.dirname(hls_playlist), fname)
            key   = f"{S3_OUTPUT_PREFIX}{fname}"
            s3.upload_file(local, S3_BUCKET, key)

    # record video and playlist keys
    update_job_status(job_id, "COMPLETED", output_key=video_key, hls_output_key=playlist_key)
    return f"Job {job_id} completed successfully."


def cleanup_s3_segments(job_id, transcoded_keys):
    """
    Delete all .ts files (original and transcoded segments) from S3 for a given job.
    """
    s3 = boto3.client('s3')
    try:
        segment_prefix = f"{S3_OUTPUT_PREFIX}segments/{job_id}/"
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=segment_prefix)
        objects_to_delete = [{'Key': obj['Key']} for obj in response.get('Contents', []) if obj['Key'].endswith('.ts')]

        if objects_to_delete:
            s3.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': objects_to_delete})
            logger.info(f"Deleted {len(objects_to_delete)} original .ts segments for job {job_id}")

        objects_to_delete = [{'Key': key} for key in transcoded_keys if key.endswith('.ts')]
        if objects_to_delete:
            s3.delete_objects(Bucket=S3_BUCKET, Delete={'Objects': objects_to_delete})
            logger.info(f"Deleted {len(objects_to_delete)} transcoded .ts segments for job {job_id}")

    except ClientError as e:
        logger.error(f"Error deleting .ts files for job {job_id}: {e}")


def process_job(job, spark, output_format, output_resolution, output_codec):
    job_id    = job['JobId']
    input_key = job['InputKey']

    temp_dir = f"/tmp/transcode_{job_id}_{uuid.uuid4().hex}"
    os.makedirs(temp_dir, exist_ok=True)

    # start timer for entire job
    job_start = time.time()

    try:
        # Check if already done
        final_key = f"{S3_OUTPUT_PREFIX}transcoded_{os.path.splitext(os.path.basename(input_key))[0]}_{output_resolution}.{output_format}"
        try:
            boto3.client('s3').head_object(Bucket=S3_BUCKET, Key=final_key)
            update_job_status(job_id, "COMPLETED", output_key=final_key)
            logger.info(f"Job {job_id} already complete with final key {final_key}")
            return f"Job {job_id} already complete."
        except ClientError as ce:
            if ce.response['Error']['Code'] == '404':
                logger.info(f"Final output {final_key} not found, proceeding with processing job {job_id}")
            else:
                logger.error(f"Error checking final output {final_key}: {ce.response['Error']['Message']} (Code: {ce.response['Error']['Code']})")
                raise

        # Validate input file existence
        logger.info(f"Validating input file s3://{S3_BUCKET}/{input_key}")
        try:
            boto3.client('s3').head_object(Bucket=S3_BUCKET, Key=input_key)
        except ClientError as ce:
            if ce.response['Error']['Code'] == '404':
                logger.error(f"Input file s3://{S3_BUCKET}/{input_key} does not exist")
                update_job_status(job_id, "FAILED")
                return f"Job {job_id} failed: Input file {input_key} not found"
            logger.error(f"Error validating input file {input_key}: {ce.response['Error']['Message']} (Code: {ce.response['Error']['Code']})")
            raise

        # Download original
        local_in = os.path.join(temp_dir, os.path.basename(input_key))
        logger.info(f"Downloading input file s3://{S3_BUCKET}/{input_key} to {local_in}")
        boto3.client('s3').download_file(S3_BUCKET, input_key, local_in)

        # Segment and upload segments
        logger.info(f"Segmenting video for job {job_id}")
        segment_keys = segment_video(local_in, temp_dir, job_id)

        # Parallel transcode
        logger.info(f"Transcoding segments for job {job_id}")
        rdd = spark.sparkContext.parallelize(segment_keys)
        transcoded_keys = rdd.map(
            lambda key: transcode_segment(key, temp_dir, output_format, output_resolution, output_codec)
        ).collect()

        # Merge and HLS
        logger.info(f"Merging segments and creating HLS for job {job_id}")
        base_name = os.path.splitext(os.path.basename(input_key))[0]
        out_file, playlist = merge_segments(temp_dir, output_format, output_resolution, base_name, transcoded_keys)

        # Final upload
        logger.info(f"Uploading final outputs for job {job_id}")
        result = upload_and_update(job_id, out_file, playlist, base_name)

        # record job duration and mode
        job_duration = time.time() - job_start
        logger.info(f"Job {job_id} completed in {job_duration:.2f}s")
        update_job_status(job_id, "COMPLETED", duration=job_duration, mode="Parallel")

        # Clean up .ts files from S3
        logger.info(f"Cleaning up .ts files for job {job_id}")
        cleanup_s3_segments(job_id, transcoded_keys)

        return result

    except Exception as e:
        logger.error(f"Processing error for job {job_id}: {str(e)}", exc_info=True)
        update_job_status(job_id, "FAILED")
        return f"Job {job_id} failed: {str(e)}"
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    if len(sys.argv) != 4:
        print("Usage: spark-submit transcode_program_s3_only.py <format> <resolution> <codec>")
        sys.exit(1)

    fmt, res, codec = sys.argv[1:]
    spark = create_spark_session()
    try:
        while True:
            pending = list_pending_jobs()
            if not pending:
                logger.info("No pending jobs. Sleeping...")
                time.sleep(60)
                continue

            locked = [j for j in pending if lock_job(j['JobId'])]
            if not locked:
                logger.info("No jobs locked. Retrying...")
                time.sleep(10)
                continue

            for job in locked:
                logger.info(process_job(job, spark, fmt, res, codec))
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
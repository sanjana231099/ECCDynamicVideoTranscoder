import os, re
import boto3
from flask import Blueprint, request, Response, abort
from botocore.exceptions import ClientError

stream_bp = Blueprint('stream', __name__)

s3 = boto3.client(
    's3',
    region_name=os.getenv('AWS_REGION'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)
BUCKET = os.getenv('S3_BUCKET')

@stream_bp.route('/stream/<path:s3_key>')
def stream_video(s3_key):
    """
    Streams a video stored in S3, supporting HTTP Range requests.
    s3_key should be the full key (e.g. "transcoded/1234_myvideo.mp4").
    """
    range_header = request.headers.get("Range", None)
    if not range_header:
        # No range: return the whole file
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=s3_key)
        except ClientError:
            abort(404)
        data = obj["Body"].read()
        return Response(
            data,
            mimetype=obj["ContentType"],
            headers={
                "Content-Length": str(obj["ContentLength"]),
                "Accept-Ranges": "bytes"
            }
        )

    # Parse "bytes=start-end"
    m = re.match(r"bytes=(\d+)-(\d*)", range_header)
    if not m:
        abort(400)
    start = int(m.group(1))
    end   = m.group(2)
    byte_range = f"bytes={start}-{end}"  # AWS allows open-ended if end is blank

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=s3_key, Range=byte_range)
    except ClientError as e:
        # If the range is invalid or missing
        abort(416)

    data = obj["Body"].read()
    resp = Response(
        data,
        status=206,
        mimetype=obj["ContentType"],
        direct_passthrough=True
    )
    # Copy over the Content-Range header from S3
    resp.headers["Content-Range"]    = obj["ContentRange"]
    resp.headers["Accept-Ranges"]    = "bytes"
    resp.headers["Content-Length"]   = str(obj["ContentLength"])
    return resp
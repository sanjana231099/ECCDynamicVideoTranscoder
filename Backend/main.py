# import os, uuid
# from datetime import datetime
# from flask import Flask, request, jsonify, render_template
# import boto3
# from botocore.exceptions import BotoCoreError, ClientError
# from werkzeug.utils import secure_filename
# from dotenv import load_dotenv
# #from Stream import stream_bp

# load_dotenv()
# app = Flask(__name__)

# # S3 client
# s3 = boto3.client(
#     "s3",
#     region_name=os.getenv("AWS_REGION"),
#     aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
#     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
# )
# BUCKET = os.getenv("S3_BUCKET")

# # DynamoDB resource & table
# dynamo = boto3.resource(
#     "dynamodb",
#     region_name=os.getenv("AWS_REGION"),
#     aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
#     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
# )
# jobs_table = dynamo.Table(os.getenv("JOBS_TABLE"))

# @app.route("/upload", methods=["POST"])
# def upload_video():
#     # 1. Validate
#     if "file" not in request.files:
#         return jsonify({"error": "No file part"}), 400
#     file = request.files["file"]
#     if file.filename == "":
#         return jsonify({"error": "No selected file"}), 400

#     # 2. Secure filename & generate Job ID
#     filename = secure_filename(file.filename)
#     job_id = str(uuid.uuid4())
#     s3_key = f"videos/{job_id}_{filename}"

#     # 3. Upload raw video to S3
#     try:
#         s3.upload_fileobj(
#             Fileobj=file.stream,
#             Bucket=BUCKET,
#             Key=s3_key,
#             ExtraArgs={"ContentType": file.mimetype}
#         )
#     except (BotoCoreError, ClientError) as e:
#         return jsonify({"error": str(e)}), 500


#     # 4. Return the Job ID so frontend can poll for status
#     return jsonify({
#         "message": "Upload successful",
#         "s3_key": s3_key,
#         "s3_url": f"https://{BUCKET}.s3.{os.getenv('AWS_REGION')}.amazonaws.com/{s3_key}"
#     }), 202

# @app.route("/")
# def form():
#     return render_template("upload.html")

# # register the streaming blueprint
# #app.register_blueprint(stream_bp)

# if __name__ == "__main__":
#     app.run(debug=True)

import os
import uuid
from flask import Flask, request, jsonify, render_template
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

# === sanitize region ===
_raw_region = os.getenv("AWS_REGION", "")
AWS_REGION = _raw_region.split("#", 1)[0].strip()  # keep only what's before any '#' and strip whitespace

# S3 client
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
BUCKET = os.getenv("S3_BUCKET")

# DynamoDB resource & table
dynamo = boto3.resource(
    "dynamodb",
    region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
jobs_table = dynamo.Table(os.getenv("JOBS_TABLE"))

@app.route("/upload", methods=["POST"])
def upload_video():
    # 1. Validate
    if "file" not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "No selected file"}), 400

    # 2. Secure filename & generate Job ID
    filename = secure_filename(file.filename)
    job_id = str(uuid.uuid4())
    s3_key = f"videos/{job_id}_{filename}"

    # 3. Upload raw video to S3
    try:
        s3.upload_fileobj(
            Fileobj=file.stream,
            Bucket=BUCKET,
            Key=s3_key,
            ExtraArgs={"ContentType": file.mimetype}
        )
    except (BotoCoreError, ClientError) as e:
        return jsonify({"error": str(e)}), 500

    # 4. Return the Job ID so frontend can poll for status
    return jsonify({
        "message": "Upload successful",
        "s3_key": s3_key
    }), 202


# List transcoded videos
@app.route("/videos")
def list_videos():
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="transcoded/")
    keys = [obj["Key"] for obj in resp.get("Contents", [])]
    return jsonify({"videos": keys})

# Get presigned stream URL
@app.route("/stream")
def get_stream_url():
    key = request.args.get("key")
    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET, "Key": key},
        ExpiresIn=3600
    )
    return jsonify({"url": url})


if __name__ == "__main__":
    app.run(debug=True)


# import os, uuid
# from datetime import datetime
# from flask import Flask, request, jsonify, render_template, abort
# import boto3
# from botocore.exceptions import BotoCoreError, ClientError
# from werkzeug.utils import secure_filename
# from dotenv import load_dotenv
# from Stream import stream_bp

# load_dotenv()
# app = Flask(__name__)

# # S3 client
# s3 = boto3.client(
#     's3',
#     region_name=os.getenv('AWS_REGION'),
#     aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
#     aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
# )
# BUCKET = os.getenv('S3_BUCKET')

# # DynamoDB resource & table
# dynamo = boto3.resource(
#     'dynamodb',
#     region_name=os.getenv('AWS_REGION'),
#     aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
#     aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
# )
# jobs_table = dynamo.Table(os.getenv('JOBS_TABLE'))

# # register streaming blueprint
# app.register_blueprint(stream_bp)

# @app.route('/')
# def form():
#     return render_template('upload.html')

# @app.route('/upload', methods=['POST'])
# def upload_video():
#     if 'file' not in request.files:
#         return jsonify({'error': 'No file part'}), 400
#     file = request.files['file']
#     if file.filename == '':
#         return jsonify({'error': 'No selected file'}), 400

#     filename = secure_filename(file.filename)
#     job_id = str(uuid.uuid4())
#     orig_key = f"videos/{job_id}/{filename}"

#     try:
#         s3.upload_fileobj(file.stream, BUCKET, orig_key,
#                           ExtraArgs={'ContentType': file.mimetype})
#     except (BotoCoreError, ClientError) as e:
#         return jsonify({'error': str(e)}), 500

#     # Lambda trigger will create PENDING record in DynamoDB
#     return jsonify({'message': 'Upload successful', 'job_id': job_id}), 202

# @app.route('/status/<job_id>')
# def status(job_id):
#     resp = jobs_table.get_item(Key={'JobId': job_id})
#     item = resp.get('Item')
#     if not item:
#         return jsonify({'error': 'Not found'}), 404

#     data = {'status': item['Status']}
#     if item['Status'] == 'COMPLETED':
#         data['s3_key'] = item.get('TransKey') or item.get('ManifestKey')
#     return jsonify(data)

# @app.route('/player/<job_id>')
# def player(job_id):
#     resp = jobs_table.get_item(Key={'JobId': job_id})
#     item = resp.get('Item')
#     if not item:
#         abort(404)
#     if item['Status'] != 'COMPLETED':
#         return f"Job status: {item['Status']}", 202
#     return render_template('player.html', s3_key=item.get('TransKey') or item.get('ManifestKey'))

# if __name__ == '__main__':
#     app.run(debug=True)
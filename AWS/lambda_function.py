import json
import logging
import os
import uuid
from datetime import datetime
import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DynamoDB resource & table (uses Lambda’s built-in region automatically)
dynamo = boto3.resource("dynamodb")
table  = dynamo.Table(os.environ["JOBS_TABLE"])

def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))

    for rec in event.get("Records", []):
        # full S3 key, e.g. "uploads/.../673f9026-415f-4823-ad1d-53be4693ea88_GroupDiscussion"
        input_key = rec["s3"]["object"]["key"]

        # get the filename portion after the last slash
        filename = os.path.basename(input_key) 
        
        # extract everything after the first underscore
        if "_" in filename:
            name_after_underscore = filename.split("_", 1)[1]
        else:
            name_after_underscore = filename

        job_id  = str(uuid.uuid4())
        now_iso = datetime.utcnow().isoformat()

        logger.info(
            "Processing S3 key %s → job_id %s (Name: %s)",
            input_key, job_id, name_after_underscore
        )

        table.put_item(Item={
            "JobId":        job_id,
            "InputKey":     input_key,
            "Name":         name_after_underscore,  # e.g. "GroupDiscussion"
            "OutputFormat": "mp4",                  # hard-coded format
            "Resolution":   "1280x720",             # hard-coded resolution
            "VideoCodec":   "libx264",              # hard-coded codec
            "Status":       "PENDING",
            "CreatedAt":    now_iso
        })

    return {"status": "OK"}

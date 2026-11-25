import unittest
from unittest.mock import patch

import boto3
from botocore.stub import Stubber

from analytics.export import upload_object


class ExportTest(unittest.TestCase):
    def test_s3_requires_explicit_bucket_creation(self):
        bucket = "cinema-archive"
        key = "daily/2001-01-10/aggregates.json"
        body = b'{"views": 2}'
        upload = {"Bucket": bucket, "Key": key, "Body": body, "ContentType": "application/json; charset=utf-8"}
        client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test")
        with Stubber(client) as s3, patch("analytics.export.boto3.session.Session") as session:
            session.return_value.client.return_value = client
            s3.add_client_error("put_object", "NoSuchBucket", http_status_code=404, expected_params=upload)
            s3.add_response("create_bucket", {}, {"Bucket": bucket})
            s3.add_response("put_object", {}, upload)
            upload_object(bucket, key, body)
            s3.assert_no_pending_responses()

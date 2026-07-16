"""S3 client for event file operations."""

import os
import json
import logging
from typing import Optional, Dict, Any, List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Client:
    """Handles S3 operations for event file management."""

    def __init__(self, bucket_name: Optional[str] = None, region: Optional[str] = None):
        self.bucket_name = bucket_name or os.environ.get("S3_BUCKET", "event-processing-bucket")
        self.region = region or os.environ.get("AWS_REGION", "us-east-1")
        self.s3 = boto3.client("s3", region_name=self.region)

    def create_bucket_if_not_exists(self):
        """Create the S3 bucket if it doesn't exist."""
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} already exists")
        except ClientError:
            logger.info(f"Creating bucket {self.bucket_name}")
            if self.region == "us-east-1":
                self.s3.create_bucket(Bucket=self.bucket_name)
            else:
                self.s3.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region},
                )
            logger.info(f"Bucket {self.bucket_name} created")

    def upload_event(self, key: str, event_data: Dict[str, Any]) -> str:
        """Upload event data as JSON to S3."""
        body = json.dumps(event_data, default=str)
        self.s3.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        logger.info(f"Uploaded event to s3://{self.bucket_name}/{key}")
        return f"s3://{self.bucket_name}/{key}"

    def download_event(self, key: str) -> Dict[str, Any]:
        """Download and parse event data from S3."""
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            body = response["Body"].read().decode("utf-8")
            return json.loads(body)
        except ClientError as e:
            logger.error(f"Error downloading from S3: {e}")
            raise

    def list_events(self, prefix: str = "events/", max_keys: int = 1000) -> List[str]:
        """List event files in the bucket."""
        response = self.s3.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix,
            MaxKeys=max_keys,
        )
        return [obj["Key"] for obj in response.get("Contents", [])]

    def delete_event(self, key: str):
        """Delete an event file from S3."""
        self.s3.delete_object(Bucket=self.bucket_name, Key=key)
        logger.info(f"Deleted s3://{self.bucket_name}/{key}")

    def configure_event_notification(self, lambda_arn: str):
        """Configure S3 bucket to trigger Lambda on object creation."""
        notification_config = {
            "LambdaFunctionConfigurations": [
                {
                    "LambdaFunctionArn": lambda_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [
                                {"Name": "prefix", "Value": "events/"},
                                {"Name": "suffix", "Value": ".json"},
                            ]
                        }
                    },
                }
            ]
        }
        self.s3.put_bucket_notification_configuration(
            Bucket=self.bucket_name,
            NotificationConfiguration=notification_config,
        )
        logger.info(f"Configured S3 event notification for {lambda_arn}")

    def parse_s3_event(self, s3_event: Dict[str, Any]) -> List[Dict[str, str]]:
        """Parse S3 event notification to extract bucket and key info."""
        records = []
        for record in s3_event.get("Records", []):
            s3_info = record.get("s3", {})
            records.append({
                "bucket": s3_info.get("bucket", {}).get("name", ""),
                "key": s3_info.get("object", {}).get("key", ""),
                "size": s3_info.get("object", {}).get("size", 0),
                "event_name": record.get("eventName", ""),
                "event_time": record.get("eventTime", ""),
            })
        return records

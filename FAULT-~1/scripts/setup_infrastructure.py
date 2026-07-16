"""
Infrastructure setup script for AWS resources.

Creates and configures:
- DynamoDB table with GSIs
- S3 bucket with event notifications
- CloudWatch dashboard and alarms
"""

import os
import json
import logging
import argparse

from src.utils.dynamo_client import DynamoDBClient
from src.utils.s3_client import S3Client
from src.utils.cloudwatch_client import CloudWatchClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def setup(region: str = "us-east-1", sns_topic_arn: str = None):
    """Set up all AWS infrastructure."""
    logger.info("Setting up infrastructure...")

    # DynamoDB
    dynamo = DynamoDBClient(region=region)
    dynamo.create_table_if_not_exists()
    logger.info("DynamoDB table ready")

    # S3
    s3 = S3Client(region=region)
    s3.create_bucket_if_not_exists()
    logger.info("S3 bucket ready")

    # CloudWatch
    cw = CloudWatchClient(region=region)
    cw.create_dashboard()
    logger.info("CloudWatch dashboard created")

    if sns_topic_arn:
        cw.create_alarms(sns_topic_arn)
        logger.info("CloudWatch alarms configured")

    logger.info("Infrastructure setup complete")


def teardown(region: str = "us-east-1"):
    """Tear down AWS infrastructure (for cleanup)."""
    import boto3

    logger.info("Tearing down infrastructure...")

    dynamodb = boto3.client("dynamodb", region_name=region)
    try:
        dynamodb.delete_table(TableName=os.environ.get("EVENTS_TABLE", "event-processing-table"))
        logger.info("DynamoDB table deleted")
    except Exception as e:
        logger.warning(f"Could not delete DynamoDB table: {e}")

    s3 = boto3.client("s3", region_name=region)
    bucket = os.environ.get("S3_BUCKET", "event-processing-bucket")
    try:
        # Delete all objects first
        response = s3.list_objects_v2(Bucket=bucket)
        for obj in response.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
        s3.delete_bucket(Bucket=bucket)
        logger.info("S3 bucket deleted")
    except Exception as e:
        logger.warning(f"Could not delete S3 bucket: {e}")

    cloudwatch = boto3.client("cloudwatch", region_name=region)
    for alarm_name in ["EventProcessing-HighErrorRate", "EventProcessing-HighLatency",
                       "EventProcessing-NoThroughput"]:
        try:
            cloudwatch.delete_alarms(AlarmNames=[alarm_name])
        except Exception:
            pass

    try:
        cloudwatch.delete_dashboards(DashboardNames=["EventProcessingDashboard"])
    except Exception:
        pass

    logger.info("Teardown complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage AWS infrastructure")
    parser.add_argument("action", choices=["setup", "teardown"], help="Action to perform")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--sns-topic-arn", help="SNS topic ARN for alarms")
    args = parser.parse_args()

    if args.action == "setup":
        setup(args.region, args.sns_topic_arn)
    else:
        teardown(args.region)

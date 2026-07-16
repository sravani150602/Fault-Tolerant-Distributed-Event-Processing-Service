"""CloudWatch client for monitoring, metrics, and alerting."""

import os
import time
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class CloudWatchClient:
    """
    Publishes custom metrics and manages CloudWatch dashboards and alarms
    for event processing monitoring.
    """

    NAMESPACE = "EventProcessingService"

    def __init__(self, region: Optional[str] = None):
        self.region = region or os.environ.get("AWS_REGION", "us-east-1")
        self.cloudwatch = boto3.client("cloudwatch", region_name=self.region)
        self.logs = boto3.client("logs", region_name=self.region)

    def put_processing_metric(self, metric_name: str, value: float, unit: str = "Count",
                               dimensions: Optional[List[Dict]] = None):
        """Publish a custom metric to CloudWatch."""
        metric_data = {
            "MetricName": metric_name,
            "Value": value,
            "Unit": unit,
            "Timestamp": datetime.utcnow(),
        }
        if dimensions:
            metric_data["Dimensions"] = dimensions

        self.cloudwatch.put_metric_data(
            Namespace=self.NAMESPACE,
            MetricData=[metric_data],
        )

    def record_event_processed(self, duration_ms: float, event_type: str, success: bool):
        """Record metrics for a processed event."""
        dimensions = [{"Name": "EventType", "Value": event_type}]

        self.put_processing_metric("EventsProcessed", 1, "Count", dimensions)
        self.put_processing_metric("ProcessingLatency", duration_ms, "Milliseconds", dimensions)

        if success:
            self.put_processing_metric("EventsSucceeded", 1, "Count", dimensions)
        else:
            self.put_processing_metric("EventsFailed", 1, "Count", dimensions)

    def record_deduplication(self, event_type: str):
        """Record a deduplication event."""
        dimensions = [{"Name": "EventType", "Value": event_type}]
        self.put_processing_metric("EventsDeduplicated", 1, "Count", dimensions)

    def record_retry(self, event_type: str, retry_count: int):
        """Record a retry attempt."""
        dimensions = [
            {"Name": "EventType", "Value": event_type},
            {"Name": "RetryAttempt", "Value": str(retry_count)},
        ]
        self.put_processing_metric("EventsRetried", 1, "Count", dimensions)

    def create_alarms(self, sns_topic_arn: str, thresholds: Optional[Dict] = None):
        """Create CloudWatch alarms with configurable thresholds."""
        defaults = {
            "error_rate_threshold": 5,
            "latency_threshold_ms": 500,
            "evaluation_periods": 3,
            "period_seconds": 300,
        }
        config = {**defaults, **(thresholds or {})}

        # High error rate alarm
        self.cloudwatch.put_metric_alarm(
            AlarmName="EventProcessing-HighErrorRate",
            AlarmDescription="Triggers when event processing error rate exceeds threshold",
            Namespace=self.NAMESPACE,
            MetricName="EventsFailed",
            Statistic="Sum",
            Period=config["period_seconds"],
            EvaluationPeriods=config["evaluation_periods"],
            Threshold=config["error_rate_threshold"],
            ComparisonOperator="GreaterThanOrEqualToThreshold",
            AlarmActions=[sns_topic_arn],
            TreatMissingData="notBreaching",
        )

        # High latency alarm
        self.cloudwatch.put_metric_alarm(
            AlarmName="EventProcessing-HighLatency",
            AlarmDescription="Triggers when processing latency exceeds threshold",
            Namespace=self.NAMESPACE,
            MetricName="ProcessingLatency",
            Statistic="p95",
            Period=config["period_seconds"],
            EvaluationPeriods=config["evaluation_periods"],
            Threshold=config["latency_threshold_ms"],
            ComparisonOperator="GreaterThanOrEqualToThreshold",
            AlarmActions=[sns_topic_arn],
            TreatMissingData="notBreaching",
        )

        # No events processed alarm (dead service)
        self.cloudwatch.put_metric_alarm(
            AlarmName="EventProcessing-NoThroughput",
            AlarmDescription="Triggers when no events are processed for extended period",
            Namespace=self.NAMESPACE,
            MetricName="EventsProcessed",
            Statistic="Sum",
            Period=config["period_seconds"],
            EvaluationPeriods=config["evaluation_periods"],
            Threshold=0,
            ComparisonOperator="LessThanOrEqualToThreshold",
            AlarmActions=[sns_topic_arn],
            TreatMissingData="breaching",
        )

        logger.info("CloudWatch alarms created successfully")

    def create_dashboard(self):
        """Create a CloudWatch dashboard for event processing metrics."""
        dashboard_body = {
            "widgets": [
                {
                    "type": "metric",
                    "x": 0, "y": 0, "width": 12, "height": 6,
                    "properties": {
                        "title": "Events Processed (Success vs Failed)",
                        "metrics": [
                            [self.NAMESPACE, "EventsSucceeded", {"stat": "Sum", "label": "Succeeded"}],
                            [self.NAMESPACE, "EventsFailed", {"stat": "Sum", "label": "Failed"}],
                        ],
                        "period": 60,
                        "view": "timeSeries",
                    },
                },
                {
                    "type": "metric",
                    "x": 12, "y": 0, "width": 12, "height": 6,
                    "properties": {
                        "title": "Processing Latency (ms)",
                        "metrics": [
                            [self.NAMESPACE, "ProcessingLatency", {"stat": "Average", "label": "Average"}],
                            [self.NAMESPACE, "ProcessingLatency", {"stat": "p95", "label": "p95"}],
                            [self.NAMESPACE, "ProcessingLatency", {"stat": "p99", "label": "p99"}],
                        ],
                        "period": 60,
                        "view": "timeSeries",
                    },
                },
                {
                    "type": "metric",
                    "x": 0, "y": 6, "width": 12, "height": 6,
                    "properties": {
                        "title": "Throughput (Events/min)",
                        "metrics": [
                            [self.NAMESPACE, "EventsProcessed", {"stat": "Sum", "label": "Total"}],
                        ],
                        "period": 60,
                        "view": "timeSeries",
                    },
                },
                {
                    "type": "metric",
                    "x": 12, "y": 6, "width": 12, "height": 6,
                    "properties": {
                        "title": "Deduplication & Retries",
                        "metrics": [
                            [self.NAMESPACE, "EventsDeduplicated", {"stat": "Sum", "label": "Deduplicated"}],
                            [self.NAMESPACE, "EventsRetried", {"stat": "Sum", "label": "Retried"}],
                        ],
                        "period": 60,
                        "view": "timeSeries",
                    },
                },
            ]
        }

        import json
        self.cloudwatch.put_dashboard(
            DashboardName="EventProcessingDashboard",
            DashboardBody=json.dumps(dashboard_body),
        )
        logger.info("CloudWatch dashboard created")

    def get_metrics_summary(self, minutes: int = 60) -> Dict[str, Any]:
        """Get a summary of recent metrics."""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=minutes)

        metrics = {}
        for metric_name in ["EventsProcessed", "EventsSucceeded", "EventsFailed",
                            "EventsDeduplicated", "EventsRetried"]:
            response = self.cloudwatch.get_metric_statistics(
                Namespace=self.NAMESPACE,
                MetricName=metric_name,
                StartTime=start_time,
                EndTime=end_time,
                Period=minutes * 60,
                Statistics=["Sum"],
            )
            datapoints = response.get("Datapoints", [])
            metrics[metric_name] = datapoints[0]["Sum"] if datapoints else 0

        # Get latency stats
        response = self.cloudwatch.get_metric_statistics(
            Namespace=self.NAMESPACE,
            MetricName="ProcessingLatency",
            StartTime=start_time,
            EndTime=end_time,
            Period=minutes * 60,
            Statistics=["Average", "Maximum"],
            ExtendedStatistics=["p95", "p99"],
        )
        datapoints = response.get("Datapoints", [])
        if datapoints:
            dp = datapoints[0]
            metrics["AvgLatencyMs"] = dp.get("Average", 0)
            metrics["MaxLatencyMs"] = dp.get("Maximum", 0)
            metrics["p95LatencyMs"] = dp.get("ExtendedStatistics", {}).get("p95", 0)
            metrics["p99LatencyMs"] = dp.get("ExtendedStatistics", {}).get("p99", 0)

        return metrics

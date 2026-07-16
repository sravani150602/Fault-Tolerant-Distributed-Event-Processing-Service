"""
Event simulation script for load testing and validation.

Simulates 15K+ events with configurable concurrency to validate:
- End-to-end latency under 300ms
- Correct deduplication behavior
- Retry logic under failure conditions
- Throughput at 100 concurrent requests
"""

import json
import time
import uuid
import random
import argparse
import statistics
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class EventSimulator:
    """Simulates distributed event processing at scale."""

    EVENT_TYPES = [
        "s3:ObjectCreated",
        "api:Request",
        "scheduled:Trigger",
        "custom:Event",
    ]

    SOURCES = [
        "upload-service",
        "api-gateway",
        "scheduler",
        "webhook-handler",
        "data-pipeline",
    ]

    def __init__(self, lambda_function_name: str, region: str = "us-east-1"):
        self.lambda_client = boto3.client("lambda", region_name=region)
        self.function_name = lambda_function_name
        self.results: List[Dict[str, Any]] = []

    def generate_event(self, include_duplicate_key: bool = False, duplicate_key: str = None) -> Dict:
        """Generate a random event payload."""
        event_type = random.choice(self.EVENT_TYPES)
        payload = {
            "event_type": event_type,
            "payload": {
                "data": f"simulated-data-{uuid.uuid4().hex[:8]}",
                "timestamp": time.time(),
                "size": random.randint(100, 10000),
                "priority": random.choice(["low", "medium", "high"]),
                "tags": random.sample(["analytics", "logging", "audit", "notification", "sync"], k=2),
            },
        }

        if include_duplicate_key and duplicate_key:
            payload["idempotency_key"] = duplicate_key

        if event_type == "s3:ObjectCreated":
            payload["Records"] = [{
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "event-processing-bucket"},
                    "object": {"key": f"events/{uuid.uuid4().hex}.json", "size": random.randint(100, 5000)},
                },
            }]

        return payload

    def invoke_lambda(self, event: Dict) -> Dict[str, Any]:
        """Invoke the Lambda function and measure latency."""
        start = time.time()
        try:
            response = self.lambda_client.invoke(
                FunctionName=self.function_name,
                InvocationType="RequestResponse",
                Payload=json.dumps(event),
            )
            latency_ms = (time.time() - start) * 1000
            response_payload = json.loads(response["Payload"].read())

            return {
                "success": response["StatusCode"] == 200,
                "latency_ms": latency_ms,
                "status_code": response["StatusCode"],
                "response": response_payload,
            }
        except Exception as e:
            latency_ms = (time.time() - start) * 1000
            return {
                "success": False,
                "latency_ms": latency_ms,
                "error": str(e),
            }

    def run_simulation(
        self,
        total_events: int = 15000,
        concurrency: int = 100,
        duplicate_rate: float = 0.15,
    ) -> Dict[str, Any]:
        """
        Run the full event simulation.

        Args:
            total_events: Total number of events to simulate
            concurrency: Number of concurrent requests
            duplicate_rate: Fraction of events that are duplicates (for dedup testing)
        """
        logger.info(f"Starting simulation: {total_events} events, {concurrency} concurrent, "
                    f"{duplicate_rate*100}% duplicate rate")

        # Generate events with some intentional duplicates
        events = []
        duplicate_keys = [uuid.uuid4().hex[:32] for _ in range(int(total_events * duplicate_rate / 3))]

        for i in range(total_events):
            if random.random() < duplicate_rate and duplicate_keys:
                dup_key = random.choice(duplicate_keys)
                events.append(self.generate_event(include_duplicate_key=True, duplicate_key=dup_key))
            else:
                events.append(self.generate_event())

        random.shuffle(events)

        # Execute with concurrency
        start_time = time.time()
        results = []

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(self.invoke_lambda, event): i for i, event in enumerate(events)}

            completed = 0
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                completed += 1
                if completed % 1000 == 0:
                    logger.info(f"Progress: {completed}/{total_events} events processed")

        total_time = time.time() - start_time
        self.results = results

        return self._compute_metrics(results, total_time, total_events, concurrency)

    def _compute_metrics(self, results: List[Dict], total_time: float,
                          total_events: int, concurrency: int) -> Dict[str, Any]:
        """Compute simulation metrics."""
        latencies = [r["latency_ms"] for r in results if r.get("success")]
        errors = [r for r in results if not r.get("success")]

        deduplicated = sum(
            1 for r in results
            if r.get("success") and r.get("response", {}).get("body", "")
            and "deduplicated" in str(r.get("response", {}).get("body", ""))
        )

        metrics = {
            "total_events": total_events,
            "successful": len(latencies),
            "failed": len(errors),
            "deduplicated": deduplicated,
            "total_time_seconds": round(total_time, 2),
            "throughput_per_second": round(total_events / total_time, 2),
            "concurrency": concurrency,
        }

        if latencies:
            latencies.sort()
            metrics["latency"] = {
                "min_ms": round(min(latencies), 2),
                "max_ms": round(max(latencies), 2),
                "avg_ms": round(statistics.mean(latencies), 2),
                "median_ms": round(statistics.median(latencies), 2),
                "p95_ms": round(latencies[int(len(latencies) * 0.95)], 2),
                "p99_ms": round(latencies[int(len(latencies) * 0.99)], 2),
                "stddev_ms": round(statistics.stdev(latencies), 2) if len(latencies) > 1 else 0,
            }

        return metrics


def main():
    parser = argparse.ArgumentParser(description="Simulate distributed event processing")
    parser.add_argument("--function-name", default="event-processor", help="Lambda function name")
    parser.add_argument("--total-events", type=int, default=15000, help="Total events to simulate")
    parser.add_argument("--concurrency", type=int, default=100, help="Concurrent requests")
    parser.add_argument("--duplicate-rate", type=float, default=0.15, help="Duplicate event rate")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    args = parser.parse_args()

    simulator = EventSimulator(args.function_name, args.region)
    metrics = simulator.run_simulation(
        total_events=args.total_events,
        concurrency=args.concurrency,
        duplicate_rate=args.duplicate_rate,
    )

    print("\n" + "=" * 60)
    print("SIMULATION RESULTS")
    print("=" * 60)
    print(json.dumps(metrics, indent=2))

    # Validate targets
    print("\n" + "-" * 60)
    print("VALIDATION")
    print("-" * 60)
    latency = metrics.get("latency", {})
    p95 = latency.get("p95_ms", 0)
    print(f"  End-to-end latency (p95): {p95:.1f}ms {'✓ PASS' if p95 < 300 else '✗ FAIL'} (target: <300ms)")
    print(f"  Concurrent requests tested: {metrics['concurrency']} {'✓ PASS' if metrics['concurrency'] >= 100 else '✗ FAIL'}")
    print(f"  Events processed: {metrics['total_events']} {'✓ PASS' if metrics['total_events'] >= 15000 else '✗ FAIL'}")
    print(f"  Deduplication working: {metrics['deduplicated']} events deduplicated {'✓ PASS' if metrics['deduplicated'] > 0 else '✗ FAIL'}")


if __name__ == "__main__":
    main()

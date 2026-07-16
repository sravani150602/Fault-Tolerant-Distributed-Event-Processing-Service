"""Unit tests for Lambda handler routing."""

import json
import pytest
from unittest.mock import patch, MagicMock

from src.handlers.lambda_handler import (
    lambda_handler,
    _is_s3_event,
    _is_api_gateway_event,
    _is_scheduled_event,
)


class TestEventDetection:
    def test_s3_event_detection(self):
        event = {"Records": [{"eventSource": "aws:s3"}]}
        assert _is_s3_event(event) is True

    def test_not_s3_event(self):
        assert _is_s3_event({}) is False
        assert _is_s3_event({"Records": []}) is False

    def test_api_gateway_event_detection(self):
        event = {"httpMethod": "POST", "path": "/events"}
        assert _is_api_gateway_event(event) is True

    def test_api_gateway_v2_detection(self):
        event = {"requestContext": {"httpMethod": "GET"}}
        assert _is_api_gateway_event(event) is True

    def test_not_api_gateway_event(self):
        assert _is_api_gateway_event({}) is False

    def test_scheduled_event_detection(self):
        event = {"source": "aws.events"}
        assert _is_scheduled_event(event) is True

    def test_not_scheduled_event(self):
        assert _is_scheduled_event({}) is False


class TestLambdaHandler:
    @patch("src.handlers.lambda_handler.processor")
    def test_api_post_event(self, mock_processor):
        mock_processor.process_event.return_value = {
            "status": "completed",
            "event_id": "test-123",
            "duration_ms": 50.0,
        }

        event = {
            "httpMethod": "POST",
            "path": "/events",
            "body": json.dumps({"event_type": "api:Request", "payload": {"data": "test"}}),
        }

        result = lambda_handler(event, None)
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["status"] == "completed"

    @patch("src.handlers.lambda_handler.processor")
    def test_api_post_invalid_json(self, mock_processor):
        event = {
            "httpMethod": "POST",
            "path": "/events",
            "body": "not json",
        }
        result = lambda_handler(event, None)
        assert result["statusCode"] == 400

    def test_health_check(self):
        event = {"httpMethod": "GET", "path": "/health"}
        result = lambda_handler(event, None)
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["status"] == "healthy"

    def test_not_found(self):
        event = {"httpMethod": "GET", "path": "/nonexistent"}
        result = lambda_handler(event, None)
        assert result["statusCode"] == 404

    @patch("src.handlers.lambda_handler.processor")
    def test_s3_event(self, mock_processor):
        mock_processor.process_event.return_value = {
            "status": "completed",
            "event_id": "test-123",
            "duration_ms": 100.0,
        }

        event = {
            "Records": [{
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "test-bucket"},
                    "object": {"key": "events/test.json", "size": 1024},
                },
            }]
        }

        result = lambda_handler(event, None)
        assert result["statusCode"] == 200
        mock_processor.process_event.assert_called_once()

    @patch("src.handlers.lambda_handler.processor")
    def test_direct_invocation(self, mock_processor):
        mock_processor.process_event.return_value = {
            "status": "completed",
            "event_id": "test-123",
            "duration_ms": 50.0,
        }

        event = {"event_type": "custom:Event", "payload": {"key": "value"}}
        result = lambda_handler(event, None)
        assert result["statusCode"] == 200

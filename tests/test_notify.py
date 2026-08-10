"""SNS/SQS notification tests against mocked AWS (moto). Owner: QA/Validation agent."""
import os, sys, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import boto3
from moto import mock_aws
from notify import publish_completion, setup_topic_and_queue

REGION = "us-east-1"

@mock_aws
def test_completion_event_reaches_sqs():
    topic_arn, queue_url = setup_topic_and_queue(region=REGION)
    msg = {"status": "SUCCESS", "rows": 12350000, "run_date": "2026-08-09"}
    mid = publish_completion(topic_arn, msg, region=REGION)
    assert mid

    sqs = boto3.client("sqs", region_name=REGION)
    resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    body = json.loads(resp["Messages"][0]["Body"])          # SNS envelope
    inner = json.loads(body["Message"])                     # our payload
    assert inner["status"] == "SUCCESS"
    assert inner["rows"] == 12350000

@mock_aws
def test_publish_returns_message_id():
    topic_arn, _ = setup_topic_and_queue(region=REGION)
    assert publish_completion(topic_arn, {"status": "SUCCESS"}, region=REGION)

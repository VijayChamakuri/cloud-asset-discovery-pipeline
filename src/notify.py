"""Event-driven completion notification via Amazon SNS -> SQS (boto3).

On pipeline success we publish a 'pipeline-complete' event to an SNS topic; a SQS
queue subscribed to the topic delivers it to downstream consumers (e.g. a security
review job). This is the real boto3 code path used in production; it is unit-tested
against mocked AWS with `moto` (see tests/test_notify.py).
Owner: Data Engineer agent."""
import json
import boto3


def publish_completion(topic_arn, message, region="us-east-1", sns=None):
    """Publish a completion event to an SNS topic. Returns the SNS MessageId."""
    sns = sns or boto3.client("sns", region_name=region)
    resp = sns.publish(
        TopicArn=topic_arn,
        Subject="asset-discovery-complete",
        Message=json.dumps(message),
        MessageAttributes={
            "pipeline": {"DataType": "String", "StringValue": "asset_discovery"},
            "status": {"DataType": "String", "StringValue": message.get("status", "SUCCESS")},
        },
    )
    return resp["MessageId"]


def setup_topic_and_queue(topic_name="asset-discovery-complete",
                          queue_name="asset-discovery-consumers",
                          region="us-east-1"):
    """Create an SNS topic and an SQS queue subscribed to it. Returns (topic_arn, queue_url)."""
    sns = boto3.client("sns", region_name=region)
    sqs = boto3.client("sqs", region_name=region)
    topic_arn = sns.create_topic(Name=topic_name)["TopicArn"]
    queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
    return topic_arn, queue_url


if __name__ == "__main__":
    # Local demo path (no AWS creds needed only under moto/tests). In production the
    # Airflow task passes a real TopicArn from the environment.
    print("[notify] publish_completion() ready; wire TopicArn via env in Airflow.")

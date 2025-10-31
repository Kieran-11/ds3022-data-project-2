from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import List, Tuple, Dict

import boto3
import requests
from botocore.exceptions import ClientError
from airflow.decorators import dag, task


# Initialize SQS client (relies on environment/instance role credentials)
sqs = boto3.client("sqs", region_name="us-east-1")


@dag(
    dag_id="sqs_message_pipeline_airflow",
    description="Collect 21 SQS messages, reassemble phrase, submit via SQS (platform=airflow)",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # Trigger manually
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["ds3022", "sqs", "airflow"],
)
def sqs_pipeline_airflow():
    @task()
    def populate_queue() -> str:
        """
        Task 1: Call API to populate SQS queue with 21 delayed messages.
        Returns the SQS Queue URL for subsequent tasks.
        """
        logger = logging.getLogger(__name__)
        url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/rrx5eg"

        logger.info("Calling API to populate queue: %s", url)
        try:
            response = requests.post(url, timeout=30)
            response.raise_for_status()
            payload: Dict[str, str] = response.json()
            sqs_url = payload.get("sqs_url")
            if not sqs_url:
                raise ValueError(f"Missing sqs_url in payload: {payload}")
            logger.info("Queue populated. SQS URL: %s | Payload: %s", sqs_url, payload)
            return sqs_url
        except requests.RequestException as exc:
            logger.exception("Failed to populate queue")
            raise exc

    @task()
    def collect_all_messages(queue_url: str) -> List[Tuple[int, str]]:
        """
        Task 2: Continuously poll the SQS queue until all 21 messages are collected.
        Returns a list of tuples (order_no, word).
        """
        logger = logging.getLogger(__name__)

        messages: Dict[int, str] = {}
        max_messages = 21
        poll_interval_seconds = 15  # spacing between polls
        max_wait_seconds = 1000  # up to ~16 minutes to cover max 900s delay

        logger.info("Starting message collection from queue: %s", queue_url)
        start_time = time.time()

        while len(messages) < max_messages:
            elapsed = time.time() - start_time
            if elapsed > max_wait_seconds:
                logger.warning(
                    "Timeout reached. Collected %s/%s messages", len(messages), max_messages
                )
                break

            try:
                attrs = sqs.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=[
                        "ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible",
                        "ApproximateNumberOfMessagesDelayed",
                    ],
                )
                attr_values = attrs.get("Attributes", {})
                num_visible = int(attr_values.get("ApproximateNumberOfMessages", 0))
                num_invisible = int(attr_values.get("ApproximateNumberOfMessagesNotVisible", 0))
                num_delayed = int(attr_values.get("ApproximateNumberOfMessagesDelayed", 0))
                total = num_visible + num_invisible + num_delayed

                logger.info(
                    "Queue status: %s visible, %s invisible, %s delayed, %s total. Collected: %s/%s",
                    num_visible,
                    num_invisible,
                    num_delayed,
                    total,
                    len(messages),
                    max_messages,
                )

                if num_visible > 0:
                    recv = sqs.receive_message(
                        QueueUrl=queue_url,
                        MaxNumberOfMessages=10,
                        MessageAttributeNames=["All"],
                        WaitTimeSeconds=1,
                    )
                    for msg in recv.get("Messages", []):
                        receipt_handle = msg["ReceiptHandle"]
                        try:
                            order_no = int(
                                msg["MessageAttributes"]["order_no"]["StringValue"]
                            )
                            word = msg["MessageAttributes"]["word"]["StringValue"]
                            messages[order_no] = word
                            logger.info("Received message %s/%s: '%s'", order_no, max_messages, word)
                        except (KeyError, ValueError) as parse_exc:
                            logger.error("Error parsing message: %s", parse_exc)
                        finally:
                            # Always delete to avoid reprocessing bad messages
                            try:
                                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                            except ClientError as del_exc:
                                logger.error("Delete failed for message: %s", del_exc)

            except ClientError as aws_exc:
                logger.error("AWS error during polling: %s", aws_exc)
            except Exception as exc:
                logger.error("Unexpected error during message collection: %s", exc)

            if len(messages) < max_messages:
                time.sleep(poll_interval_seconds)

        result = sorted(messages.items())
        logger.info("Collection complete: %s/%s messages", len(result), max_messages)
        return result

    @task()
    def reassemble_and_submit(message_tuples: List[Tuple[int, str]]) -> bool:
        """
        Task 3: Reassemble messages by order and submit to submission queue with platform='airflow'.
        """
        logger = logging.getLogger(__name__)

        try:
            words = [w for _, w in sorted(message_tuples, key=lambda t: t[0])]
            phrase = " ".join(words)
            logger.info("Reassembled phrase (%s words): %s", len(words), phrase)

            submission_queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
            response = sqs.send_message(
                QueueUrl=submission_queue_url,
                MessageBody="Submission from rrx5eg",
                MessageAttributes={
                    "uvaid": {"DataType": "String", "StringValue": "rrx5eg"},
                    "phrase": {"DataType": "String", "StringValue": phrase},
                    "platform": {"DataType": "String", "StringValue": "airflow"},
                },
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            logger.info("Submission response: %s", response)
            logger.info("HTTP Status Code: %s", status)
            if status != 200:
                raise RuntimeError(f"Submission returned non-200 status: {status}")
            return True

        except ClientError as aws_exc:
            logger.error("AWS error during submission: %s", aws_exc)
            raise
        except Exception as exc:
            logger.error("Error during reassembly/submission: %s", exc)
            raise

    q_url = populate_queue()
    msgs = collect_all_messages(q_url)
    reassemble_and_submit(msgs)


sqs_pipeline_airflow()
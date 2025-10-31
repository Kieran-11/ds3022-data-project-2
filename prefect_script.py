from prefect import flow, task, get_run_logger
from typing import Dict, List, Tuple
import boto3
import requests
import time
from botocore.exceptions import ClientError


# Initialize SQS client
sqs = boto3.client('sqs', region_name='us-east-1')


@task
def populate_queue() -> str:
    """
    Task 1: Call API to populate SQS queue with 21 messages.
    Returns the SQS queue URL.
    """
    logger = get_run_logger()
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/rrx5eg"
    
    try:
        logger.info(f"Calling API to populate queue: {url}")
        response = requests.post(url)
        response.raise_for_status()
        
        payload = response.json()
        sqs_url = payload.get('sqs_url')
        
        logger.info(f"Queue populated successfully. SQS URL: {sqs_url}")
        logger.info(f"Response: {payload}")
        
        return sqs_url
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to populate queue: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during queue population: {e}")
        raise


@task
def collect_all_messages(queue_url: str) -> List[Tuple[int, str]]:
    """
    Task 2: Continuously poll SQS queue until all 21 messages are collected.
    Returns a list of tuples (order_no, word).
    """
    logger = get_run_logger()
    messages = {}
    max_messages = 21
    poll_interval = 15  # seconds between polls
    max_wait_time = 1000  # seconds (to handle max delay of 900s)
    
    logger.info("Starting message collection from queue")
    start_time = time.time()
    
    while len(messages) < max_messages:
        elapsed_time = time.time() - start_time
        
        if elapsed_time > max_wait_time:
            logger.warning(f"Timeout reached. Collected {len(messages)}/{max_messages} messages")
            break
        
        try:
            # Get queue attributes to monitor message status
            attributes_response = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages', 
                               'ApproximateNumberOfMessagesNotVisible',
                               'ApproximateNumberOfMessagesDelayed']
            )
            
            num_visible = int(attributes_response['Attributes'].get('ApproximateNumberOfMessages', 0))
            num_invisible = int(attributes_response['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))
            num_delayed = int(attributes_response['Attributes'].get('ApproximateNumberOfMessagesDelayed', 0))
            total = num_visible + num_invisible + num_delayed
            
            logger.info(f"Queue status: {num_visible} visible, {num_invisible} invisible, {num_delayed} delayed, {total} total. Collected: {len(messages)}/{max_messages}")
            
            # Try to receive messages
            if num_visible > 0:
                receive_response = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,  # AWS allows max 10 per request
                    MessageAttributeNames=['All'],
                    WaitTimeSeconds=1
                )
                
                # Check if messages were received
                if 'Messages' in receive_response:
                    for msg in receive_response['Messages']:
                        receipt_handle = msg['ReceiptHandle']
                        
                        try:
                            # Extract order_no and word from message attributes
                            order_no = int(msg['MessageAttributes']['order_no']['StringValue'])
                            word = msg['MessageAttributes']['word']['StringValue']
                            
                            # Store the message
                            messages[order_no] = word
                            logger.info(f"Received message {order_no}/{max_messages}: '{word}'")
                            
                            # Delete the message from queue
                            sqs.delete_message(
                                QueueUrl=queue_url,
                                ReceiptHandle=receipt_handle
                            )
                            
                        except (KeyError, ValueError) as e:
                            logger.error(f"Error parsing message: {e}")
                            # Still delete to avoid leaving bad messages in queue
                            sqs.delete_message(
                                QueueUrl=queue_url,
                                ReceiptHandle=receipt_handle
                            )
                
        except ClientError as e:
            logger.error(f"AWS error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during message collection: {e}")
        
        # Wait before next poll (except if we already have all messages)
        if len(messages) < max_messages:
            time.sleep(poll_interval)
    
    logger.info(f"Collection complete. Total messages: {len(messages)}/{max_messages}")
    
    # Convert dict to list of tuples sorted by order_no
    result = sorted(messages.items())
    return result


@task
def reassemble_and_submit(message_tuples: List[Tuple[int, str]]) -> bool:
    """
    Task 3: Reassemble messages in order and submit to submission queue.
    Returns True if submission was successful.
    """
    logger = get_run_logger()
    
    try:
        # Sort by order_no (already sorted from collect_all_messages)
        # Extract just the words in order
        words = [word for _, word in message_tuples]
        phrase = ' '.join(words)
        
        logger.info(f"Reassembled phrase ({len(words)} words): {phrase}")
        
        # Prepare submission
        submission_queue_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
        
        response = sqs.send_message(
            QueueUrl=submission_queue_url,
            MessageBody=f"Submission from rrx5eg",
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': 'rrx5eg'
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': 'prefect'
                }
            }
        )
        
        logger.info(f"Submission successful. Response: {response}")
        logger.info(f"HTTP Response Code: {response['ResponseMetadata']['HTTPStatusCode']}")
        
        return True
        
    except ClientError as e:
        logger.error(f"AWS error during submission: {e}")
        raise
    except Exception as e:
        logger.error(f"Error during reassembly/submission: {e}")
        raise


@flow(name="sqs_message_pipeline")
def sqs_message_pipeline():
    """
    Main Prefect flow that orchestrates the SQS message pipeline.
    """
    logger = get_run_logger()
    logger.info("Starting SQS Message Pipeline")
    
    # Task 1: Populate queue
    queue_url = populate_queue()
    
    # Task 2: Collect all messages
    messages = collect_all_messages(queue_url)
    
    # Task 3: Reassemble and submit
    success = reassemble_and_submit(messages)
    
    logger.info("Pipeline completed successfully")
    return success


if __name__ == "__main__":
    sqs_message_pipeline()

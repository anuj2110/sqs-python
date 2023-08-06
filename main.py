from dotenv import load_dotenv
import os
import logging
import boto3
from botocore.exceptions import ClientError
from pprint import pprint

load_dotenv('.env')
logger = logging.getLogger(__name__)


def create_queue(queueu_name):
    sqs = boto3.resource('sqs', region_name=os.getenv("REGION"), aws_access_key_id=os.getenv(
        "ACCESS_KEY"), aws_secret_access_key=os.getenv("SECRET_KEY"))
    queue = sqs.create_queue(QueueName=queueu_name)
    logger.info(f'Created queue {queueu_name} with url={queue.url}')
    return queue


def send_message(queue, message_body):
    try:
        response = queue.send_message(MessageBody=message_body)
    except ClientError as error:
        logger.exception("Send message failed ", message_body)
        raise error
    else:
        return response


def receive_message(queue, queue_url, max_num_messages):
    response = queue.receive_messages(MaxNumberOfMessages=max_num_messages)
    for msg in response:
        pprint(msg.body)


if __name__=="__main__":
    queue = create_queue('my_first_queue')
    # print(queue)
    response = send_message(queue,'{"name":"Anuj2"}')
    # print(response)
    receive_message(queue,queue.url,2)
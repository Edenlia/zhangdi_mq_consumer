import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from datetime import datetime

sqs_url = 'https://sqs.ap-northeast-1.amazonaws.com/562819648096/zhangdi-recommend'

sqs = boto3.client('sqs')
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id='AKIAYGCVKJJQBNZKNJOO',
    aws_secret_access_key='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'
)


def send_stay_message2Dynamo(user_id, book_id, time):
    try:
        response = dynamodb.Table('zhangdi-recommend-train').put_item(
           Item={
                'train_record_id': '10',
                'create_time': datetime.now().strftime("%d/%m/%Y %H:%M"),
                'book_id': book_id,
                'time': time,
                'type': 'stay',
                'user_id': user_id
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response


def send_click_message2Dynamo(user_id, book_id):
    try:
        response = dynamodb.Table('zhangdi-recommend-train').put_item(
           Item={
                'train_record_id': '20',
                'create_time': datetime.now().strftime("%d/%m/%Y %H:%M"),
                'book_id': book_id,
                'type': 'click',
                'user_id': user_id
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response


def split_message(message):
    items = message.split(',')
    if not items[0].isdigit() or not items[1].isdigit() or not items[3].isdigit():
        return 'stay', [0, 0, 0]
    else:
        if items[2] == 'stay':
            return 'stay', [int(items[0]), int(items[1]), int(items[3])]
        else:
            return 'click', [int(items[0]), int(items[1])]


def delete_message(receipt_handle):
    response = sqs.delete_message(
        QueueUrl=sqs_url,
        ReceiptHandle=receipt_handle,
    )
    print(response)


def receive_message():
    response = sqs.receive_message(
        QueueUrl=sqs_url,
        MaxNumberOfMessages=5,
        WaitTimeSeconds=10,
        MessageAttributeNames=['All']
    )

    print(f"Number of messages received: {len(response.get('Messages', []))}")

    for message in response.get("Messages", []):
        message_body = message["Body"]
        # print(f"Message body: {message_body}")
        # print(f"Receipt Handle: {message['ReceiptHandle']}")
        ty, ml = split_message(message_body)
        print("type: " + ty)
        print(ml)
        if ty == 'stay':
            send_stay_message2Dynamo(ml[0], ml[1], ml[2])
        else:
            send_click_message2Dynamo(ml[0], ml[1])
        delete_message(message['ReceiptHandle'])


def get_message_from_Dynamo():
    try:
        response = dynamodb.Table('zhangdi-recommend-train').scan(
            FilterExpression=Attr('type').eq("stay_time")
        )
        for i in response['Items']:
            print(i)
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']


def get_all_message_from_Dynamo():
    try:
        response = dynamodb.Table('zhangdi-recommend-train').scan()
        print(len(response['Items']))
        for i in response['Items']:
            print(i)
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']

# receive_message()
# get_message_from_Dynamo()
# send_message2Dynamo(19301022, 195153448, "stay_time", 43)
# get_message_from_Dynamo()

while True:
    receive_message()
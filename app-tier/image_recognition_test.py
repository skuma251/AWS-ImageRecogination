import boto3
from dotenv import load_dotenv
import os
import subprocess
import json

load_dotenv()
AWS_INPUT_BUCKET_NAME=os.getenv('AWS_INPUT_BUCKET_NAME')
AWS_OUTPUT_BUCKET_NAME=os.getenv('AWS_OUTPUT_BUCKET_NAME')
AWS_ACCESS_KEY=os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY=os.getenv('AWS_SECRET_KEY')
AWS_SQS_REGION=os.getenv('AWS_SQS_REGION')
AWS_SENDER_QUEUE_URL=os.getenv('AWS_SENDER_QUEUE_URL')
AWS_ACCOUNT_ID=os.getenv('AWS_ACCOUNT_ID')

def get_queue_url(queue_name):
    sqs_client = boto3.client('sqs', region_name=AWS_SQS_REGION,
                          aws_access_key_id=AWS_ACCESS_KEY,
                          aws_secret_access_key=AWS_SECRET_KEY)
    queue_name = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    return queue_name["QueueUrl"]

inputSQS = get_queue_url("InputQueue")
outputSQS = get_queue_url("OutputQueue")

def read_message_sqs(queueUrl):
    sqs_client = boto3.client('sqs', region_name="us-east-1",
                          aws_access_key_id=AWS_ACCESS_KEY,
                          aws_secret_access_key=AWS_SECRET_KEY)

    messages = sqs_client.receive_message(QueueUrl=queueUrl, MaxNumberOfMessages=10
                                        ,WaitTimeSeconds=10,MessageAttributeNames=['All'])

    if messages['Messages'] != None:
        return messages['Messages']
    else:
        print("InputQueue SQS is empty, no message to read")
        return None

def download_image_S3(image_name):
    s3_resource = boto3.resource('s3', region_name="us-east-1",
                        aws_access_key_id=AWS_ACCESS_KEY,
                        aws_secret_access_key=AWS_SECRET_KEY)
    file_name = '/home/ubuntu/images/' + image_name
    s3_resource.meta.client.download_file(AWS_INPUT_BUCKET_NAME, image_name, file_name)

def run_image_classification():
    dir_path = '/home/ubuntu/images/'
    for path in os.listdir(dir_path):
        path = dir_path + str(path)
        filename = path.rsplit(".", 1)[0]
        filename = filename.rsplit("/")[4]
        filename = '/home/ubuntu/result/' + str(filename) + '.txt'
        subprocess.run(['touch', filename])
        output_file = open(filename, "w")
        subprocess.run(('python3', './image_classification.py', path), stdout=output_file)


def write_message_sqs(queue_url, image_name):
    path = '/home/ubuntu/images/' + str(image_name)
    file_name = path.rsplit(".", 1)[0]
    file_name = file_name.rsplit("/")[4]
    file_name = '/home/ubuntu/result/' + str(file_name) + '.txt'
    with open(file_name, 'r') as f:
        lines = f.readline()
    lines = lines.split("\n")
    message_body = lines[0].split(",")[1]
    sqs_message = {image_name: message_body}

    sqs_client = boto3.client('sqs', region_name="us-east-1",
                              aws_access_key_id=AWS_ACCESS_KEY,
                              aws_secret_access_key=AWS_SECRET_KEY)

    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(sqs_message)
    )
    print("Writing message to response SQS")
    print(response['ResponseMetadata']["HTTPStatusCode"])

def write_to_bucket_s3(image_name):
    path = '/home/ubuntu/images/' + str(image_name)
    file_name = path.rsplit(".", 1)[0]
    file_name = file_name.rsplit("/")[4]
    result_file = '/home/ubuntu/result/' + str(file_name) + '.txt'
    with open(result_file, 'r') as f:
        lines = f.readline()
    lines = lines.split("\n")
    #test_0, bathtub
    message_body = file_name + "," + lines[0].split(",")[1]
    #s3_client = boto3.client("s3")
    s3_client = boto3.client('s3', region_name="us-east-1",
                              aws_access_key_id=AWS_ACCESS_KEY,
                              aws_secret_access_key=AWS_SECRET_KEY)

    #key : test_0, value:(test0, bathtub)
    s3_client.put_object(Bucket=AWS_OUTPUT_BUCKET_NAME, Body=message_body, Key=file_name)

def delete_message_sqs(queueUrl, receipthandle):
    sqs_client = boto3.client('sqs', region_name="us-east-1",
                              aws_access_key_id=AWS_ACCESS_KEY,
                              aws_secret_access_key=AWS_SECRET_KEY)
    response = sqs_client.delete_message(
        QueueUrl=queueUrl,
        ReceiptHandle=receipthandle,
    )
    print("Deleting message from request SQS")
    print(response["ResponseMetadata"]["HTTPStatusCode"])

def delete_local_image(image_name):
    file_path = "/home/ubuntu/images/" + image_name
    if os.path.exists(file_path):
        os.remove(file_path)
    else:
        print("The file does not exist")

def run_process():
    messages = read_message_sqs(inputSQS)
    for message in messages:
        image_name =  message['Body']
        receipthandle = message['ReceiptHandle']
        if image_name != None and receipthandle != None:
            download_image_S3(image_name)
            run_image_classification()
            write_message_sqs(outputSQS, image_name)
            write_to_bucket_s3(image_name)
            delete_message_sqs(inputSQS, receipthandle)
            delete_local_image(image_name)
        else:
            exit(1)
    exit(0)

if __name__ == "__main__":
    run_process()
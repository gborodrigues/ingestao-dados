#!/bin/bash

# Read inputs
read -p "Enter the account aws number: " ACCOUNT
read -p "Enter the S3 Bucket name to host the lambda code: " S3_BUCKET_NAME_CODE
read -p "Enter the S3 Bucket Raw name: " S3_BUCKET_NAME
read -p "Enter the SQS Queue name: " SQS_QUEUE_NAME

# Ensure `TEMPLATE_FILE` is defined
TEMPLATE_FILE="template.yaml"

# Install dependencies
pip3 install --target ./producer -r producer/requirements.txt

# Check if producer.zip exists and delete it
if [ -f producer.zip ]; then
    echo "producer.zip exists, deleting it..."
    rm producer.zip
fi

# Zip the producer directory
zip -r9 producer.zip producer

# Create the CloudFormation YAML template
cat <<EOL > $TEMPLATE_FILE
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Resources:
  RawLayer:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: producer
      Handler: script.handler
      Runtime: python3.10
      CodeUri: producer/
      MemorySize: 128
      Role: arn:aws:iam::$ACCOUNT:role/LabRole
      Timeout: 15
      Environment: 
        Variables:
          BUCKET_NAME: $S3_BUCKET_NAME
          SQS_URL: https://sqs.us-east-1.amazonaws.com/$ACCOUNT/$SQS_QUEUE_NAME
      Events:
        MyScheduledEvent:
          Type: Schedule
          Properties:
            Schedule: cron(0 12 * * ? *)
EOL

# Package the CloudFormation template
echo "Packaging the CloudFormation template..."
aws cloudformation package \
    --template-file $TEMPLATE_FILE \
    --s3-bucket $S3_BUCKET_NAME_CODE \
    --output-template-file packaged-template.yaml

# Deploy the CloudFormation stack
echo "Deploying Lambda..."
aws cloudformation deploy \
    --template-file packaged-template.yaml \
    --stack-name producer-stack \
    --capabilities CAPABILITY_IAM
#!/bin/bash

# Function to generate a random hash for the stack name
generate_stack_name() {
  echo "Exercicio7Stack-$(date +%s%N | md5sum | head -c 8)"
}

# Ask for the S3 bucket and SQS queue names
read -p "Enter the S3 Bucket name: " S3_BUCKET_NAME
read -p "Enter the SQS Queue name: " SQS_QUEUE_NAME

# Define the CloudFormation template file path
TEMPLATE_FILE="infra_template.yaml"

# Create the CloudFormation YAML template
cat <<EOL > $TEMPLATE_FILE
AWSTemplateFormatVersion: '2010-09-09'
Description: CloudFormation template to create an S3 bucket and an SQS queue

Resources:
  RawS3Bucket:
    Type: 'AWS::S3::Bucket'
    Properties:
      BucketName: $S3_BUCKET_NAME
      VersioningConfiguration:
        Status: Enabled

  ProcessSQSQueue:
    Type: 'AWS::SQS::Queue'
    Properties:
      QueueName: $SQS_QUEUE_NAME

Outputs:
  S3BucketName:
    Description: "Name of the S3 bucket"
    Value: !Ref RawS3Bucket

  SQSQueueURL:
    Description: "URL of the SQS queue"
    Value: !Ref ProcessSQSQueue

  SQSQueueArn:
    Description: "ARN of the SQS queue"
    Value: !GetAtt ProcessSQSQueue.Arn
EOL

# Generate a random stack name
STACK_NAME=$(generate_stack_name)
echo "Generated stack name: $STACK_NAME"

# Run the CloudFormation create-stack command
echo "Creating CloudFormation stack..."
aws cloudformation create-stack \
  --stack-name "$STACK_NAME" \
  --template-body "file://$TEMPLATE_FILE" \
  --capabilities CAPABILITY_NAMED_IAM

# Check if the command was successful
if [ $? -eq 0 ]; then
  echo "CloudFormation stack creation initiated successfully."
  echo "Stack name: $STACK_NAME"
else
  echo "Error initiating CloudFormation stack creation."
fi
#!/bin/bash

generate_stack_name() {
  echo "Exercicio7Stack-$(date +%s%N | md5sum | head -c 8)"
}

read -p "Enter the S3 Bucket name: " S3_BUCKET_NAME
read -p "Enter the SQS Queue name: " SQS_QUEUE_NAME
read -p "Enter the S3 Bucket name to host the lambda code: " S3_BUCKET_NAME_CODE

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

  CodeS3Bucket:
    Type: 'AWS::S3::Bucket'
    Properties:
      BucketName: $S3_BUCKET_NAME_CODE

  ProcessSQSQueue:
    Type: 'AWS::SQS::Queue'
    Properties:
      QueueName: $SQS_QUEUE_NAME

Outputs:
  S3BucketName:
    Description: "Name of the S3 bucket"
    Value: !Ref RawS3Bucket

  S3BucketName:
    Description: "Name of the S3 bucket"
    Value: !Ref CodeS3Bucket

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

echo "Creating CloudFormation stack..."
aws cloudformation create-stack \
  --stack-name "$STACK_NAME" \
  --template-body "file://$TEMPLATE_FILE" \
  --capabilities CAPABILITY_NAMED_IAM

if [ $? -eq 0 ]; then
  echo "CloudFormation stack creation initiated successfully. Waiting for stack to complete..."

  # Wait for stack creation to complete
  aws cloudformation wait stack-create-complete --stack-name "$STACK_NAME"

  if [ $? -eq 0 ]; then
    echo "Stack creation completed successfully."
    
    S3_BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query "Stacks[0].Outputs[?OutputKey=='S3BucketName'].OutputValue" --output text)
    echo "S3 Bucket created: $S3_BUCKET_NAME"

    echo "Uploading local folders to S3 bucket: $S3_BUCKET_NAME"

    FOLDER1="data_sources/Bancos"
    FOLDER2="data_sources/Empregados"
    FOLDER3="data_sources/Reclamacoes"

    upload_to_s3() {
      local folder_path=$1
      local s3_path=$2

      echo "Uploading $folder_path to s3://$S3_BUCKET_NAME/$s3_path"
      
      aws s3 sync "$folder_path" "s3://$S3_BUCKET_NAME/$s3_path"

      if [ $? -eq 0 ]; then
        echo "Upload of $folder_path to s3://$S3_BUCKET_NAME/$s3_path completed successfully."
      else
        echo "Failed to upload $folder_path to s3://$S3_BUCKET_NAME/$s3_path."
      fi
    }

    upload_to_s3 "$FOLDER1" "Bancos"
    upload_to_s3 "$FOLDER2" "Empregados"
    upload_to_s3 "$FOLDER3" "Reclamacoes"

  else
    echo "Error: Stack creation failed."
  fi
else
  echo "Error: CloudFormation stack creation initiation failed."
fi

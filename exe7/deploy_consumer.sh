#!/bin/bash

# Define paths to .env file and CloudFormation template
ENV_FILE="consumer/.env"
TEMPLATE_FILE="template-consumer.yaml"

# Check if the .env file exists
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: .env file not found!"
  exit 1
fi

# Export environment variables from .env file
export $(grep -v '^#' "$ENV_FILE" | xargs)

# Verify the environment variables are set correctly (for debugging)
echo "BUCKET_NAME: $BUCKET_NAME"
echo "SQS_URL: $SQS_URL"
echo "OUTPUT_BUCKET_NAME: $OUTPUT_BUCKET_NAME"
echo "OUTPUT_FILE_NAME: $OUTPUT_FILE_NAME"
echo "DB_USER: $DB_USER"
echo "DB_PASSWORD: $DB_PASSWORD"
echo "DB_NAME: $DB_NAME"
echo "DB_TABLE_NAME: $DB_TABLE_NAME"
echo "DB_IDENTIFIER: $DB_IDENTIFIER"
echo "SG_NAME: $SG_NAME"
echo "IAM: $IAM"

# Generate the CloudFormation template with placeholders for environment variables
cat > "$TEMPLATE_FILE" <<EOL
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Resources:
  RawLayer:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: consumer
      Handler: script.handler
      Runtime: python3.10
      CodeUri: consumer/
      MemorySize: 128
      Role: arn:aws:iam::${IAM}:role/LabRole
      Timeout: 15
      Environment: 
        Variables:
          BUCKET_NAME: ${BUCKET_NAME}
          SQS_URL: ${SQS_URL}
          OUTPUT_BUCKET_NAME: ${OUTPUT_BUCKET_NAME}
          OUTPUT_FILE_NAME: ${OUTPUT_FILE_NAME}
          DB_USER: ${DB_USER}
          DB_PASSWORD: ${DB_PASSWORD}
          DB_NAME: ${DB_NAME}
          DB_TABLE_NAME: ${DB_TABLE_NAME}
          DB_IDENTIFIER: ${DB_IDENTIFIER}
          SG_NAME: ${SG_NAME}
          IAM: ${IAM}
      Events:
        MySQSEvent:
          Type: SQS
          Properties:
            Queue: ${SQS_URL}
            BatchSize: 10
EOL

echo "Generated $TEMPLATE_FILE with environment variables from $ENV_FILE"

pip3 install --target ./consumer -r consumer/requirements.txt

if [ -f consumer.zip ]; then
    echo "consumer.zip exists, deleting it..."
    rm consumer.zip
fi

if [ -d consumer ]; then
    zip -r9 consumer.zip consumer
else
    echo "Error: 'consumer' directory or file not found!"
    exit 1
fi

echo "Packaging the CloudFormation template..."
aws cloudformation package \
    --template-file "$TEMPLATE_FILE" \
    --s3-bucket "$BUCKET_NAME" \
    --output-template-file packaged-template-consumer.yaml

echo "Deploying Lambda..."
aws cloudformation deploy \
    --template-file packaged-template-consumer.yaml \
    --stack-name consumer-stack \
    --capabilities CAPABILITY_IAM

echo "Deployment completed."

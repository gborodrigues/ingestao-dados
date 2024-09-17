#!/bin/bash

empty_s3_bucket() {
  local bucket_name=$1

  echo "Emptying S3 bucket: $bucket_name"
  aws s3 rm s3://"$bucket_name" --recursive

  if [ $? -eq 0 ]; then
    echo "S3 bucket emptied successfully."
  else
    echo "Failed to empty the S3 bucket. Please check the bucket and try again."
    exit 1
  fi
}

delete_stack() {
  local stack_name=$1
  local bucket_name=$2

  if [ -n "$bucket_name" ]; then
    empty_s3_bucket "$bucket_name"
  fi

  echo "Deleting CloudFormation stack: $stack_name"
  aws cloudformation delete-stack --stack-name "$stack_name"

  if [ $? -eq 0 ]; then
    echo "Stack deletion initiated successfully. Monitoring stack deletion..."

    aws cloudformation wait stack-delete-complete --stack-name "$stack_name"

    if [ $? -eq 0 ]; then
      echo "Stack deleted successfully."
    else
      echo "Failed to delete the stack. Please check the AWS CloudFormation console for more details."
    fi
  else
    echo "Error initiating stack deletion. Please check the stack name and try again."
  fi
}

if [ -f "last_stack_name.txt" ]; then
  read -p "Use the last created stack name found in last_stack_name.txt (y/n)? " use_last_stack
  if [[ $use_last_stack == "y" || $use_last_stack == "Y" ]]; then
    STACK_NAME=$(cat last_stack_name.txt)
  else
    read -p "Enter the CloudFormation stack name to delete: " STACK_NAME
  fi
else
  read -p "Enter the CloudFormation stack name to delete: " STACK_NAME
fi

BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --query "Stacks[0].Outputs[?OutputKey=='S3BucketName'].OutputValue" --output text)

delete_stack "$STACK_NAME" "$BUCKET_NAME"
#!/bin/bash

# Function to delete the CloudFormation stack
delete_stack() {
  local stack_name=$1

  # Run the AWS CLI command to delete the stack
  echo "Deleting CloudFormation stack: $stack_name"
  aws cloudformation delete-stack --stack-name "$stack_name"

  # Check if the command was successful
  if [ $? -eq 0 ]; then
    echo "Stack deletion initiated successfully. Monitoring stack deletion..."

    # Wait for the stack deletion to complete
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

# Ask for the stack name (or read from a stored file if applicable)
if [ -f "last_stack_name.txt" ]; then
  # If the stack name is stored in a file, offer to use it
  read -p "Use the last created stack name found in last_stack_name.txt (y/n)? " use_last_stack
  if [[ $use_last_stack == "y" || $use_last_stack == "Y" ]]; then
    STACK_NAME=$(cat last_stack_name.txt)
  else
    read -p "Enter the CloudFormation stack name to delete: " STACK_NAME
  fi
else
  # If no file exists, prompt the user for the stack name
  read -p "Enter the CloudFormation stack name to delete: " STACK_NAME
fi

# Delete the stack
delete_stack "$STACK_NAME"
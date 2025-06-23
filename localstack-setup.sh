#!/bin/sh
echo "Initializing localstack sqs"

awslocal sqs create-queue --queue-name test-queue
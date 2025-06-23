package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func main() {
	sqsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint("http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	pubClient := sqs.NewFromConfig(sqsCfg)
	go func(pub *sqs.Client) {
		// Example of publishing a message to a queue
		queue, err := pubClient.GetQueueUrl(context.TODO(), &sqs.GetQueueUrlInput{
			QueueName: aws.String("test-queue"),
		})
		if err != nil {
			log.Fatalf("failed to get queue URL: %v", err)
		}

		input := &sqs.SendMessageInput{
			QueueUrl:    queue.QueueUrl,
			MessageBody: aws.String("Hello, SQS!"),
		}

		for i := 0; i < 10; i++ {
			input.MessageBody = aws.String("Hello, SQS! " + fmt.Sprintf("%d", i))
			// Simulate some processing delay
			time.Sleep(100 * time.Millisecond)
			log.Printf("Sending message: %s", *input.MessageBody)
			// Send the message
			_, err := pubClient.SendMessage(context.TODO(), input)
			if err != nil {
				log.Fatalf("failed to send message: %v", err)
			}
		}

		log.Println("All messages sent successfully.")
	}(pubClient)

	subClient := sqs.NewFromConfig(sqsCfg)
	go func(sub *sqs.Client) {
		// Example of subscribing to a queue and processing messages
		queue, err := subClient.GetQueueUrl(context.TODO(), &sqs.GetQueueUrlInput{
			QueueName: aws.String("test-queue"),
		})
		if err != nil {
			log.Fatalf("failed to get queue URL: %v", err)
		}

		for {
			// Receive messages from the queue
			output, err := subClient.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
				QueueUrl:            queue.QueueUrl,
				MaxNumberOfMessages: 10,
				WaitTimeSeconds:     20, // Long polling
				VisibilityTimeout:   30, // Visibility timeout
			})
			if err != nil {
				log.Fatalf("failed to receive messages: %v", err)
			}

			if len(output.Messages) == 0 {
				log.Println("No messages received, waiting for new messages...")
				time.Sleep(5 * time.Second) // Wait before checking for new messages
				continue
			}

			for _, msg := range output.Messages {
				log.Printf("Received message: %s", *msg.Body)
				// Process the message (for example, print it)
				// After processing, delete the message from the queue
				_, err := subClient.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
					QueueUrl:      queue.QueueUrl,
					ReceiptHandle: msg.ReceiptHandle,
				})
				if err != nil {
					log.Printf("failed to delete message: %v", err)
				} else {
					log.Printf("Deleted message: %s", *msg.Body)
				}
			}
			// Simulate some processing delay
			time.Sleep(1 * time.Second)
		}
	}(subClient)

	// Keep the main function running to allow goroutines to execute
	select {
	case <-time.After(30 * time.Second):
		log.Println("Main function completed, exiting.")
		return
	case <-context.Background().Done():
		log.Println("Context done, exiting.")
		return
	}
}

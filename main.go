package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func main() {
	fmt.Println("Queue Engine starting...")

	session, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: aws.String("ap-south-1"),
		},
		Profile: "sb_sqs_general",
	})
	if err != nil {
		panic(err)
	}

	queueName := "MyFirstQueue.fifo"

	queueURL, err := GetQueueURL(session, queueName)
	if err != nil {
		panic(err)
	}

	fmt.Println("Listening to queue...")

	// for {
	maxMessages := 10
	msgRes, err := GetMessages(session, *queueURL.QueueUrl, maxMessages)
	if err != nil {
		fmt.Printf("Got an error while trying to retrieve message: %v", err)
		os.Exit(1)
	}

	if len(*&msgRes.Messages) == 0 {
		os.Exit(1)
	}

	fmt.Println("LENGTH: %v", len(msgRes.Messages))

	for _, message := range msgRes.Messages {
		fmt.Println("----")
		fmt.Println("Message Body: " + *message.Body)
		fmt.Println("Message Handle: " + *message.ReceiptHandle)

		receiptHandle := msgRes.Messages[0].ReceiptHandle
		err = DeleteMessage(session, *queueURL.QueueUrl, receiptHandle)
		if err != nil {
			fmt.Printf("Got an error while trying to delete message: %v", err)
			return
		}

		fmt.Println("Deleted message")
		fmt.Println("----")
		fmt.Println("----")
	}
	// }
}

func GetQueueURL(sess *session.Session, queue string) (*sqs.GetQueueUrlOutput, error) {
	sqsClient := sqs.New(sess)

	result, err := sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queue,
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func GetMessages(sess *session.Session, queueUrl string, maxMessages int) (*sqs.ReceiveMessageOutput, error) {
	sqsClient := sqs.New(sess)

	msgResult, err := sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            &queueUrl,
		MaxNumberOfMessages: aws.Int64(int64(maxMessages)),
	})

	if err != nil {
		return nil, err
	}

	return msgResult, nil
}

func DeleteMessage(sess *session.Session, queueUrl string, messageHandle *string) error {
	sqsClient := sqs.New(sess)

	_, err := sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &queueUrl,
		ReceiptHandle: messageHandle,
	})

	return err
}

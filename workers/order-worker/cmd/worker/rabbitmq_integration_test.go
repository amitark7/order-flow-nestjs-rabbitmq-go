package main

import (
	"context"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRabbitMQDeadLetterRouting(t *testing.T) {
	if os.Getenv("RABBITMQ_INTEGRATION") != "1" {
		t.Skip("set RABBITMQ_INTEGRATION=1 to run RabbitMQ integration tests")
	}

	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://orderflow:local_dev_password@localhost:5672/"
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open RabbitMQ channel: %v", err)
	}
	defer ch.Close()

	const testQueue = "order.integration.test"
	const deadLetterQueue = "order.integration.dlq"

	if _, err := ch.QueueDeclarePassive(
		testQueue, true, false, false, false, nil,
	); err != nil {
		t.Fatalf("test queue is unavailable: %v", err)
	}

	if _, err := ch.QueueDeclarePassive(
		deadLetterQueue, true, false, false, false, nil,
	); err != nil {
		t.Fatalf("dead-letter queue is unavailable: %v", err)
	}

	if err := ch.Confirm(false); err != nil {
		t.Fatalf("enable publisher confirms: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	body := []byte(`{"eventId":"","eventType":"invalid","version":1,"data":{}}`)

	if err := ch.PublishWithContext(
		ctx, "", testQueue, true, false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	); err != nil {
		t.Fatalf("publish test message: %v", err)
	}

	select {
	case confirmation := <-confirms:
		if !confirmation.Ack {
			t.Fatal("RabbitMQ negatively acknowledged the published message")
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for publisher confirmation")
	}

	deliveries, err := ch.Consume(
		testQueue, "", false, false, false, false, nil,
	)
	if err != nil {
		t.Fatalf("consume test message: %v", err)
	}

	var delivery amqp.Delivery
	select {
	case delivery = <-deliveries:
		if string(delivery.Body) != string(body) {
			t.Fatalf("unexpected message body: %s", delivery.Body)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for test message")
	}

	if err := delivery.Nack(false, false); err != nil {
		t.Fatalf("reject test message: %v", err)
	}

	dlqCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("open dead-letter channel: %v", err)
	}
	defer dlqCh.Close()

	deadLetterDeliveries, err := dlqCh.Consume(
		deadLetterQueue, "", true, false, false, false, nil,
	)
	if err != nil {
		t.Fatalf("consume dead-letter queue: %v", err)
	}

	select {
	case deadLetter := <-deadLetterDeliveries:
		if string(deadLetter.Body) != string(body) {
			t.Fatalf("unexpected dead-letter body: %s", deadLetter.Body)
		}
	case <-ctx.Done():
		t.Fatal("rejected message did not arrive in the dead-letter queue")
	}
}

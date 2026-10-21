package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

type OrderCompletedEvent struct {
	EventID    string `json:"eventId"`
	EventType  string `json:"eventType"`
	Version    int    `json:"version"`
	OccurredAt string `json:"occurredAt"`
	Data       struct {
		ID            string `json:"id"`
		CustomerEmail string `json:"customerEmail"`
		Amount        int    `json:"amount"`
		Status        string `json:"status"`
	} `json:"data"`
}

func main() {
	broker := os.Getenv("KAFKA_BROKER")

	if broker == "" {
		broker = "localhost:9092"
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    "order.completed",
		GroupID:  "notification-worker-group",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Printf("Notification Worker listening on Kafka topic order.completed at %s", broker)

	for {
		message, err := reader.FetchMessage(ctx)

		if err != nil {
			if ctx.Err() != nil {
				log.Println("Shutdown Signal recieve")
				return
			}

			log.Printf("Failed to fetch Kafka message: %v", err)
			continue
		}

		var event OrderCompletedEvent

		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Printf("Invalid event JSON at partition %d offset %d: %v",
				message.Partition, message.Offset, err)

			if err := reader.CommitMessages(ctx, message); err != nil {
				log.Printf("Failed to commit malformed message: %v", err)
			}
			continue
		}
		log.Printf("DEBUG EVENT: %+v", event)

		if event.EventType != "order.completed" || event.Data.ID == "" ||
			event.Data.CustomerEmail == "" {
			log.Printf("Invalid order.completed event at partition %d offset %d",
				message.Partition, message.Offset)
			if err := reader.CommitMessages(ctx, message); err != nil {
				log.Printf("Failed to commit invalid event: %v", err)
			}
			continue
		}

		log.Printf(
			"NOTIFICATION: Order %s completed for %s; amount=%d; status=%s",
			event.Data.ID,
			event.Data.CustomerEmail,
			event.Data.Amount,
			event.Data.Status,
		)

		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("Failed to commit Kafka message: %v", err)
			continue
		}
	}

}

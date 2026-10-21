package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
)

type OrderMessage struct {
	EventId   string `json:"eventId"`
	EventType string `json:"eventType"`
	Version   int    `json:"version"`
	Data      struct {
		ID            string `json:"id"`
		CustomerEmail string `json:"customerEmail"`
		Amount        int    `json:"amount"`
	} `json:"data"`
}

func main() {
	rabbitMqUrl := os.Getenv("RABBITMQ_URL")
	if rabbitMqUrl == "" {
		rabbitMqUrl = "amqp://orderflow:local_dev_password@localhost:5672/"
	}

	databaseUrl := os.Getenv("DATABASE_URL")

	if databaseUrl == "" {
		databaseUrl = "postgres://orderflow:local_dev_password@localhost:5432/orderflow?sslmode=disable"
	}

	db, err := sql.Open("pgx", databaseUrl)

	if err != nil {
		log.Fatalf("Failed to configure PostgreSQL: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}

	log.Printf("Connected to PostgreSQL")

	kafkaBroker := os.Getenv("KAFKA_BROKER")

	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        "order.completed",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}

	defer kafkaWriter.Close()

	log.Println("Kafka producer configured")

	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	conn, err := amqp.Dial(rabbitMqUrl)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	defer conn.Close()

	ch, err := conn.Channel()

	if err != nil {
		log.Fatalf("Failed to open RabbitMQ channel: %v", err)
	}

	defer conn.Close()

	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatalf("Failed to configure consumer QoS: %v", err)
	}

	msgs, err := ch.Consume(
		"order.process",
		"order-worker",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatalf("Failed to register consumer: %v", err)
	}

	log.Println("Go Order Worker waiting for messages...")

	for msg := range msgs {
		var event OrderMessage

		if err := json.Unmarshal(msg.Body, &event); err != nil {
			log.Printf("Invalid JSON; rejecting message: %v", err)
			if err := msg.Nack(false, false); err != nil {
				log.Printf("Failed to reject message %v", err)
			}
			continue
		}

		if event.Data.ID == "" {
			log.Println("Missing Order ID; rejecting message")

			if err := msg.Nack(false, false); err != nil {
				log.Printf("Failed to reject message %v", err)
			}
			continue
		}

		result, err := db.Exec(
			`UPDATE orders
			 SET status = 'PROCESSING', "updatedAt" = NOW()
			 WHERE id = $1 AND status = 'PENDING'`,
			event.Data.ID,
		)

		if err != nil {
			log.Printf("Database update failed for order %s :%v ", event.Data.ID, err)
			if err := msg.Nack(false, true); err != nil {
				log.Printf("Failed to requeue message: %v", err)
			}
			continue
		}

		rows, err := result.RowsAffected()

		if err != nil {
			log.Printf("Could not checked update result")
			if err := msg.Nack(false, true); err != nil {
				log.Printf("Failed to requeue message : %v", err)
			}
			continue
		}

		if rows == 0 {
			log.Printf("Order %s not updated: missing order or not pending", event.Data.ID)

			if err := msg.Ack(false); err != nil {
				log.Printf("Failed to acknowledge message: %v", err)
			}
			continue
		}

		if err := msg.Ack(false); err != nil {
			log.Printf("Failed to acknownledge order %s : %v", event.Data.ID, err)
			continue
		}

		result, err = db.Exec(
			`
			UPDATE orders
			SET status = 'COMPLETED', "updatedAt" = NOW()
			WHERE id = $1 AND status = 'PROCESSING'`,
			event.Data.ID,
		)

		if err != nil {
			log.Printf("Failed to complete order %s: %v", event.Data.ID, err)

			if err := msg.Nack(false, true); err != nil {
				log.Printf("Failed to requeue message : %v", err)
			}
			continue
		}

		rows, err = result.RowsAffected()
		if err != nil || rows == 0 {
			log.Printf("Order %s could not be marked COMPLETED", event.Data.ID)
			if err := msg.Nack(false, true); err != nil {
				log.Printf("Failed to requeue message: %v", err)
			}
			continue
		}

		completedEvent := map[string]interface{}{
			"eventId":    event.EventId,
			"eventType":  "order.completed",
			"version":    event.Version,
			"occurredAt": time.Now().UTC().Format(time.RFC3339Nano),
			"data": map[string]interface{}{
				"id":            event.Data.ID,
				"customerEmail": event.Data.CustomerEmail,
				"amount":        event.Data.Amount,
				"status":        "COMPLETED",
			},
		}

		payload, err := json.Marshal(completedEvent)
		if err != nil {
			log.Printf("Failed to encode completion event: %v", err)
			if err := msg.Nack(false, true); err != nil {
				log.Printf("Failed to requeue message: %v", err)
			}
			continue
		}

		err = kafkaWriter.WriteMessages(
			context.Background(),
			kafka.Message{
				Key:   []byte(event.Data.ID),
				Value: payload,
				Time:  time.Now().UTC(),
			},
		)

		if err != nil {
			log.Printf("Kafka publish Failed for order %s: %v", event.Data.ID, err)
			if err := msg.Nack(false, true); err != nil {
				log.Printf("Failed to requeue message: %v", err)
			}
			continue
		}

		if err := msg.Ack(false); err != nil {
			log.Printf("Failed to acknowledge order %s: %v", event.Data.ID, err)
			continue
		}

		log.Printf("Order %s COMPLETED; event published to Kafka", event.Data.ID)
	}

}

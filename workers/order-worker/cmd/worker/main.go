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

	conn, err := amqp.Dial(rabbitMqUrl)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	defer conn.Close()

	ch, err := conn.Channel()

	if err != nil {
		log.Fatalf("Failed to open RabbitMQ channel: %v", err)
	}

	defer ch.Close()

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

		if event.EventType != "order.process" || event.Data.ID == "" || event.Data.CustomerEmail == "" || event.Data.Amount < 0 {
			log.Printf("Invalid order.process event; rejecting message")

			if err := msg.Nack(false, false); err != nil {
				log.Printf("Failed to reject message: %v", err)
			}
			continue
		}

		tx, err := db.BeginTx(context.Background(), nil)

		if err != nil {
			log.Printf("Failed to begin transaction %v", err)

			if err := msg.Nack(false, true); err != nil {
				log.Printf("Failed to requeue message: %v", err)
			}
			continue
		}

		result, err := tx.ExecContext(
			context.Background(),
			`UPDATE orders
			 SET status = 'COMPLETED', "updatedAt" = NOW()
			 WHERE id = $1 AND status = 'PENDING'`,
			event.Data.ID,
		)

		if err != nil {
			_ = tx.Rollback()
			log.Printf("Failed to update order %s: %v", event.Data.ID, err)
			if err := msg.Nack(false, true); err != nil {
				log.Printf("Failed to requeue message: %v", err)
			}
			continue
		}

		rows, err := result.RowsAffected()

		if err != nil {
			_ = tx.Rollback()
			log.Printf("Failed to check update result: %v", err)
			if err := msg.Nack(false, true); err != nil {
				log.Printf("Failed to requeue message : %v", err)
			}
			continue
		}

		if rows == 0 {
			_ = tx.Rollback()
			log.Printf(
				"Order %s is missing or no longer PENDING; acknowledging duplicate",
				event.Data.ID,
			)

			if err := msg.Ack(false); err != nil {
				log.Printf("Failed to acknowledge message: %v", err)
			}
			continue
		}

		var outboxID string
		err = tx.QueryRowContext(
			context.Background(),
			`SELECT gen_random_uuid()::text`).Scan(&outboxID)

		if err != nil {
			_ = tx.Rollback()
			log.Printf("Failed to generate outbox event ID: %v", err)
			if err := msg.Nack(false, true); err != nil {
				log.Printf("Failed to requeue message: %v", err)
			}
			continue
		}

		completedEvent := map[string]interface{}{
			"eventId":    outboxID,
			"eventType":  "order.completed",
			"version":    1,
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
			_ = tx.Rollback()
			log.Printf("Failed to encode completion event: %v", err)
			if err := msg.Nack(false, true); err != nil {
				log.Printf("Failed to requeue message: %v", err)
			}
			continue
		}

		_, err = tx.ExecContext(
			context.Background(),
			`INSERT INTO outbox_events
			 (id, aggregate_id, event_type, payload)
			 VALUES ($1, $2, $3, $4::jsonb)`,
			outboxID,
			event.Data.ID,
			"order.completed",
			string(payload),
		)
		if err != nil {
			_ = tx.Rollback()
			log.Printf("Failed to insert outbox event for order %s: %v", event.Data.ID, err)
			if err := msg.Nack(false, true); err != nil {
				log.Printf("Failed to requeue message: %v", err)
			}
			continue
		}

		if err := tx.Commit(); err != nil {
			log.Printf("Transaction commit failed for order %s: %v", event.Data.ID, err)
			if nackErr := msg.Nack(false, true); nackErr != nil {
				log.Printf("Failed to requeue message: %v", nackErr)
			}
			continue
		}

		// Acknowledge only after both database changes are committed.
		if err := msg.Ack(false); err != nil {
			log.Printf("Failed to acknowledge order %s: %v", event.Data.ID, err)
			continue
		}

		log.Printf(
			"Order %s COMPLETED; outbox event %s saved",
			event.Data.ID,
			outboxID,
		)
	}
}

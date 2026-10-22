package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/segmentio/kafka-go"
)

type OutboxEvent struct {
	ID          string
	AggregateID string
	EventType   string
	Payload     []byte
}

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://orderflow:local_dev_password@localhost:5432/orderflow?sslmode=disable"
	}

	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		log.Fatalf("Failed to configure PostgreSQL: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	log.Println("Outbox Publisher connected to PostgreSQL")

	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        "order.completed",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
	defer writer.Close()

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	log.Printf("Outbox Publisher running; Kafka broker=%s", broker)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		if err := publishPending(ctx, db, writer); err != nil && ctx.Err() == nil {
			log.Printf("Outbox publishing cycle failed: %v", err)
		}

		select {
		case <-ctx.Done():
			log.Println("Outbox Publisher shutting down")
			return
		case <-ticker.C:
		}
	}
}

func publishPending(
	ctx context.Context,
	db *sql.DB,
	writer *kafka.Writer,
) error {
	rows, err := db.QueryContext(
		ctx,
		`SELECT id::text, aggregate_id::text, event_type, payload
		 FROM outbox_events
		 WHERE published_at IS NULL
		 ORDER BY created_at
		 LIMIT 100`,
	)
	if err != nil {
		return err
	}

	var events []OutboxEvent
	for rows.Next() {
		var event OutboxEvent
		if err := rows.Scan(
			&event.ID,
			&event.AggregateID,
			&event.EventType,
			&event.Payload,
		); err != nil {
			rows.Close()
			return err
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	for _, event := range events {
		if event.EventType != "order.completed" {
			log.Printf("Skipping unsupported outbox event %s: %s", event.ID, event.EventType)
			continue
		}

		err := writer.WriteMessages(
			ctx,
			kafka.Message{
				Key:   []byte(event.AggregateID),
				Value: event.Payload,
				Time:  time.Now().UTC(),
			},
		)
		if err != nil {
			log.Printf("Kafka publish failed for outbox event %s: %v", event.ID, err)
			continue
		}

		result, err := db.ExecContext(
			ctx,
			`UPDATE outbox_events
			 SET published_at = NOW(), attempts = attempts + 1
			 WHERE id = $1 AND published_at IS NULL`,
			event.ID,
		)
		if err != nil {
			log.Printf("Kafka accepted event %s, but marking it published failed: %v", event.ID, err)
			continue
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 1 {
			log.Printf("Published outbox event %s for order %s", event.ID, event.AggregateID)
		}
	}

	return nil
}

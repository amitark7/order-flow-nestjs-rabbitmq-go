package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
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

type NotificationJob struct {
	ID            string
	EventID       string
	OrderID       string
	CustomerEmail string
	Amount        int
	Attempts      int
}

func claimNotificationJob(
	ctx context.Context,
	db *sql.DB,
) (*NotificationJob, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	job := &NotificationJob{}

	err = tx.QueryRowContext(ctx, `
		SELECT id, event_id, order_id, customer_email, amount, attempts
		FROM notifications
		WHERE (
			(
				status = 'PENDING'
				AND attempts < 3
				AND (
					next_attempt_at IS NULL
					OR next_attempt_at <= NOW()
				)
			)
			OR (
				status = 'PROCESSING'
				AND updated_at < NOW() - INTERVAL '2 minutes'
				AND attempts < 3
			)
		)
		ORDER BY created_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`).Scan(
		&job.ID,
		&job.EventID,
		&job.OrderID,
		&job.CustomerEmail,
		&job.Amount,
		&job.Attempts,
	)

	if err != nil {
		return nil, err
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE notifications
		SET status = 'PROCESSING',
		    attempts = attempts + 1,
		    updated_at = NOW()
		WHERE id = $1
	`, job.ID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	job.Attempts++
	return job, nil
}

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://orderflow:local_dev_password@localhost:5432/orderflow?sslmode=disable"
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		log.Fatalf("Failed to configure PostgreSQL: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	log.Println("Notification Worker connected to PostgreSQL")

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

	// Process pending notification jobs every 2 seconds.
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			processPendingNotifications(ctx, db)

			select {
			case <-ctx.Done():
				log.Println("Notification processor shutting down")
				return
			case <-ticker.C:
			}
		}
	}()

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Shutdown signal received")
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

		if event.EventID == "" ||
			event.EventType != "order.completed" ||
			event.Data.ID == "" ||
			event.Data.CustomerEmail == "" {
			log.Printf("Invalid order.completed event at partition %d offset %d",
				message.Partition, message.Offset)
			if err := reader.CommitMessages(ctx, message); err != nil {
				log.Printf("Failed to commit invalid event: %v", err)
			}
			continue
		}

		// Atomically claim this event ID. The primary key prevents duplicates.
		result, err := db.ExecContext(ctx, `
			INSERT INTO notifications (
				event_id,
				order_id,
				customer_email,
				amount,
				status
			)
			VALUES ($1, $2, $3, $4, 'PENDING')
			ON CONFLICT (event_id) DO NOTHING
		`,
			event.EventID,
			event.Data.ID,
			event.Data.CustomerEmail,
			event.Data.Amount,
		)
		if err != nil {
			log.Printf("Failed to save notification job for event %s: %v", event.EventID, err)
			continue
		}

		inserted, err := result.RowsAffected()
		if err != nil {
			log.Printf("Failed to inspect notification insert result: %v", err)
			continue
		}

		if inserted == 0 {
			log.Printf("Duplicate event %s; notification job already exists", event.EventID)
		} else {
			log.Printf("Notification job created for order %s; status=PENDING", event.Data.ID)
		}

		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("Failed to commit Kafka message: %v", err)
			continue
		}
	}
}

func processPendingNotifications(ctx context.Context, db *sql.DB) {

	const maxAttempts = 3

	_, err := db.ExecContext(ctx, `
		UPDATE notifications
		SET status = 'FAILED',
			last_error = 'Maximum notification attempts reached',
			updated_at = NOW()
		WHERE status = 'PENDING'
		AND attempts >= $1
	`, maxAttempts)
	if err != nil {
		log.Printf("Failed to mark exhausted pending jobs: %v", err)
		return
	}

	result, err := db.ExecContext(ctx, `
		UPDATE notifications
		SET status = 'FAILED',
			last_error = 'Worker crashed after maximum attempts',
			updated_at = NOW()
		WHERE status = 'PROCESSING'
		AND updated_at < NOW() - INTERVAL '2 minutes'
		AND attempts >= $1
	`, maxAttempts)
	if err != nil {
		log.Printf("Failed to mark exhausted notification jobs: %v", err)
		return
	}

	if affected, _ := result.RowsAffected(); affected > 0 {
		log.Printf("Marked %d stale notification job(s) as FAILED", affected)
	}

	for {
		if ctx.Err() != nil {
			return
		}

		job, err := claimNotificationJob(ctx, db)
		if errors.Is(err, sql.ErrNoRows) {
			return
		}
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("Failed to claim notification job: %v", err)
			}
			return
		}

		// Simulate delivery failure when the email is test-fail@example.com.
		var deliveryErr error
		if job.CustomerEmail == "test-fail@example.com" {
			deliveryErr = errors.New("simulated notification delivery failure")
		} else {
			log.Printf(
				"SIMULATED NOTIFICATION: Order %s completed for %s; amount=%d",
				job.OrderID,
				job.CustomerEmail,
				job.Amount,
			)
		}

		if deliveryErr == nil {
			result, err := db.ExecContext(ctx, `
				UPDATE notifications
				SET status = 'SENT',
					updated_at = NOW(),
					last_error = NULL,
					next_attempt_at = NULL
				WHERE id = $1 AND status = 'PROCESSING'
			`, job.ID)
			if err != nil {
				log.Printf("Failed to mark notification %s as SENT: %v", job.ID, err)
				return
			}

			if affected, _ := result.RowsAffected(); affected == 1 {
				log.Printf("Notification job %s marked SENT", job.ID)
			}
			continue
		}

		log.Printf(
			"Delivery failed for job %s (attempt %d/%d): %v",
			job.ID, job.Attempts, maxAttempts, deliveryErr,
		)

		nextStatus := "PENDING"
		var nextAttemptAt any

		if job.Attempts >= maxAttempts {
			nextStatus = "FAILED"
		} else {
			delay := time.Duration(1<<job.Attempts) * time.Second
			nextAttemptAt = time.Now().Add(delay)
		}
		_, err = db.ExecContext(ctx, `
			UPDATE notifications
			SET status = $2,
				last_error = $3,
				next_attempt_at = $4,
				updated_at = NOW()
			WHERE id = $1 AND status = 'PROCESSING'
		`, job.ID, nextStatus, deliveryErr.Error(), nextAttemptAt)

		if err != nil {
			log.Printf("Failed to update notification job %s after delivery failure: %v", job.ID, err)
			return
		}

		if nextStatus == "FAILED" {
			log.Printf("Notification job %s exhausted retries; status=FAILED", job.ID)
		} else {
			log.Printf("Notification job %s scheduled for retry; status=PENDING", job.ID)
			return // Avoid retrying immediately in a tight loop.
		}
	}
}

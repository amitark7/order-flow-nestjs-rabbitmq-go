package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/segmentio/kafka-go"
)

type OrderCompletedEvent struct {
	EventID       string `json:"eventId"`
	EventType     string `json:"eventType"`
	Version       int    `json:"version"`
	OccurredAt    string `json:"occurredAt"`
	CorrelationID string `json:"correlationId"`
	Data          struct {
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
	CorrelationID string
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
		SELECT id, event_id, correlation_id, order_id, customer_email, amount, attempts
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
		&job.CorrelationID,
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

func saveNotificationJob(
	ctx context.Context,
	db *sql.DB,
	event OrderCompletedEvent,
) (bool, error) {
	result, err := db.ExecContext(ctx, `
		INSERT INTO notifications (
			event_id,
			correlation_id,
			order_id,
			customer_email,
			amount,
			status
		)
		VALUES ($1, $2, $3, $4, $5, 'PENDING')
		ON CONFLICT (event_id) DO NOTHING
	`,
		event.EventID,
		event.CorrelationID,
		event.Data.ID,
		event.Data.CustomerEmail,
		event.Data.Amount,
	)
	if err != nil {
		return false, err
	}

	inserted, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	return inserted == 1, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

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
		slog.Error("Failed to configure PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		slog.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	slog.Info("Notification Worker connected to PostgreSQL")

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

	slog.Info(
		"Notification Worker listening on Kafka topic",
		"topic", "order.completed",
		"broker", broker,
	)

	var processorWG sync.WaitGroup
	processorWG.Add(1)
	// Process pending notification jobs every 2 seconds.
	go func() {
		defer processorWG.Done()

		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			processPendingNotifications(ctx, db)

			select {
			case <-ctx.Done():
				slog.Info("Notification processor shutting down")
				return
			case <-ticker.C:
			}
		}
	}()

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("Shutdown signal received")
				break
			}
			slog.Error("Failed to fetch Kafka message", "error", err)
			continue
		}

		var event OrderCompletedEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			slog.Error(
				"Invalid event JSON",
				"partition", message.Partition,
				"offset", message.Offset,
				"error", err,
			)
			if err := reader.CommitMessages(ctx, message); err != nil {
				slog.Error(
					"Failed to commit malformed Kafka message",
					"partition", message.Partition,
					"offset", message.Offset,
					"error", err,
				)
			}
			continue
		}

		if event.EventID == "" ||
			event.EventType != "order.completed" ||
			event.Data.ID == "" ||
			event.Data.CustomerEmail == "" {
			slog.Error(
				"Invalid order.completed event",
				"partition", message.Partition,
				"offset", message.Offset,
				"correlation_id", event.CorrelationID,
			)
			if err := reader.CommitMessages(ctx, message); err != nil {
				slog.Error(
					"Failed to commit invalid Kafka event",
					"partition", message.Partition,
					"offset", message.Offset,
					"error", err,
				)
			}
			continue
		}

		// Atomically claim this event ID. The primary key prevents duplicates.
		inserted, err := saveNotificationJob(ctx, db, event)
		if err != nil {
			slog.Error(
				"Failed to save notification job",
				"event_id", event.EventID,
				"correlation_id", event.CorrelationID,
				"error", err,
			)
			continue
		}

		if !inserted {
			slog.Info(
				"Duplicate event; notification job already exists",
				"event_id", event.EventID,
				"correlation_id", event.CorrelationID,
			)
		} else {
			slog.Info(
				"Notification job created",
				"event_id", event.EventID,
				"correlation_id", event.CorrelationID,
				"order_id", event.Data.ID,
				"customer_email", event.Data.CustomerEmail,
				"status", "PENDING",
			)
		}

		if err := reader.CommitMessages(ctx, message); err != nil {
			slog.Error(
				"Failed to commit Kafka message",
				"partition", message.Partition,
				"offset", message.Offset,
				"error", err,
			)
			continue
		}
	}

	// Cancel the shared context so the processor exits.
	stop()

	// Close Kafka reader to release any remaining resources.
	if err := reader.Close(); err != nil {
		slog.Error("Failed to close Kafka reader", "error", err)
	}

	// Wait for the processor before closing PostgreSQL.
	processorWG.Wait()

	if err := db.Close(); err != nil {
		slog.Error("Failed to close PostgreSQL connection", "error", err)
	}

	slog.Info("Notification Worker shut down cleanly")
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
		slog.Error("Failed to mark exhausted pending jobs", "error", err)
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
		slog.Error("Failed to mark exhausted notification jobs", "error", err)
		return
	}

	if affected, _ := result.RowsAffected(); affected > 0 {
		slog.Info(
			"Marked stale notification jobs as FAILED",
			"count", affected,
		)
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
				slog.Error("Failed to claim notification job", "error", err)
			}
			return
		}

		// Simulate delivery failure when the email is test-fail@example.com.
		var deliveryErr error
		if job.CustomerEmail == "test-fail@example.com" {
			deliveryErr = errors.New("simulated notification delivery failure")
		} else {
			slog.Info(
				"SIMULATED NOTIFICATION",
				"correlation_id", job.CorrelationID,
				"order_id", job.OrderID,
				"customer_email", job.CustomerEmail,
				"amount", job.Amount,
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
				slog.Error(
					"Failed to mark notification as SENT",
					"notification_id", job.ID,
					"order_id", job.OrderID,
					"error", err,
				)
				return
			}

			if affected, _ := result.RowsAffected(); affected == 1 {
				slog.Info(
					"Notification job marked SENT",
					"notification_id", job.ID,
					"event_id", job.EventID,
					"order_id", job.OrderID,
					"customer_email", job.CustomerEmail,
					"attempt", job.Attempts,
					"status", "SENT",
				)
			}
			continue
		}

		slog.Error(
			"Notification delivery failed",
			"notification_id", job.ID,
			"event_id", job.EventID,
			"order_id", job.OrderID,
			"customer_email", job.CustomerEmail,
			"attempt", job.Attempts,
			"error", deliveryErr.Error(),
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
			slog.Error(
				"Failed to update notification job after delivery failure",
				"notification_id", job.ID,
				"order_id", job.OrderID,
				"error", err,
			)
			return
		}

		if nextStatus == "FAILED" {
			slog.Error(
				"Notification job exhausted retries",
				"notification_id", job.ID,
				"event_id", job.EventID,
				"order_id", job.OrderID,
				"customer_email", job.CustomerEmail,
				"attempt", job.Attempts,
				"status", nextStatus,
				"error", deliveryErr.Error(),
			)
		} else {
			slog.Info(
				"Notification job scheduled for retry",
				"notification_id", job.ID,
				"event_id", job.EventID,
				"order_id", job.OrderID,
				"attempt", job.Attempts,
				"status", nextStatus,
				"next_attempt_at", nextAttemptAt,
			)
			return // Avoid retrying immediately in a tight loop.
		}
	}
}

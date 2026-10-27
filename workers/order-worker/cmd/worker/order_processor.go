package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

func processOrder(
	ctx context.Context,
	db *sql.DB,
	event OrderMessage,
	correlationID string,
) OrderProcessOutcome {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return OrderProcessOutcome{Err: fmt.Errorf("begin transaction: %w", err)}
	}

	// Roll back if we exit before successfully committing.
	defer tx.Rollback()

	result, err := tx.ExecContext(
		ctx,
		`UPDATE orders
		 SET status = 'COMPLETED', "updatedAt" = NOW()
		 WHERE id = $1 AND status = 'PENDING'`,
		event.Data.ID,
	)
	if err != nil {
		return OrderProcessOutcome{Err: fmt.Errorf("update order: %w", err)}
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return OrderProcessOutcome{Err: fmt.Errorf("check updated rows: %w", err)}
	}

	if rows == 0 {
		return OrderProcessOutcome{Result: OrderDuplicate}
	}

	var outboxID string
	err = tx.QueryRowContext(
		ctx,
		`SELECT gen_random_uuid()::text`,
	).Scan(&outboxID)
	if err != nil {
		return OrderProcessOutcome{Err: fmt.Errorf("generate outbox ID: %w", err)}
	}

	completedEvent := map[string]interface{}{
		"eventId":       outboxID,
		"eventType":     "order.completed",
		"version":       1,
		"occurredAt":    time.Now().UTC().Format(time.RFC3339Nano),
		"correlationId": correlationID,
		"data": map[string]interface{}{
			"id":            event.Data.ID,
			"customerEmail": event.Data.CustomerEmail,
			"amount":        event.Data.Amount,
			"status":        "COMPLETED",
		},
	}

	payload, err := json.Marshal(completedEvent)
	if err != nil {
		return OrderProcessOutcome{Err: fmt.Errorf("encode completion event: %w", err)}
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO outbox_events
		 (id, aggregate_id, event_type, payload)
		 VALUES ($1, $2, $3, $4::jsonb)`,
		outboxID,
		event.Data.ID,
		"order.completed",
		string(payload),
	)
	if err != nil {
		return OrderProcessOutcome{Err: fmt.Errorf("insert outbox event: %w", err)}
	}

	if err := tx.Commit(); err != nil {
		return OrderProcessOutcome{Err: fmt.Errorf("commit transaction: %w", err)}
	}

	return OrderProcessOutcome{Result: OrderCompleted}
}

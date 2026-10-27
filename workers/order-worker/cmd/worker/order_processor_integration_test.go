package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestProcessOrderPostgresIntegration(t *testing.T) {
	if os.Getenv("POSTGRES_INTEGRATION") != "1" {
		t.Skip("set POSTGRES_INTEGRATION=1 to run PostgreSQL integration tests")
	}

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://orderflow:local_dev_password@localhost:5432/orderflow_go_test?sslmode=disable"
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open PostgreSQL connection: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("connect to PostgreSQL test database: %v", err)
	}

	var databaseName string
	if err := db.QueryRowContext(ctx, "SELECT current_database()").Scan(&databaseName); err != nil {
		t.Fatalf("check database name: %v", err)
	}

	if databaseName != "orderflow_go_test" {
		t.Fatalf("refusing to run against database %q", databaseName)
	}

	orderID := "11111111-1111-4111-8111-111111111111"

	// Clean up any leftovers from an earlier run.
	if _, err := db.ExecContext(ctx,
		`DELETE FROM outbox_events WHERE aggregate_id = $1`, orderID); err != nil {
		t.Fatalf("clean test outbox row: %v", err)
	}

	if _, err := db.ExecContext(ctx,
		`DELETE FROM orders WHERE id = $1`, orderID); err != nil {
		t.Fatalf("clean test order row: %v", err)
	}

	t.Cleanup(func() {
		_, _ = db.Exec(
			`DELETE FROM outbox_events WHERE aggregate_id = $1`, orderID)
		_, _ = db.Exec(
			`DELETE FROM orders WHERE id = $1`, orderID)
	})

	// Arrange: create a pending order in the real test database.
	_, err = db.ExecContext(ctx, `
		INSERT INTO orders (id, status)
		VALUES ($1, 'PENDING')
	`, orderID)
	if err != nil {
		t.Fatalf("insert pending order: %v", err)
	}

	event := OrderMessage{
		EventId:   "integration-event-001",
		EventType: "order.process",
		Version:   1,
		Data: OrderData{
			ID:            orderID,
			CustomerEmail: "postgres-integration@example.com",
			Amount:        499,
		},
	}

	// Act: process the order using PostgreSQL.
	outcome := processOrder(
		ctx,
		db,
		event,
		"integration-correlation-001",
	)

	if outcome.Err != nil {
		t.Fatalf("process order: %v", outcome.Err)
	}

	if outcome.Result != OrderCompleted {
		t.Fatalf("expected result %q, got %q",
			OrderCompleted, outcome.Result)
	}

	// Assert: the order was completed.
	var status string
	err = db.QueryRowContext(
		ctx,
		`SELECT status FROM orders WHERE id = $1`,
		orderID,
	).Scan(&status)
	if err != nil {
		t.Fatalf("read order status: %v", err)
	}

	if status != "COMPLETED" {
		t.Fatalf("expected order status COMPLETED, got %q", status)
	}

	// Assert: an outbox event was committed.
	var eventType string
	var payloadBytes []byte

	err = db.QueryRowContext(
		ctx,
		`SELECT event_type, payload
		 FROM outbox_events
		 WHERE aggregate_id = $1`,
		orderID,
	).Scan(&eventType, &payloadBytes)
	if err != nil {
		t.Fatalf("read outbox event: %v", err)
	}

	if eventType != "order.completed" {
		t.Fatalf("expected event type order.completed, got %q", eventType)
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		t.Fatalf("decode outbox payload: %v", err)
	}

	if payload["eventType"] != "order.completed" {
		t.Fatalf("unexpected payload eventType: %v", payload["eventType"])
	}

	if payload["correlationId"] != "integration-correlation-001" {
		t.Fatalf("unexpected correlationId: %v", payload["correlationId"])
	}

	data, ok := payload["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected payload data object, got %T", payload["data"])
	}

	if data["id"] != orderID {
		t.Fatalf("unexpected order ID in payload: %v", data["id"])
	}

	if data["customerEmail"] != event.Data.CustomerEmail {
		t.Fatalf("unexpected customer email: %v", data["customerEmail"])
	}

	if data["amount"] != float64(499) {
		t.Fatalf("expected amount 499, got %v", data["amount"])
	}

	if data["status"] != "COMPLETED" {
		t.Fatalf("expected payload status COMPLETED, got %v", data["status"])
	}
}

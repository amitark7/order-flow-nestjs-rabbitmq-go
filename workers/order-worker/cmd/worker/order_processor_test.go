package main

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

func TestProcessOrderSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	event := OrderMessage{
		EventId:   "event-123",
		EventType: "order.process",
		Version:   1,
		Data: OrderData{
			ID:            "order-123",
			CustomerEmail: "customer@example.com",
			Amount:        499,
		},
	}

	mock.ExpectBegin()

	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE orders
		 SET status = 'COMPLETED', "updatedAt" = NOW()
		 WHERE id = $1 AND status = 'PENDING'`,
	)).
		WithArgs("order-123").
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT gen_random_uuid()::text`,
	)).
		WillReturnRows(
			sqlmock.NewRows([]string{"outbox_id"}).
				AddRow("outbox-123"),
		)

	mock.ExpectExec(regexp.QuoteMeta(
		`INSERT INTO outbox_events
		 (id, aggregate_id, event_type, payload)
		 VALUES ($1, $2, $3, $4::jsonb)`,
	)).
		WithArgs(
			"outbox-123",
			"order-123",
			"order.completed",
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit()

	outcome := processOrder(
		context.Background(),
		db,
		event,
		"correlation-123",
	)

	if outcome.Err != nil {
		t.Fatalf("expected success, got error: %v", outcome.Err)
	}

	if outcome.Result != OrderCompleted {
		t.Fatalf("expected %q, got %q", OrderCompleted, outcome.Result)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled database expectations: %v", err)
	}

}

func TestProcessOrderDuplicate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	event := OrderMessage{
		EventId:   "event-duplicate",
		EventType: "order.process",
		Version:   1,
		Data: OrderData{
			ID:            "order-already-completed",
			CustomerEmail: "customer@example.com",
			Amount:        499,
		},
	}

	mock.ExpectBegin()

	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE orders
		 SET status = 'COMPLETED', "updatedAt" = NOW()
		 WHERE id = $1 AND status = 'PENDING'`,
	)).
		WithArgs("order-already-completed").
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectRollback()

	outcome := processOrder(
		context.Background(),
		db,
		event,
		"correlation-duplicate",
	)

	if outcome.Err != nil {
		t.Fatalf("expected duplicate without error, got: %v", outcome.Err)
	}

	if outcome.Result != OrderDuplicate {
		t.Fatalf("expected %q, got %q", OrderDuplicate, outcome.Result)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled database expectations: %v", err)
	}
}

func TestProcessOrderUpdateFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	event := OrderMessage{
		EventId:   "event-failure",
		EventType: "order.process",
		Version:   1,
		Data: OrderData{
			ID:            "order-db-failure",
			CustomerEmail: "customer@example.com",
			Amount:        499,
		},
	}

	mock.ExpectBegin()

	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE orders
		 SET status = 'COMPLETED', "updatedAt" = NOW()
		 WHERE id = $1 AND status = 'PENDING'`,
	)).
		WithArgs("order-db-failure").
		WillReturnError(fmt.Errorf("simulated database failure"))

	mock.ExpectRollback()

	outcome := processOrder(
		context.Background(),
		db,
		event,
		"correlation-failure",
	)

	if outcome.Err == nil {
		t.Fatal("expected a database error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled database expectations: %v", err)
	}
}

func TestProcessOrderOutboxFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	event := OrderMessage{
		EventId:   "event-outbox-failure",
		EventType: "order.process",
		Version:   1,
		Data: OrderData{
			ID:            "order-outbox-failure",
			CustomerEmail: "customer@example.com",
			Amount:        499,
		},
	}

	mock.ExpectBegin()

	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE orders
		 SET status = 'COMPLETED', "updatedAt" = NOW()
		 WHERE id = $1 AND status = 'PENDING'`,
	)).
		WithArgs("order-outbox-failure").
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT gen_random_uuid()::text`,
	)).
		WillReturnRows(
			sqlmock.NewRows([]string{"outbox_id"}).
				AddRow("outbox-failure-123"),
		)

	mock.ExpectExec(regexp.QuoteMeta(
		`INSERT INTO outbox_events
		 (id, aggregate_id, event_type, payload)
		 VALUES ($1, $2, $3, $4::jsonb)`,
	)).
		WithArgs(
			"outbox-failure-123",
			"order-outbox-failure",
			"order.completed",
			sqlmock.AnyArg(),
		).
		WillReturnError(fmt.Errorf("simulated outbox insertion failure"))

	mock.ExpectRollback()

	outcome := processOrder(
		context.Background(),
		db,
		event,
		"correlation-outbox-failure",
	)

	if outcome.Err == nil {
		t.Fatal("expected an outbox insertion error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled database expectations: %v", err)
	}
}

func TestProcessOrderBeginFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin().WillReturnError(fmt.Errorf("simulated begin failure"))

	event := OrderMessage{
		EventId:   "event-begin-failure",
		EventType: "order.process",
		Version:   1,
		Data: OrderData{
			ID:            "order-begin-failure",
			CustomerEmail: "customer@example.com",
			Amount:        499,
		},
	}

	outcome := processOrder(
		context.Background(),
		db,
		event,
		"correlation-begin-failure",
	)

	if outcome.Err == nil {
		t.Fatal("expected transaction begin error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled database expectations: %v", err)
	}
}

func TestProcessOrderRowsAffectedFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	event := OrderMessage{
		EventId:   "event-rows-failure",
		EventType: "order.process",
		Version:   1,
		Data: OrderData{
			ID:            "order-rows-failure",
			CustomerEmail: "customer@example.com",
			Amount:        499,
		},
	}

	mock.ExpectBegin()

	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE orders
		 SET status = 'COMPLETED', "updatedAt" = NOW()
		 WHERE id = $1 AND status = 'PENDING'`,
	)).
		WithArgs("order-rows-failure").
		WillReturnResult(sqlmock.NewErrorResult(
			fmt.Errorf("simulated RowsAffected failure"),
		))

	mock.ExpectRollback()

	outcome := processOrder(
		context.Background(),
		db,
		event,
		"correlation-rows-failure",
	)

	if outcome.Err == nil {
		t.Fatal("expected RowsAffected error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled database expectations: %v", err)
	}
}

func TestProcessOrderOutboxIDFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	event := OrderMessage{
		EventId:   "event-id-failure",
		EventType: "order.process",
		Version:   1,
		Data: OrderData{
			ID:            "order-id-failure",
			CustomerEmail: "customer@example.com",
			Amount:        499,
		},
	}

	mock.ExpectBegin()

	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE orders
		 SET status = 'COMPLETED', "updatedAt" = NOW()
		 WHERE id = $1 AND status = 'PENDING'`,
	)).
		WithArgs("order-id-failure").
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT gen_random_uuid()::text`,
	)).
		WillReturnError(fmt.Errorf("simulated outbox ID failure"))

	mock.ExpectRollback()

	outcome := processOrder(
		context.Background(),
		db,
		event,
		"correlation-id-failure",
	)

	if outcome.Err == nil {
		t.Fatal("expected outbox ID generation error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled database expectations: %v", err)
	}
}

func TestProcessOrderCommitFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	event := OrderMessage{
		EventId:   "event-commit-failure",
		EventType: "order.process",
		Version:   1,
		Data: OrderData{
			ID:            "order-commit-failure",
			CustomerEmail: "customer@example.com",
			Amount:        499,
		},
	}

	mock.ExpectBegin()

	mock.ExpectExec(regexp.QuoteMeta(
		`UPDATE orders
		 SET status = 'COMPLETED', "updatedAt" = NOW()
		 WHERE id = $1 AND status = 'PENDING'`,
	)).
		WithArgs("order-commit-failure").
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT gen_random_uuid()::text`,
	)).
		WillReturnRows(
			sqlmock.NewRows([]string{"outbox_id"}).
				AddRow("outbox-commit-failure"),
		)

	mock.ExpectExec(regexp.QuoteMeta(
		`INSERT INTO outbox_events
		 (id, aggregate_id, event_type, payload)
		 VALUES ($1, $2, $3, $4::jsonb)`,
	)).
		WithArgs(
			"outbox-commit-failure",
			"order-commit-failure",
			"order.completed",
			sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectCommit().WillReturnError(fmt.Errorf("simulated commit failure"))

	outcome := processOrder(
		context.Background(),
		db,
		event,
		"correlation-commit-failure",
	)

	if outcome.Err == nil {
		t.Fatal("expected commit error, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled database expectations: %v", err)
	}
}

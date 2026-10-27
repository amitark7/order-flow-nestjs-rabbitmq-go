package main

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestSaveNotificationJobIdempotency(t *testing.T) {
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
	if err := db.QueryRowContext(
		ctx, "SELECT current_database()",
	).Scan(&databaseName); err != nil {
		t.Fatalf("check database name: %v", err)
	}

	if databaseName != "orderflow_go_test" {
		t.Fatalf("refusing to run against database %q", databaseName)
	}

	const eventID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	const orderID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	const correlationID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"

	_, err = db.ExecContext(ctx,
		`DELETE FROM notifications WHERE event_id = $1`, eventID)
	if err != nil {
		t.Fatalf("clean test notification: %v", err)
	}

	t.Cleanup(func() {
		_, _ = db.Exec(
			`DELETE FROM notifications WHERE event_id = $1`, eventID)
	})

	event := OrderCompletedEvent{
		EventID:       eventID,
		EventType:     "order.completed",
		Version:       1,
		OccurredAt:    time.Now().UTC().Format(time.RFC3339Nano),
		CorrelationID: correlationID,
	}
	event.Data.ID = orderID
	event.Data.CustomerEmail = "idempotency-test@example.com"
	event.Data.Amount = 499
	event.Data.Status = "COMPLETED"

	// First delivery should insert a notification job.
	inserted, err := saveNotificationJob(ctx, db, event)
	if err != nil {
		t.Fatalf("save first notification: %v", err)
	}
	if !inserted {
		t.Fatal("expected first delivery to insert a notification job")
	}

	// Second delivery of the same event must not insert another job.
	inserted, err = saveNotificationJob(ctx, db, event)
	if err != nil {
		t.Fatalf("save duplicate notification: %v", err)
	}
	if inserted {
		t.Fatal("expected duplicate delivery not to insert another job")
	}

	var count int
	err = db.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM notifications WHERE event_id = $1`,
		eventID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count notification jobs: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected exactly one notification job, got %d", count)
	}
}

func TestClaimNotificationJobRecoversStaleProcessingJob(t *testing.T) {
	if os.Getenv("POSTGRES_INTEGRATION") != "1" {
		t.Skip("set POSTGRES_INTEGRATION=1 to run PostgreSQL integration tests")
	}

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://orderflow:local_dev_password@localhost:5432/orderflow_go_test?sslmode=disable"
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	var databaseName string
	if err := db.QueryRowContext(ctx, "SELECT current_database()").Scan(&databaseName); err != nil {
		t.Fatalf("get database name: %v", err)
	}
	if databaseName != "orderflow_go_test" {
		t.Fatalf("refusing to run against database %q", databaseName)
	}

	const eventID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	const orderID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	const correlationID = "ffffffff-ffff-4fff-8fff-ffffffffffff"

	_, err = db.ExecContext(ctx, `
		DELETE FROM notifications WHERE event_id = $1
	`, eventID)
	if err != nil {
		t.Fatalf("clean up test fixture: %v", err)
	}

	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(),
			`DELETE FROM notifications WHERE event_id = $1`, eventID)
	})

	_, err = db.ExecContext(ctx, `
		INSERT INTO notifications (
			event_id, correlation_id, order_id, customer_email,
			amount, status, attempts, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, 'PROCESSING', 1,
			NOW() - INTERVAL '3 minutes')
	`, eventID, correlationID, orderID, "recovery-test@example.com", 499)
	if err != nil {
		t.Fatalf("insert stale notification: %v", err)
	}

	// Prevent unrelated pending jobs from being claimed by this test.
	_, err = db.ExecContext(ctx, `
    UPDATE notifications
    SET status = 'SENT', updated_at = NOW()
    WHERE event_id <> $1
      AND (
          status = 'PENDING'
          OR (
              status = 'PROCESSING'
              AND updated_at < NOW() - INTERVAL '2 minutes'
              AND attempts < 3
          )
      )
	`, eventID)
	if err != nil {
		t.Fatalf("isolate recovery test fixtures: %v", err)
	}

	job, err := claimNotificationJob(ctx, db)
	if err != nil {
		t.Fatalf("claim stale notification: %v", err)
	}

	if job.EventID != eventID {
		t.Fatalf("expected event ID %s, got %s", eventID, job.EventID)
	}
	if job.Attempts != 2 {
		t.Fatalf("expected claimed job attempt count 2, got %d", job.Attempts)
	}

	var status string
	var attempts int
	err = db.QueryRowContext(ctx, `
		SELECT status, attempts
		FROM notifications
		WHERE event_id = $1
	`, eventID).Scan(&status, &attempts)
	if err != nil {
		t.Fatalf("query recovered notification: %v", err)
	}

	if status != "PROCESSING" {
		t.Fatalf("expected status PROCESSING after reclaim, got %s", status)
	}
	if attempts != 2 {
		t.Fatalf("expected attempts to increase to 2, got %d", attempts)
	}
}

func TestStaleProcessingJobAtMaxAttemptsBecomesFailed(t *testing.T) {
	if os.Getenv("POSTGRES_INTEGRATION") != "1" {
		t.Skip("set POSTGRES_INTEGRATION=1 to run PostgreSQL integration tests")
	}

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://orderflow:local_dev_password@localhost:5432/orderflow_go_test?sslmode=disable"
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	var databaseName string
	if err := db.QueryRowContext(
		ctx, "SELECT current_database()",
	).Scan(&databaseName); err != nil {
		t.Fatalf("get database name: %v", err)
	}
	if databaseName != "orderflow_go_test" {
		t.Fatalf("refusing to run against database %q", databaseName)
	}

	const eventID = "11111111-1111-4111-8111-111111111111"
	const orderID = "22222222-2222-4222-8222-222222222222"
	const correlationID = "33333333-3333-4333-8333-333333333333"

	_, err = db.ExecContext(ctx, `
		DELETE FROM notifications WHERE event_id = $1
	`, eventID)
	if err != nil {
		t.Fatalf("clean up test fixture: %v", err)
	}

	t.Cleanup(func() {
		_, _ = db.ExecContext(
			context.Background(),
			`DELETE FROM notifications WHERE event_id = $1`,
			eventID,
		)
	})

	_, err = db.ExecContext(ctx, `
		INSERT INTO notifications (
			event_id, correlation_id, order_id, customer_email,
			amount, status, attempts, updated_at
		)
		VALUES (
			$1, $2, $3, $4, $5, 'PROCESSING', 3,
			NOW() - INTERVAL '3 minutes'
		)
	`, eventID, correlationID, orderID, "max-attempts-test@example.com", 499)
	if err != nil {
		t.Fatalf("insert exhausted stale notification: %v", err)
	}

	processPendingNotifications(ctx, db)

	var status, lastError string
	var attempts int
	err = db.QueryRowContext(ctx, `
		SELECT status, last_error, attempts
		FROM notifications
		WHERE event_id = $1
	`, eventID).Scan(&status, &lastError, &attempts)
	if err != nil {
		t.Fatalf("query exhausted notification: %v", err)
	}

	if status != "FAILED" {
		t.Fatalf("expected status FAILED, got %s", status)
	}
	if attempts != 3 {
		t.Fatalf("expected attempts to remain 3, got %d", attempts)
	}
	if lastError != "Worker crashed after maximum attempts" {
		t.Fatalf("unexpected last_error: %q", lastError)
	}
}

func TestConcurrentWorkersClaimDifferentJobs(t *testing.T) {
	if os.Getenv("POSTGRES_INTEGRATION") != "1" {
		t.Skip("set POSTGRES_INTEGRATION=1 to run PostgreSQL integration tests")
	}

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://orderflow:local_dev_password@localhost:5432/orderflow_go_test?sslmode=disable"
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	var databaseName string
	if err := db.QueryRowContext(ctx, "SELECT current_database()").Scan(&databaseName); err != nil {
		t.Fatalf("get database name: %v", err)
	}
	if databaseName != "orderflow_go_test" {
		t.Fatalf("refusing to run against database %q", databaseName)
	}

	const eventID1 = "44444444-4444-4444-8444-444444444444"
	const eventID2 = "55555555-5555-4555-8555-555555555555"

	_, err = db.ExecContext(ctx, `
		DELETE FROM notifications WHERE event_id IN ($1, $2)
	`, eventID1, eventID2)
	if err != nil {
		t.Fatalf("clean up test fixtures: %v", err)
	}

	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `
			DELETE FROM notifications WHERE event_id IN ($1, $2)
		`, eventID1, eventID2)
	})

	// Ensure this test only has its own eligible jobs.
	_, err = db.ExecContext(ctx, `
		UPDATE notifications
		SET status = 'SENT', updated_at = NOW()
		WHERE status = 'PENDING'
		   OR (status = 'PROCESSING'
		       AND updated_at < NOW() - INTERVAL '2 minutes'
		       AND attempts < 3)
	`)
	if err != nil {
		t.Fatalf("isolate concurrency test: %v", err)
	}

	_, err = db.ExecContext(ctx, `
		INSERT INTO notifications (
			event_id, order_id, customer_email, amount, status, attempts
		)
		VALUES
			($1, '66666666-6666-4666-8666-666666666666',
			 'concurrency-a@example.com', 100, 'PENDING', 0),
			($2, '77777777-7777-4777-8777-777777777777',
			 'concurrency-b@example.com', 200, 'PENDING', 0)
	`, eventID1, eventID2)
	if err != nil {
		t.Fatalf("insert test jobs: %v", err)
	}

	tx1, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin first transaction: %v", err)
	}
	defer tx1.Rollback()

	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin second transaction: %v", err)
	}
	defer tx2.Rollback()

	claimFromTx := func(tx *sql.Tx) (string, error) {
		var eventID string
		err := tx.QueryRowContext(ctx, `
			SELECT event_id
			FROM notifications
			WHERE status = 'PENDING'
			  AND attempts < 3
			  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
			ORDER BY created_at, id
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		`).Scan(&eventID)
		if err != nil {
			return "", err
		}

		_, err = tx.ExecContext(ctx, `
			UPDATE notifications
			SET status = 'PROCESSING',
			    attempts = attempts + 1,
			    updated_at = NOW()
			WHERE event_id = $1
		`, eventID)
		return eventID, err
	}

	firstClaim, err := claimFromTx(tx1)
	if err != nil {
		t.Fatalf("first worker claim: %v", err)
	}

	secondClaim, err := claimFromTx(tx2)
	if err != nil {
		t.Fatalf("second worker claim: %v", err)
	}

	if firstClaim == secondClaim {
		t.Fatalf("both workers claimed the same event: %s", firstClaim)
	}

	if err := tx1.Commit(); err != nil {
		t.Fatalf("commit first claim: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("commit second claim: %v", err)
	}

	t.Logf("worker 1 claimed %s; worker 2 claimed %s", firstClaim, secondClaim)
}

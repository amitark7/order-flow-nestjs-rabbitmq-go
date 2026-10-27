package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	amqp "github.com/rabbitmq/amqp091-go"
)

type OrderData struct {
	ID            string `json:"id"`
	CustomerEmail string `json:"customerEmail"`
	Amount        int    `json:"amount"`
}

type OrderMessage struct {
	EventId   string    `json:"eventId"`
	EventType string    `json:"eventType"`
	Version   int       `json:"version"`
	Data      OrderData `json:"data"`
}

type OrderProcessResult string

const (
	OrderCompleted OrderProcessResult = "completed"
	OrderDuplicate OrderProcessResult = "duplicate"
)

type OrderProcessOutcome struct {
	Result OrderProcessResult
	Err    error
}

func validateOrderMessage(event OrderMessage) bool {
	return event.EventId != "" &&
		event.EventType == "order.process" &&
		event.Version == 1 &&
		event.Data.ID != "" &&
		event.Data.CustomerEmail != "" &&
		event.Data.Amount > 0
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

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
		slog.Error("Failed to configure PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		slog.Error("Failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}

	slog.Info("Connected to PostgreSQL")

	conn, err := amqp.Dial(rabbitMqUrl)
	if err != nil {
		slog.Error("Failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}

	defer conn.Close()

	ch, err := conn.Channel()

	if err != nil {
		slog.Error("Failed to open RabbitMQ channel", "error", err)
		os.Exit(1)
	}

	defer ch.Close()

	if err := ch.Qos(1, 0, false); err != nil {
		slog.Error("Failed to configure consumer QoS", "error", err)
		os.Exit(1)
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
		slog.Error("Failed to register consumer", "error", err)
		os.Exit(1)
	}

	slog.Info("Go Order Worker waiting for messages")

	for msg := range msgs {
		correlationID := msg.CorrelationId
		var event OrderMessage

		if err := json.Unmarshal(msg.Body, &event); err != nil {
			slog.Error(
				"Invalid JSON; rejecting message",
				"correlation_id", correlationID,
				"error", err,
			)
			if err := msg.Nack(false, false); err != nil {
				slog.Error("Failed to reject message", "error", err)
			}
			continue
		}

		if !validateOrderMessage(event) {
			slog.Error(
				"Invalid order.process event; rejecting message",
				"correlation_id", correlationID,
				"order_id", event.Data.ID,
				"event_type", event.EventType,
			)

			if err := msg.Nack(false, false); err != nil {
				slog.Error("Failed to reject invalid event", "error", err)
			}
			continue
		}

		outcome := processOrder(
			context.Background(),
			db,
			event,
			correlationID,
		)

		if outcome.Err != nil {
			slog.Error(
				"Failed to process order; requeueing message",
				"correlation_id", correlationID,
				"order_id", event.Data.ID,
				"error", outcome.Err,
			)

			if nackErr := msg.Nack(false, true); nackErr != nil {
				slog.Error(
					"Failed to requeue message",
					"order_id", event.Data.ID,
					"error", nackErr,
				)
			}
			continue
		}

		if outcome.Result == OrderDuplicate {
			slog.Info(
				"Order is missing or no longer PENDING; acknowledging duplicate",
				"order_id", event.Data.ID,
			)

			if err := msg.Ack(false); err != nil {
				slog.Error(
					"Failed to acknowledge message",
					"order_id", event.Data.ID,
					"error", err,
				)
			}
			continue
		}

		if err := msg.Ack(false); err != nil {
			slog.Error(
				"Failed to acknowledge completed order",
				"order_id", event.Data.ID,
				"error", err,
			)
			continue
		}

		slog.Info(
			"Order COMPLETED; outbox event saved",
			"correlation_id", correlationID,
			"order_id", event.Data.ID,
			"status", "COMPLETED",
		)
	}
}

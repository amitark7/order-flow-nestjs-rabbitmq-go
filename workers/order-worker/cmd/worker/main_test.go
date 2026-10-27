package main

import (
	"encoding/json"
	"testing"
)

func TestValidateOrderMessage(t *testing.T) {
	tests := []struct {
		name  string
		event OrderMessage
		want  bool
	}{
		{
			name: "valid order event",
			event: OrderMessage{
				EventId:   "event-123",
				EventType: "order.process",
				Version:   1,
				Data: OrderData{
					ID:            "order-123",
					CustomerEmail: "test@example.com",
					Amount:        500,
				},
			},
			want: true,
		},
		{
			name: "missing event ID",
			event: OrderMessage{
				EventType: "order.process",
				Version:   1,
			},
			want: false,
		},
		{
			name: "wrong event type",
			event: OrderMessage{
				EventId:   "event-123",
				EventType: "order.created",
				Version:   1,
			},
			want: false,
		},
		{
			name: "unsupported version",
			event: OrderMessage{
				EventId:   "event-123",
				EventType: "order.process",
				Version:   2,
			},
			want: false,
		},
		{
			name: "zero amount",
			event: OrderMessage{
				EventId:   "event-123",
				EventType: "order.process",
				Version:   1,
				Data: OrderData{
					ID:            "order-123",
					CustomerEmail: "test@example.com",
					Amount:        0,
				},
			},
			want: false,
		},
		{
			name: "Missing Order ID",
			event: OrderMessage{
				EventId:   "event-123",
				EventType: "order.process",
				Version:   1,
				Data: OrderData{
					ID:            "",
					CustomerEmail: "test@example.com",
					Amount:        10,
				},
			},
			want: false,
		},
		{
			name: "Missing Cutomer email",
			event: OrderMessage{
				EventId:   "event-123",
				EventType: "order.process",
				Version:   1,
				Data: OrderData{
					ID:            "order-123",
					CustomerEmail: "",
					Amount:        20,
				},
			},
			want: false,
		},
		{
			name: "Negative Amount",
			event: OrderMessage{
				EventId:   "event-123",
				EventType: "order.process",
				Version:   1,
				Data: OrderData{
					ID:            "order-123",
					CustomerEmail: "test@example.com",
					Amount:        -39,
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateOrderMessage(tt.event)

			if got != tt.want {
				t.Errorf("validateOrderMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOrderMessageJSON(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr bool
	}{
		{
			name: "valid JSON",
			payload: `{
				"eventId": "event-123",
				"eventType": "order.process",
				"version": 1,
				"data": {
					"id": "order-123",
					"customerEmail": "test@example.com",
					"amount": 500
				}
			}`,
			wantErr: false,
		},
		{
			name:    "malformed JSON",
			payload: `{"eventId":`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var event OrderMessage
			err := json.Unmarshal([]byte(tt.payload), &event)

			if (err != nil) != tt.wantErr {
				t.Fatalf("json.Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err == nil && !validateOrderMessage(event) {
				t.Fatal("expected decoded event to pass validation")
			}
		})
	}
}

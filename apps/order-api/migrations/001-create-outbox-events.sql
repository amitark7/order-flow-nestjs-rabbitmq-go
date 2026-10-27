
CREATE TABLE IF NOT EXISTS order_api_outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ NULL,

    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_api_outbox_unpublished
    ON order_api_outbox_events (created_at)
    WHERE published_at IS NULL;

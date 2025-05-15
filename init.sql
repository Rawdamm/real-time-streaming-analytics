CREATE TABLE IF NOT EXISTS metrics (
    aggregation_time          TIMESTAMPTZ PRIMARY KEY,
    total_transactions        INTEGER      NOT NULL,
    total_anomalies           INTEGER      NOT NULL,
    anomaly_rate              NUMERIC(8,4) NOT NULL,
    average_amount            NUMERIC(12,4) NOT NULL,
    max_amount                NUMERIC(12,2) NOT NULL,
    min_amount                NUMERIC(12,2) NOT NULL,
    merchant_category_breakdown JSONB       NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_metrics_aggregation_time
    ON metrics (aggregation_time DESC);

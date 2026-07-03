-- mqtt_outbox table — local MySQL only
-- Created automatically by mqtt_publisher.py on first start,
-- but you can also run this manually to pre-create it.

CREATE TABLE IF NOT EXISTS mqtt_outbox (
    id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    topic      VARCHAR(255) NOT NULL,
    payload    JSON         NOT NULL,
    qos        TINYINT      NOT NULL DEFAULT 1,
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_created (created_at)
);

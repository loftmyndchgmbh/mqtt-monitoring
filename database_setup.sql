CREATE TABLE IF NOT EXISTS mqtt_status (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    publisher VARCHAR(50) NOT NULL,
    status VARCHAR(10) NOT NULL,
    message_count INT NOT NULL DEFAULT 0,
    last_message TIMESTAMP NULL DEFAULT NULL,
    ping_failed TINYINT(1) NOT NULL DEFAULT 0,
    INDEX idx_mqtt_status_timestamp (timestamp),
    INDEX idx_mqtt_status_publisher (publisher)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
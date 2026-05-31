CREATE TABLE IF NOT EXISTS consumed_events (
    id BIGINT NOT NULL AUTO_INCREMENT,
    event_id VARCHAR(80) NOT NULL,
    producer_id INT NOT NULL,
    sequence_number BIGINT NOT NULL,
    payload TEXT NOT NULL,
    generated_at TIMESTAMP(6) NOT NULL,
    consumed_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_consumed_events_event_id (event_id),
    KEY idx_consumed_events_producer_sequence (producer_id, sequence_number)
);

CREATE TABLE IF NOT EXISTS vendor_events (
    id BIGINT NOT NULL AUTO_INCREMENT,
    event_id VARCHAR(120) NOT NULL,
    vendor_name VARCHAR(120) NOT NULL,
    schema_version VARCHAR(40) NOT NULL,
    fields_json JSON NOT NULL,
    received_at TIMESTAMP(6) NOT NULL,
    consumed_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_vendor_events_vendor_event (vendor_name, event_id),
    KEY idx_vendor_events_vendor_consumed (vendor_name, consumed_at)
);

CREATE TABLE IF NOT EXISTS vendor_dead_letters (
    id BIGINT NOT NULL AUTO_INCREMENT,
    event_id VARCHAR(120),
    vendor_name VARCHAR(120),
    schema_version VARCHAR(40),
    payload_json JSON,
    source_topic VARCHAR(255) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    error_type VARCHAR(255) NOT NULL,
    error_message TEXT,
    retry_count INT NOT NULL,
    failed_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (id),
    KEY idx_vendor_dead_letters_vendor_failed (vendor_name, failed_at),
    KEY idx_vendor_dead_letters_source (source_topic, source_partition, source_offset)
);

CREATE TABLE IF NOT EXISTS vendor_callback_queue_config (
  queue_id int(11) NOT NULL AUTO_INCREMENT,
  queue_name varchar(128) DEFAULT NULL,
  cons_pool_size int(11) DEFAULT NULL,
  prod_block_queue_size int(5) unsigned DEFAULT '0',
  cons_block_queue_size int(5) unsigned DEFAULT '0',
  fetch_size int(5) unsigned DEFAULT NULL,
  producer_sleep_time bigint(20) NOT NULL DEFAULT '60000',
  consumer_sleep_time bigint(20) NOT NULL DEFAULT '60000',
  status tinyint(1) DEFAULT '1',
  refetch_interval int(11) DEFAULT '1',
  vendor_circle_flag tinyint(1) DEFAULT '0',
  vendor_name varchar(50) DEFAULT NULL,
  circle_name varchar(50) DEFAULT NULL,
  max_retry_count int(10) DEFAULT '3',
  table_name varchar(50) DEFAULT NULL,
  PRIMARY KEY (queue_id),
  UNIQUE KEY queue_name (queue_name),
  UNIQUE KEY table_name (table_name)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS sm_vendor_master (
    vendor_id INT NOT NULL AUTO_INCREMENT,
    vendor_name VARCHAR(50) NOT NULL,
    isCallbackActive TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (vendor_id),
    UNIQUE KEY uk_sm_vendor_master_name (vendor_name)
);

CREATE TABLE IF NOT EXISTS sm_vendor_operator_mapping (
    id INT NOT NULL AUTO_INCREMENT,
    vendor_id INT NOT NULL,
    operator_id VARCHAR(50) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_vendor_operator (vendor_id, operator_id)
);

CREATE TABLE IF NOT EXISTS sm_vendor_pack (
    id INT NOT NULL AUTO_INCREMENT,
    vendor_id INT NOT NULL,
    pack_id VARCHAR(50) NOT NULL,
    isactive TINYINT(1) NOT NULL DEFAULT 1,
    PRIMARY KEY (id),
    UNIQUE KEY uk_vendor_pack (vendor_id, pack_id)
);

CREATE TABLE IF NOT EXISTS sm_vendor_param_configuration (
    id INT NOT NULL AUTO_INCREMENT,
    vendor_id INT NOT NULL,
    circle VARCHAR(50) DEFAULT NULL,
    param_key VARCHAR(100) NOT NULL,
    source_field VARCHAR(100) NOT NULL,
    is_required TINYINT(1) NOT NULL DEFAULT 1,
    PRIMARY KEY (id),
    KEY idx_vendor_param (vendor_id, circle)
);

CREATE TABLE IF NOT EXISTS sm_vendor_callback_config (
    id INT NOT NULL AUTO_INCREMENT,
    vendor_id INT NOT NULL,
    circle VARCHAR(50) DEFAULT NULL,
    callback_url VARCHAR(512) NOT NULL,
    channel_url VARCHAR(512) DEFAULT NULL,
    http_method VARCHAR(10) NOT NULL DEFAULT 'POST',
    PRIMARY KEY (id),
    KEY idx_vendor_callback (vendor_id, circle)
);

CREATE TABLE IF NOT EXISTS sm_vendor_ip_mapping (
    id INT NOT NULL AUTO_INCREMENT,
    vendor_id INT NOT NULL,
    ip_address VARCHAR(45) NOT NULL,
    PRIMARY KEY (id),
    KEY idx_vendor_ip (vendor_id)
);

CREATE TABLE IF NOT EXISTS vendor_a_events (
    id BIGINT NOT NULL AUTO_INCREMENT,
    eventId varchar(120) NOT NULL,
    customerId varchar(120) NOT NULL,
    amount decimal(18, 2) NOT NULL,
    operator_id varchar(50) NOT NULL,
    pack_id varchar(50) NOT NULL,
    process_status varchar(20) NOT NULL DEFAULT 'NEW',
    retry_count int(11) NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE KEY uk_vendor_a_events_eventId (eventId),
    KEY idx_vendor_a_events_process_status (process_status)
);

CREATE TABLE IF NOT EXISTS vendor_b_events (
    id BIGINT NOT NULL AUTO_INCREMENT,
    messageId varchar(120) NOT NULL,
    accountNumber varchar(120) NOT NULL,
    status varchar(50) NOT NULL,
    process_status varchar(20) NOT NULL DEFAULT 'NEW',
    retry_count int(11) NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE KEY uk_vendor_b_events_messageId (messageId),
    KEY idx_vendor_b_events_process_status (process_status)
);

INSERT IGNORE INTO vendor_callback_queue_config
(queue_name, cons_pool_size, prod_block_queue_size, cons_block_queue_size, fetch_size,
 producer_sleep_time, consumer_sleep_time, status, refetch_interval, vendor_circle_flag,
 vendor_name, circle_name, max_retry_count, table_name)
VALUES
('vendor-a.raw', 12, 1000, 1000, 500, 5000, 60000, 1, 1, 0, 'vendor-a', NULL, 3, 'vendor_a_events'),
('vendor-b.raw', 12, 1000, 1000, 500, 60000, 60000, 1, 1, 0, 'vendor-b', NULL, 3, 'vendor_b_events');

INSERT IGNORE INTO sm_vendor_master (vendor_name, isCallbackActive) VALUES ('vendor-a', 1);

INSERT IGNORE INTO sm_vendor_operator_mapping (vendor_id, operator_id)
SELECT vendor_id, 'OP1' FROM sm_vendor_master WHERE vendor_name = 'vendor-a';

INSERT IGNORE INTO sm_vendor_pack (vendor_id, pack_id, isactive)
SELECT vendor_id, 'PACK1', 1 FROM sm_vendor_master WHERE vendor_name = 'vendor-a';

INSERT IGNORE INTO sm_vendor_callback_config (vendor_id, circle, callback_url, channel_url, http_method)
SELECT vendor_id, 'default', 'http://localhost:8080/actuator/health', NULL, 'GET'
FROM sm_vendor_master WHERE vendor_name = 'vendor-a';

INSERT IGNORE INTO sm_vendor_param_configuration (vendor_id, circle, param_key, source_field, is_required)
SELECT vendor_id, 'default', 'eventId', 'eventId', 1 FROM sm_vendor_master WHERE vendor_name = 'vendor-a';

INSERT IGNORE INTO sm_vendor_param_configuration (vendor_id, circle, param_key, source_field, is_required)
SELECT vendor_id, 'default', 'customerId', 'customerId', 1 FROM sm_vendor_master WHERE vendor_name = 'vendor-a';

INSERT IGNORE INTO sm_vendor_param_configuration (vendor_id, circle, param_key, source_field, is_required)
SELECT vendor_id, 'default', 'amount', 'amount', 1 FROM sm_vendor_master WHERE vendor_name = 'vendor-a';

INSERT IGNORE INTO vendor_a_events (eventId, customerId, amount, operator_id, pack_id, process_status, retry_count)
VALUES ('evt-demo-1', 'cust-100', 99.50, 'OP1', 'PACK1', 'NEW', 0);

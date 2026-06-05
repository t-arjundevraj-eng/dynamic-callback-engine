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
    isCallback_active TINYINT(1) NOT NULL DEFAULT 0,
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
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    PRIMARY KEY (id),
    UNIQUE KEY uk_vendor_pack (vendor_id, pack_id)
);

CREATE TABLE IF NOT EXISTS sm_vendor_param_configuration (
    id INT NOT NULL AUTO_INCREMENT,
    vendor_name VARCHAR(120) NOT NULL,
    circle_name VARCHAR(50) DEFAULT NULL,
    param VARCHAR(512) NOT NULL,
    PRIMARY KEY (id),
    KEY idx_vendor_param (vendor_name, circle_name)
);

CREATE TABLE IF NOT EXISTS sm_vendor_callback_config (
    id INT NOT NULL AUTO_INCREMENT,
    vendor_id INT NOT NULL,
    callback_url VARCHAR(512) NOT NULL,
    circle VARCHAR(50) DEFAULT NULL,
    channel_url VARCHAR(512) DEFAULT NULL,
    other_url VARCHAR(512) DEFAULT NULL,
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

-- Source queue for one97 / tanzania (table_name from vendor_callback_queue_config)
CREATE TABLE IF NOT EXISTS vendor_callback_queue_one97_tanzania (
    request_id VARCHAR(120) NOT NULL,
    msisdn VARCHAR(50) DEFAULT NULL,
    pack_name VARCHAR(50) DEFAULT NULL,
    action VARCHAR(50) DEFAULT NULL,
    channel VARCHAR(50) DEFAULT NULL,
    info VARCHAR(255) DEFAULT NULL,
    circle VARCHAR(50) DEFAULT NULL,
    username VARCHAR(50) DEFAULT NULL,
    password VARCHAR(50) DEFAULT NULL,
    start_date DATETIME DEFAULT NULL,
    end_date DATETIME DEFAULT NULL,
    transaction_id VARCHAR(120) DEFAULT NULL,
    price_point_charged VARCHAR(64) DEFAULT NULL,
    subscriber_type VARCHAR(50) DEFAULT NULL,
    status VARCHAR(20) DEFAULT NULL,
    vendor_name VARCHAR(120) DEFAULT NULL,
    next_retry_time DATETIME DEFAULT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    operator VARCHAR(50) DEFAULT NULL,
    callback_status VARCHAR(20) NOT NULL DEFAULT 'NEW',
    request_time DATETIME DEFAULT NULL,
    updation_time DATETIME DEFAULT NULL,
    params VARCHAR(512) DEFAULT NULL,
    isNotified TINYINT DEFAULT 0,
    user_params VARCHAR(512) DEFAULT NULL,
    PRIMARY KEY (request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- Source queue for paytmchemba (table_name from vendor_callback_queue_config)
CREATE TABLE IF NOT EXISTS vendor_callback_queue_paytmchemba (
    request_id VARCHAR(120) NOT NULL,
    msisdn VARCHAR(50) DEFAULT NULL,
    pack_name VARCHAR(50) DEFAULT NULL,
    action VARCHAR(50) DEFAULT NULL,
    channel VARCHAR(50) DEFAULT NULL,
    info VARCHAR(255) DEFAULT NULL,
    circle VARCHAR(50) DEFAULT NULL,
    username VARCHAR(50) DEFAULT NULL,
    password VARCHAR(50) DEFAULT NULL,
    start_date DATETIME DEFAULT NULL,
    end_date DATETIME DEFAULT NULL,
    transaction_id VARCHAR(120) DEFAULT NULL,
    price_point_charged VARCHAR(64) DEFAULT NULL,
    subscriber_type VARCHAR(50) DEFAULT NULL,
    status VARCHAR(20) DEFAULT NULL,
    vendor_name VARCHAR(120) DEFAULT NULL,
    next_retry_time DATETIME DEFAULT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    operator VARCHAR(50) DEFAULT NULL,
    callback_status VARCHAR(20) NOT NULL DEFAULT 'NEW',
    request_time DATETIME DEFAULT NULL,
    updation_time DATETIME DEFAULT NULL,
    params VARCHAR(512) DEFAULT NULL,
    isNotified TINYINT DEFAULT 0,
    user_params VARCHAR(512) DEFAULT NULL,
    PRIMARY KEY (request_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT IGNORE INTO vendor_callback_queue_config
(queue_name, cons_pool_size, prod_block_queue_size, cons_block_queue_size, fetch_size,
 producer_sleep_time, consumer_sleep_time, status, vendor_circle_flag,
 vendor_name, circle_name, max_retry_count, table_name)
VALUES
('queue_callback_one97', 12, 1000, 1000, 50, 5000, 60000, 1, 1, 'one97', 'tanzania', 3,
 'vendor_callback_queue_one97_tanzania'),
('queue_callback_paytmchemba', 12, 1000, 1000, 50, 5000, 60000, 1, 0, 'paytmchemba', NULL, 3,
 'vendor_callback_queue_paytmchemba');

INSERT IGNORE INTO sm_vendor_master (vendor_name, isCallback_active) VALUES ('one97', 1);
INSERT IGNORE INTO sm_vendor_master (vendor_name, isCallback_active) VALUES ('paytmchemba', 1);

INSERT IGNORE INTO sm_vendor_operator_mapping (vendor_id, operator_id)
SELECT vendor_id, 'OP1' FROM sm_vendor_master WHERE vendor_name = 'one97';
INSERT IGNORE INTO sm_vendor_operator_mapping (vendor_id, operator_id)
SELECT vendor_id, 'OP1' FROM sm_vendor_master WHERE vendor_name = 'paytmchemba';

INSERT IGNORE INTO sm_vendor_pack (vendor_id, pack_id, is_active)
SELECT vendor_id, 'PACK1', 1 FROM sm_vendor_master WHERE vendor_name = 'one97';
INSERT IGNORE INTO sm_vendor_pack (vendor_id, pack_id, is_active)
SELECT vendor_id, 'PACK1', 1 FROM sm_vendor_master WHERE vendor_name = 'paytmchemba';

INSERT IGNORE INTO sm_vendor_callback_config (vendor_id, circle, callback_url, channel_url, other_url)
SELECT vendor_id, 'tanzania', 'http://localhost:8080/actuator/health', NULL, NULL
FROM sm_vendor_master WHERE vendor_name = 'one97';

INSERT IGNORE INTO sm_vendor_callback_config (vendor_id, circle, callback_url, channel_url, other_url)
SELECT vendor_id, 'default', 'http://localhost:8080/actuator/health', NULL, NULL
FROM sm_vendor_master WHERE vendor_name = 'paytmchemba';

INSERT IGNORE INTO sm_vendor_param_configuration (vendor_name, circle_name, param) 
VALUES ('one97', 'tanzania', 'event_id,customer_id,amount');

INSERT IGNORE INTO sm_vendor_param_configuration (vendor_name, circle_name, param) 
VALUES ('paytmchemba', 'default', 'transaction_id,msisdn,amount');

INSERT IGNORE INTO vendor_callback_queue_one97_tanzania 
(callback_status, retry_count, operator, pack_name, request_id, msisdn, price_point_charged) 
VALUES ('NEW', 0, 'OP1', 'PACK1', 'evt-one97-1', 'cust-100', '99.50');

INSERT IGNORE INTO vendor_callback_queue_paytmchemba 
(request_id, callback_status, retry_count, operator, pack_name, transaction_id, msisdn, price_point_charged) 
VALUES ('99999', 'NEW', 0, 'OP1', 'PACK1', 'txn-paytm-1', '255700000001', '1500.00');

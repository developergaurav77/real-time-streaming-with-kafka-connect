
CREATE DATABASE transactions;
CREATE TABLE transaction_details (
    txn_id BIGINT PRIMARY KEY,
    sender_account_id BIGINT NOT NULL,
    receiver_account_id BIGINT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    last_modified_date TIMESTAMP DEFAULT NULL,
    product_id INTEGER,
    product_type_id INTEGER,
    transactor_module_id INTEGER,
    module_id INTEGER,
    status INTEGER
);

CREATE DATABASE rp;
USE rp;
CREATE TABLE rp.`rp_agg_15min_transactions` (
  `window_start` timestamp,
  `window_end` timestamp,
  `total_amount` decimal(22,2) DEFAULT NULL,
  `txn_count` bigint NOT NULL,
  PRIMARY KEY (`window_start`) );
  
  CREATE TABLE rp.`rp_stage_agg_15min_transactions` (
  `window_start` timestamp,
  `window_end` timestamp,
  `total_amount` decimal(22,2) DEFAULT NULL,
  `txn_count` bigint NOT NULL,
  PRIMARY KEY (`window_start`) );

  drop table rp.rp_stage_agg_15min_transactions;

  CREATE TABLE rp.`rp_stage_agg_15min_transactions` (
  `window_start` timestamp,
  `window_end` timestamp,
  `total_amount` decimal(22,2) DEFAULT NULL,
  `txn_count` bigint NOT NULL );

ALTER TABLE rp.rp_agg_15min_transactions
  ADD COLUMN created_at TIMESTAMP
    NOT NULL
    DEFAULT CURRENT_TIMESTAMP
    AFTER txn_count;

-- optional
SET GLOBAL sql_mode = 'ALLOW_INVALID_DATES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';


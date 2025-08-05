
CREATE DATABASE transactions;
USE transactions;
CREATE TABLE transaction_details (
    txn_id BIGINT PRIMARY KEY,
    sender_account_id BIGINT NOT NULL,
    receiver_account_id BIGINT NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    last_modified_date TIMESTAMP NOT NULL,
    product_id INTEGER,
    product_type_id INTEGER,
    transactor_module_id INTEGER,
    module_id INTEGER,
    status INTEGER
);


-- verify if replocation_slot created on source db or not
SELECT slot_name, plugin, active, restart_lsn
  FROM pg_replication_slots;
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    name varchar(255),
    email varchar(255),
    country varchar(255),
    city varchar(255),
    latitude float,
    longitude float,
    created_at timestamp
);

CREATE TABLE user_devices (
    device_id SERIAL PRIMARY KEY,
    user_id integer NOT NULL,
    device_type varchar(255),
    first_used timestamp,
    last_used timestamp
);

CREATE TABLE payment_methods (
    payment_method_id SERIAL PRIMARY KEY,
    user_id integer NOT NULL,
    payment_method varchar(255),
    payment_service_provider varchar(255)
);

CREATE TABLE merchants (
    merchant_id SERIAL PRIMARY KEY,
    merchant_name varchar(255),
    country varchar(255),
    rating varchar(255)
);

CREATE TABLE transactions (
    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_amount_local NUMERIC(15,2),
    transaction_amount_usd NUMERIC(15,2),
    transaction_timestamp timestamp,
    transaction_status varchar(255),
    transaction_currency varchar(255),
    transaction_country varchar(255),
    transaction_channel varchar(255),
    user_id integer NOT NULL,
    merchant_id integer NOT NULL,
    payment_id integer NOT NULL,
    device_id integer NOT NULL,
    is_fraudulent integer,
    fraud_type varchar(255)
);

ALTER TABLE transactions
    ADD CONSTRAINT fk_transaction_user
        FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE RESTRICT;

ALTER TABLE transactions
    ADD CONSTRAINT fk_transaction_merchant
        FOREIGN KEY (merchant_id) REFERENCES merchants (merchant_id);

ALTER TABLE transactions
    ADD CONSTRAINT fk_transaction_payment
        FOREIGN KEY (payment_id) REFERENCES payment_methods (payment_method_id);

ALTER TABLE payment_methods
    ADD CONSTRAINT fk_payment_user
        FOREIGN KEY (user_id) REFERENCES users (user_id);

ALTER TABLE transactions
    ADD CONSTRAINT fk_transaction_device
        FOREIGN KEY (device_id) REFERENCES user_devices (device_id);

ALTER TABLE user_devices
    ADD CONSTRAINT fk_device_user
        FOREIGN KEY (user_id) REFERENCES users (user_id);
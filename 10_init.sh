#!/bin/bash
set -e

echo $POSTGRES_USER

# Create additional databases (connected to default 'postgres' DB)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    create role account with login password 'example';
    create database account owner account;
    grant all privileges on database account TO account;

    create role explore with login password 'example';
    create database explore owner explore;
    grant all privileges on database explore TO explore;
EOSQL

echo "--------------------1111  $?"

# Create schemas in each new database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "account" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS account authorization account;
    GRANT USAGE ON SCHEMA account TO account;

    ALTER DEFAULT PRIVILEGES IN SCHEMA account GRANT ALL ON TABLES TO account;
    ALTER DEFAULT PRIVILEGES IN SCHEMA account GRANT ALL ON SEQUENCES TO account;

    ALTER DATABASE account SET search_path TO account;
    ALTER ROLE account SET search_path TO account;

EOSQL

echo "-----------------------------2222  $?"


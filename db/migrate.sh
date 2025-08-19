#!/bin/sh
set -e

export PGPASSWORD="$DB_PASSWORD"

echo "Waiting for database to be ready..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME"; do 
    echo "Waiting for database..."; 
    sleep 2; 
done

echo "Database is ready. Running migration..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -f /migrations/V1__create_table.sql

echo "Migration completed successfully."

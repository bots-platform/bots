-- Initialize RPA Bots Database
-- This script runs when the PostgreSQL container starts for the first time

-- Create extensions if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Create additional databases if needed
-- CREATE DATABASE rpa_bots_test;

-- Create additional users if needed
-- CREATE USER rpa_test_user WITH PASSWORD 'test_password';
-- GRANT ALL PRIVILEGES ON DATABASE rpa_bots_test TO rpa_test_user;

-- Set timezone
SET timezone = 'UTC';

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'RPA Bots database initialized successfully';
END $$; 
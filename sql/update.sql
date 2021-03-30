-- Update tests. Check that the whole sequence of extension updates can be applied successfully.
-- These tests do not require a Kafka instance. They can be run as common tests.

-- start_ignore
DROP EXTENSION IF EXISTS kadb_fdw CASCADE;

-- Prevent WARNINGs that ALTER EXTENSION may invalidate OPTIONs
SET client_min_messages = ERROR;
-- end_ignore


CREATE EXTENSION kadb_fdw WITH VERSION '0.5';

ALTER EXTENSION kadb_fdw UPDATE TO '0.6';
ALTER EXTENSION kadb_fdw UPDATE TO '0.7';
ALTER EXTENSION kadb_fdw UPDATE TO '0.8';
ALTER EXTENSION kadb_fdw UPDATE TO '0.9';
ALTER EXTENSION kadb_fdw UPDATE TO '0.10';
ALTER EXTENSION kadb_fdw UPDATE TO '0.10.1';
ALTER EXTENSION kadb_fdw UPDATE TO '0.10.2';

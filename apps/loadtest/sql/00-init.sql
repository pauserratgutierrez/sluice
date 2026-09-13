-- Roles and publications for the load-test app.
-- Same Sluice contract as a real deployment: two roles, two publications, no
-- Sluice-owned schemas. No GoTrue, no PostgREST.

\set ON_ERROR_STOP on

\getenv postgres_db POSTGRES_DB
\getenv sluice_repl_password SLUICE_REPL_PASSWORD
\getenv sluice_authz_password SLUICE_AUTHZ_PASSWORD

CREATE SCHEMA IF NOT EXISTS auth;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;

CREATE ROLE anon NOLOGIN;
CREATE ROLE authenticated NOLOGIN;
CREATE ROLE service_role NOLOGIN BYPASSRLS;

GRANT USAGE ON SCHEMA public, auth TO anon, authenticated, service_role;

CREATE ROLE sluice_repl WITH LOGIN REPLICATION PASSWORD :'sluice_repl_password';

CREATE ROLE sluice_authz WITH LOGIN NOINHERIT PASSWORD :'sluice_authz_password';
GRANT anon, authenticated TO sluice_authz;
GRANT pg_monitor TO sluice_authz;

CREATE PUBLICATION sluice_rls;
CREATE PUBLICATION sluice_issuer;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO service_role;

ALTER DATABASE :"postgres_db" SET search_path TO public, auth;

-- Sluice bootstrap. Runs once on a fresh volume.
--
-- Everything Sluice itself needs is:
--
--   * wal_level=logical                      (compose command flags)
--   * a role with LOGIN REPLICATION          (sluice_repl)
--   * a role with LOGIN NOINHERIT            (sluice_authz)
--   * a publication                          (sluice)
--
-- Nothing else. No extensions, no tables, no functions, no schemas owned by
-- Sluice. The auth/PostgREST parts below exist only because the test harness
-- needs a realistic JWT issuer and an authorization oracle.

\set ON_ERROR_STOP on

\getenv postgres_db POSTGRES_DB
\getenv auth_db_password AUTH_DB_PASSWORD
\getenv pgrst_auth_password PGRST_AUTH_PASSWORD
\getenv sluice_repl_password SLUICE_REPL_PASSWORD
\getenv sluice_authz_password SLUICE_AUTHZ_PASSWORD

-- ---------------------------------------------------------------------------
-- Schemas
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS extensions;
CREATE SCHEMA IF NOT EXISTS auth;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;

CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA extensions;

-- ---------------------------------------------------------------------------
-- Application roles (the ones RLS policies are written against)
-- ---------------------------------------------------------------------------
CREATE ROLE anon NOLOGIN;
CREATE ROLE authenticated NOLOGIN;
CREATE ROLE service_role NOLOGIN BYPASSRLS;

GRANT USAGE ON SCHEMA public, extensions TO anon, authenticated, service_role;

-- ---------------------------------------------------------------------------
-- SLUICE ROLE 1: replication.
--
-- Deliberately has NO table privileges. Verified empirically: a role with the
-- REPLICATION attribute and zero grants streams every column of every
-- published table, because logical decoding is not subject to RLS or grants.
-- That is exactly why this role is used for nothing else -- its credential is
-- as sensitive as a superuser's.
-- ---------------------------------------------------------------------------
CREATE ROLE sluice_repl WITH LOGIN REPLICATION PASSWORD :'sluice_repl_password';

-- ---------------------------------------------------------------------------
-- SLUICE ROLE 2: authorization.
--
-- Mirrors PostgREST's `authenticator`. NOINHERIT is load-bearing: sluice_authz
-- can only ASSUME anon/authenticated via SET LOCAL ROLE, never use their
-- privileges implicitly. All impersonation is transaction-scoped, so a
-- connection returned to the pool can never carry a stale role.
-- ---------------------------------------------------------------------------
CREATE ROLE sluice_authz WITH LOGIN NOINHERIT PASSWORD :'sluice_authz_password';
GRANT anon, authenticated TO sluice_authz;

-- Reading pg_policy / pg_class / pg_attribute / pg_index needs no grant.
-- Reading pg_publication_tables needs no grant.
-- has_column_privilege() needs no grant.
-- Snapshots run through SET LOCAL ROLE, so no direct table grant is required.

-- Allows Sluice to hold the single-reader advisory lock and to observe slots.
GRANT pg_monitor TO sluice_authz;

-- ---------------------------------------------------------------------------
-- THE PUBLICATION.
--
-- Created empty on purpose. Tables opt in explicitly; FOR ALL TABLES is
-- supported by Sluice but discouraged.
--
-- Note there are deliberately NO row filters and NO column lists here.
-- Verified: a column used in a publication WHERE expression must be part of
-- the replica identity, and when it is not, the application's UPDATE and
-- DELETE statements FAIL:
--
--   ERROR:  cannot update table "docs"
--   DETAIL: Column used in the publication WHERE expression is not part of
--           the replica identity.
--
-- A filtering mechanism that can break writes is not acceptable. Sluice
-- filters in process, per subscriber.
-- ---------------------------------------------------------------------------
CREATE PUBLICATION sluice;

-- ---------------------------------------------------------------------------
-- Harness only: GoTrue
-- ---------------------------------------------------------------------------
CREATE ROLE supabase_auth_admin NOINHERIT CREATEROLE LOGIN NOREPLICATION
  PASSWORD :'auth_db_password';
GRANT CREATE ON DATABASE :"postgres_db" TO supabase_auth_admin;
ALTER SCHEMA auth OWNER TO supabase_auth_admin;
ALTER ROLE supabase_auth_admin SET search_path TO auth;
GRANT USAGE ON SCHEMA auth TO service_role;

-- NOTE: auth.uid() and friends are deliberately NOT created here.
--
-- GoTrue's first migration does `CREATE OR REPLACE FUNCTION auth.uid()`, and it
-- connects as supabase_auth_admin. If postgres created those functions first,
-- GoTrue's migration fails with "must be owner of function uid" and the service
-- crash-loops. The harness therefore lets GoTrue create them, and fixtures.sql
-- upgrades them afterwards to the modern claims-based definitions.

-- ---------------------------------------------------------------------------
-- Harness only: PostgREST
-- ---------------------------------------------------------------------------
CREATE ROLE authenticator WITH LOGIN NOINHERIT PASSWORD :'pgrst_auth_password';
GRANT anon, authenticated, service_role TO authenticator;

-- ---------------------------------------------------------------------------
-- Default privileges: nothing is exposed until explicitly granted.
-- ---------------------------------------------------------------------------
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO service_role;

ALTER DATABASE :"postgres_db" SET search_path TO public, extensions;

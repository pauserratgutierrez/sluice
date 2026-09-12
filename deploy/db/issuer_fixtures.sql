-- Issuer-oracle overlay fixtures.
--
-- Own publication, own tables. RLS off. Not added to publication `sluice`.
-- auth.sessions / auth.users stay off this publication: issuer startup validation
-- requires SELECT + (BYPASSRLS or RLS off) on every published table, and GRANT
-- BYPASSRLS to sluice_authz would poison the RLS process that shares the role.
--
-- Idempotent: safe to re-run after fixtures.sql.

\set ON_ERROR_STOP on

BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'sluice_issuer') THEN
    CREATE PUBLICATION sluice_issuer;
  END IF;
END $$;

DROP TABLE IF EXISTS public.iss_documents CASCADE;
DROP TABLE IF EXISTS public.iss_project_members CASCADE;

-- Hold table. UNIQUE (project_id, user_id) is the replica identity: a PK on id
-- alone would not put those columns in the old tuple, so a DELETE could not cut.
CREATE TABLE public.iss_project_members (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id text NOT NULL,
  user_id    uuid NOT NULL
);

CREATE UNIQUE INDEX iss_project_members_ri ON public.iss_project_members (project_id, user_id);
ALTER TABLE public.iss_project_members REPLICA IDENTITY USING INDEX iss_project_members_ri;

CREATE TABLE public.iss_documents (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id text NOT NULL,
  title      text NOT NULL,
  body       text
);

CREATE INDEX iss_documents_project_id ON public.iss_documents (project_id);

-- Snapshots and hold EXISTS run as sluice_authz with no SET ROLE.
GRANT SELECT ON public.iss_project_members, public.iss_documents TO sluice_authz;

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'public.iss_project_members', 'public.iss_documents'
  ] LOOP
    IF NOT EXISTS (
      SELECT 1 FROM pg_publication_tables
       WHERE pubname = 'sluice_issuer'
         AND schemaname || '.' || tablename = t
    ) THEN
      EXECUTE format('ALTER PUBLICATION sluice_issuer ADD TABLE %s', t);
    END IF;
  END LOOP;
END $$;

COMMIT;

SELECT c.oid::regclass AS relation,
       c.relrowsecurity AS rls,
       c.relreplident   AS replica_identity
FROM pg_publication_tables pt
JOIN pg_class c ON c.oid = (pt.schemaname || '.' || pt.tablename)::regclass
WHERE pt.pubname = 'sluice_issuer'
ORDER BY 1;

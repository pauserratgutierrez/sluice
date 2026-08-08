-- Sluice test fixtures.
--
-- Each table is chosen to force the authorizer down a specific tier, so the
-- test suite can assert that the tier classification is correct AND that the
-- delivered events match what PostgREST would let the same JWT SELECT.
--
--   documents  -> Tier A  (policy references only the shape's filter column)
--   posts      -> Tier B  (policy is pure over the row, but row-dependent)
--   invoices   -> Tier C  (policy contains a subquery against another table)
--   metrics    -> Tier A  (RLS disabled entirely)
--   articles   -> Tier A, but with a deliberately inadequate replica identity
--                 and a TOAST-able column, to exercise the warning paths
--
-- Idempotent: safe to re-run.

\set ON_ERROR_STOP on

BEGIN;

-- ===========================================================================
-- JWT helpers.
--
-- GoTrue's migration 00 creates auth.uid() and auth.role() reading the LEGACY
-- singular GUCs (`request.jwt.claim.sub`). Modern Supabase overrides them to read
-- the JSON claim set from `request.jwt.claims`, which is what PostgREST and
-- Sluice both set. Upgrading them here means the fixtures exercise the same
-- function bodies a real deployment has.
--
-- Sluice's predicate compiler understands both spellings, so a deployment that
-- never upgraded these still resolves to Tier A.
--
-- Runs as postgres (superuser), after GoTrue has created its versions.
-- ===========================================================================
CREATE OR REPLACE FUNCTION auth.uid() RETURNS uuid
  LANGUAGE sql STABLE AS $$
  SELECT nullif(current_setting('request.jwt.claims', true)::jsonb ->> 'sub', '')::uuid
$$;

CREATE OR REPLACE FUNCTION auth.role() RETURNS text
  LANGUAGE sql STABLE AS $$
  SELECT nullif(current_setting('request.jwt.claims', true)::jsonb ->> 'role', '')::text
$$;

CREATE OR REPLACE FUNCTION auth.email() RETURNS text
  LANGUAGE sql STABLE AS $$
  SELECT nullif(current_setting('request.jwt.claims', true)::jsonb ->> 'email', '')::text
$$;

CREATE OR REPLACE FUNCTION auth.jwt() RETURNS jsonb
  LANGUAGE sql STABLE AS $$
  SELECT coalesce(nullif(current_setting('request.jwt.claims', true), ''), '{}')::jsonb
$$;

GRANT USAGE ON SCHEMA auth TO anon, authenticated, service_role, sluice_authz;
GRANT EXECUTE ON FUNCTION auth.uid(), auth.role(), auth.email(), auth.jwt()
  TO anon, authenticated, service_role, sluice_authz;

-- ===========================================================================
-- TIER A: policy references only owner_id, which shapes will pin to a constant
--
-- REPLICA IDENTITY USING INDEX is the recommendation from.
-- Measured against REPLICA IDENTITY FULL on a table with a 300 KB TOAST-able
-- column: 93-byte DELETE messages instead of 300,127, and 248 WAL bytes
-- instead of 3,704.
-- ===========================================================================
DROP TABLE IF EXISTS public.documents CASCADE;
CREATE TABLE public.documents (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id   uuid        NOT NULL,
  title      text        NOT NULL,
  body       text,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- unique, non-partial, no expressions, all columns NOT NULL -> eligible
CREATE UNIQUE INDEX documents_ri ON public.documents (owner_id, id);
ALTER TABLE public.documents REPLICA IDENTITY USING INDEX documents_ri;

ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
CREATE POLICY documents_own ON public.documents
  FOR SELECT TO authenticated
  USING (owner_id = auth.uid());

-- Write access too, so the test suite can exercise the SDK end to end as a real
-- user rather than as a superuser. Separate per-command policies keep the SELECT
-- policy set clean: Sluice's combined predicate only reads polcmd 'r' and '*',
-- so a FOR ALL policy here would needlessly duplicate the read predicate.
CREATE POLICY documents_insert ON public.documents
  FOR INSERT TO authenticated WITH CHECK (owner_id = auth.uid());
CREATE POLICY documents_update ON public.documents
  FOR UPDATE TO authenticated USING (owner_id = auth.uid()) WITH CHECK (owner_id = auth.uid());
CREATE POLICY documents_delete ON public.documents
  FOR DELETE TO authenticated USING (owner_id = auth.uid());

GRANT SELECT, INSERT, UPDATE, DELETE ON public.documents TO authenticated;
GRANT ALL ON public.documents TO service_role;

-- ===========================================================================
-- TIER B: pure over the row, but row-dependent.
--
-- A shape filtered on owner_id alone cannot reduce this predicate to a
-- constant, because it also references `visibility`. It IS compilable in
-- process, so it costs zero database round trips per change -- and DELETE
-- authorization works correctly, which Supabase documents as impossible.
-- ===========================================================================
DROP TABLE IF EXISTS public.posts CASCADE;
CREATE TABLE public.posts (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id   uuid NOT NULL,
  visibility text NOT NULL DEFAULT 'private'
             CHECK (visibility IN ('private','unlisted','public')),
  title      text NOT NULL,
  score      int  NOT NULL DEFAULT 0
);

-- REPLICA IDENTITY FULL here for two reasons: it gives the test suite a complete
-- old tuple to check Tier B DELETE authorization against, AND it deliberately
-- trips the `replica_identity_full_with_toast` warning, because `title text` has
-- attstorage 'x' like every text column. Rows here are small so the actual
-- amplification is negligible, but the warning must still fire -- Sluice cannot
-- know a column is small, only that it is TOAST-able.
ALTER TABLE public.posts REPLICA IDENTITY FULL;

ALTER TABLE public.posts ENABLE ROW LEVEL SECURITY;
CREATE POLICY posts_visible ON public.posts
  FOR SELECT TO authenticated
  USING (visibility = 'public' OR owner_id = auth.uid());

GRANT SELECT ON public.posts TO authenticated;
GRANT ALL    ON public.posts TO service_role;

-- ===========================================================================
-- TIER C: subquery against another table. Not compilable in process.
--
-- Also carries a RESTRICTIVE policy, so the test suite can assert that the
-- combined predicate ANDs restrictive policies rather than ORing them in.
-- ===========================================================================
DROP TABLE IF EXISTS public.invoices CASCADE;
DROP TABLE IF EXISTS public.memberships CASCADE;

CREATE TABLE public.memberships (
  user_id uuid NOT NULL,
  team_id int  NOT NULL,
  PRIMARY KEY (user_id, team_id)
);

CREATE TABLE public.invoices (
  id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  team_id int    NOT NULL,
  amount  numeric(12,2) NOT NULL,
  note    text
);

CREATE UNIQUE INDEX invoices_ri ON public.invoices (team_id, id);
ALTER TABLE public.invoices REPLICA IDENTITY USING INDEX invoices_ri;

ALTER TABLE public.invoices ENABLE ROW LEVEL SECURITY;

CREATE POLICY invoices_team_member ON public.invoices
  FOR SELECT TO authenticated
  USING (EXISTS (
    SELECT 1 FROM public.memberships m
     WHERE m.team_id = invoices.team_id
       AND m.user_id = auth.uid()));

CREATE POLICY invoices_no_zero ON public.invoices
  AS RESTRICTIVE FOR SELECT TO authenticated
  USING (amount > 0);

ALTER TABLE public.memberships ENABLE ROW LEVEL SECURITY;
CREATE POLICY memberships_own ON public.memberships
  FOR SELECT TO authenticated
  USING (user_id = auth.uid());

GRANT SELECT ON public.invoices, public.memberships TO authenticated;
GRANT ALL    ON public.invoices, public.memberships TO service_role;

-- ===========================================================================
-- TIER A via "no RLS at all": the predicate is TRUE, so every subscriber is
-- authorized unconditionally with zero work.
-- ===========================================================================
DROP TABLE IF EXISTS public.metrics CASCADE;
CREATE TABLE public.metrics (
  id    bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name  text NOT NULL,
  value double precision NOT NULL,
  at    timestamptz NOT NULL DEFAULT now()
);
GRANT SELECT ON public.metrics TO anon, authenticated;
GRANT ALL    ON public.metrics TO service_role;

-- ===========================================================================
-- Warning paths: a TOAST-able column plus REPLICA IDENTITY DEFAULT.
--
-- A shape filtering on owner_id here MUST produce a
-- `replica_identity_insufficient` warning with a runnable remedy, because the
-- old tuple carries only the primary key, so DELETE events cannot be filtered
-- or authorized on owner_id.
--
-- The body column also exercises the unchanged-TOAST 'u' marker: an UPDATE
-- that only touches `views` must arrive with `unchanged: ["body"]`, never with
-- body silently absent. That distinction is the whole reason Sluice uses
-- pgoutput instead of wal2json.
-- ===========================================================================
DROP TABLE IF EXISTS public.articles CASCADE;
CREATE TABLE public.articles (
  id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  views    int  NOT NULL DEFAULT 0,
  body     text                        -- storage 'x', i.e. TOAST-able
);
-- deliberately left at REPLICA IDENTITY DEFAULT

ALTER TABLE public.articles ENABLE ROW LEVEL SECURITY;
CREATE POLICY articles_own ON public.articles
  FOR SELECT TO authenticated
  USING (owner_id = auth.uid());

GRANT SELECT ON public.articles TO authenticated;
GRANT ALL    ON public.articles TO service_role;

-- ===========================================================================
-- Publication membership. Explicit opt-in, no row filters, no column lists.
-- ===========================================================================
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'public.documents', 'public.posts', 'public.invoices',
    'public.memberships', 'public.metrics', 'public.articles'
  ] LOOP
    IF NOT EXISTS (
      SELECT 1 FROM pg_publication_tables
       WHERE pubname = 'sluice'
         AND schemaname || '.' || tablename = t
    ) THEN
      EXECUTE format('ALTER PUBLICATION sluice ADD TABLE %s', t);
    END IF;
  END LOOP;
END $$;

-- Session revocation: Sluice tails these on the same slot.
-- auth.sessions has sessions_pkey (id) and auth.users has users_pkey (id), so
-- both carry old-tuple keys on DELETE/UPDATE at REPLICA IDENTITY DEFAULT.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='auth' AND tablename='sessions')
     AND NOT EXISTS (SELECT 1 FROM pg_publication_tables
                      WHERE pubname='sluice' AND schemaname='auth' AND tablename='sessions') THEN
    ALTER PUBLICATION sluice ADD TABLE auth.sessions;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='auth' AND tablename='users')
     AND NOT EXISTS (SELECT 1 FROM pg_publication_tables
                      WHERE pubname='sluice' AND schemaname='auth' AND tablename='users') THEN
    ALTER PUBLICATION sluice ADD TABLE auth.users;
  END IF;
END $$;

-- ===========================================================================
-- Seed helper. The test suite signs a user up through GoTrue (so the JWT is
-- genuinely ES256-signed and carries a real session_id), then calls this with
-- the resulting user id.
-- ===========================================================================
CREATE OR REPLACE FUNCTION public.sluice_seed(p_owner uuid, p_team int DEFAULT 1)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
  INSERT INTO documents (owner_id, title, body)
    VALUES (p_owner, 'owned doc', 'hello'),
           (gen_random_uuid(), 'someone elses doc', 'secret');

  INSERT INTO posts (owner_id, visibility, title)
    VALUES (p_owner,            'private',  'my private post'),
           (gen_random_uuid(),  'public',   'a public post'),
           (gen_random_uuid(),  'private',  'not for you');

  INSERT INTO memberships (user_id, team_id) VALUES (p_owner, p_team)
    ON CONFLICT DO NOTHING;
  INSERT INTO invoices (team_id, amount, note)
    VALUES (p_team, 100.00, 'in my team'),
           (p_team,   0.00, 'zero, blocked by the restrictive policy'),
           (p_team + 999, 50.00, 'other team');

  INSERT INTO metrics (name, value) VALUES ('boot', 1);

  INSERT INTO articles (owner_id, views, body)
    VALUES (p_owner, 0, repeat('X', 200000));
END $$;

REVOKE EXECUTE ON FUNCTION public.sluice_seed(uuid, int) FROM PUBLIC;
GRANT  EXECUTE ON FUNCTION public.sluice_seed(uuid, int) TO service_role;

COMMIT;

-- PostgREST caches the schema at startup and only reloads on this notification.
-- Without it the oracle would report zero relations and every comparison test
-- would trivially "pass".
NOTIFY pgrst, 'reload schema';

-- Report what a fresh harness looks like, so `docker compose logs fixtures`
-- is a useful diagnostic on its own.
SELECT c.oid::regclass AS relation,
       c.relrowsecurity AS rls,
       c.relreplident   AS replica_identity,
       (SELECT count(*) FROM pg_policy p WHERE p.polrelid = c.oid) AS policies
FROM pg_publication_tables pt
JOIN pg_class c ON c.oid = (pt.schemaname || '.' || pt.tablename)::regclass
WHERE pt.pubname = 'sluice'
ORDER BY 1;

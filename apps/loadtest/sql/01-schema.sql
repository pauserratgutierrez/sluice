-- Application schema. Written like a small SaaS that happens to stream with
-- Sluice: one table per authorization cost, plus an issuer-hold pair.
--
--   notes           RLS tier A  — policy reads only owner_id, shape pins it
--   posts           RLS tier B  — row-dependent, compilable
--   invoices        RLS tier C  — EXISTS over team_members
--   project_docs    issuer      — subscribed table, RLS off
--   project_members issuer      — hold rows, RLS off
--
-- Runs once on a fresh volume (docker-entrypoint-initdb.d).

\set ON_ERROR_STOP on

BEGIN;

CREATE OR REPLACE FUNCTION auth.uid() RETURNS uuid
  LANGUAGE sql STABLE AS $$
  SELECT nullif(current_setting('request.jwt.claims', true)::jsonb ->> 'sub', '')::uuid
$$;

CREATE OR REPLACE FUNCTION auth.role() RETURNS text
  LANGUAGE sql STABLE AS $$
  SELECT nullif(current_setting('request.jwt.claims', true)::jsonb ->> 'role', '')::text
$$;

GRANT EXECUTE ON FUNCTION auth.uid(), auth.role() TO anon, authenticated, service_role, sluice_authz;

-- ---------------------------------------------------------------------------
-- Tier A
-- ---------------------------------------------------------------------------
CREATE TABLE public.notes (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id   uuid        NOT NULL,
  title      text        NOT NULL,
  body       text        NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX notes_ri ON public.notes (owner_id, id);
ALTER TABLE public.notes REPLICA IDENTITY USING INDEX notes_ri;

ALTER TABLE public.notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_own ON public.notes
  FOR SELECT TO authenticated
  USING (owner_id = (select auth.uid()));

GRANT SELECT ON public.notes TO authenticated;
GRANT ALL    ON public.notes TO service_role;

-- ---------------------------------------------------------------------------
-- Tier B
-- ---------------------------------------------------------------------------
CREATE TABLE public.posts (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id   uuid NOT NULL,
  visibility text NOT NULL DEFAULT 'private'
             CHECK (visibility IN ('private', 'public')),
  title      text NOT NULL
);

CREATE INDEX posts_owner_id ON public.posts (owner_id);
ALTER TABLE public.posts REPLICA IDENTITY FULL;

ALTER TABLE public.posts ENABLE ROW LEVEL SECURITY;
CREATE POLICY posts_visible ON public.posts
  FOR SELECT TO authenticated
  USING (visibility = 'public' OR owner_id = (select auth.uid()));

GRANT SELECT ON public.posts TO authenticated;
GRANT ALL    ON public.posts TO service_role;

-- ---------------------------------------------------------------------------
-- Tier C
-- ---------------------------------------------------------------------------
CREATE TABLE public.team_members (
  user_id uuid NOT NULL,
  team_id int  NOT NULL,
  PRIMARY KEY (user_id, team_id)
);

CREATE TABLE public.invoices (
  id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  team_id int    NOT NULL,
  amount  numeric(12,2) NOT NULL,
  note    text   NOT NULL
);

CREATE UNIQUE INDEX invoices_ri ON public.invoices (team_id, id);
ALTER TABLE public.invoices REPLICA IDENTITY USING INDEX invoices_ri;

ALTER TABLE public.invoices ENABLE ROW LEVEL SECURITY;
CREATE POLICY invoices_team_member ON public.invoices
  FOR SELECT TO authenticated
  USING (EXISTS (
    SELECT 1 FROM public.team_members m
     WHERE m.team_id = invoices.team_id
       AND m.user_id = (select auth.uid())));

ALTER TABLE public.team_members ENABLE ROW LEVEL SECURITY;
CREATE POLICY team_members_own ON public.team_members
  FOR SELECT TO authenticated
  USING (user_id = (select auth.uid()));

GRANT SELECT ON public.invoices, public.team_members TO authenticated;
GRANT ALL    ON public.invoices, public.team_members TO service_role;

-- ---------------------------------------------------------------------------
-- Issuer (RLS off; sluice_authz must SELECT for snapshots and hold EXISTS)
-- ---------------------------------------------------------------------------
CREATE TABLE public.project_members (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id text NOT NULL,
  user_id    uuid NOT NULL
);

CREATE UNIQUE INDEX project_members_ri ON public.project_members (project_id, user_id);
ALTER TABLE public.project_members REPLICA IDENTITY USING INDEX project_members_ri;

CREATE TABLE public.project_docs (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id text NOT NULL,
  title      text NOT NULL,
  body       text NOT NULL DEFAULT ''
);

CREATE INDEX project_docs_project_id ON public.project_docs (project_id);

GRANT SELECT ON public.project_members, public.project_docs TO sluice_authz;
GRANT ALL    ON public.project_members, public.project_docs TO service_role;

-- ---------------------------------------------------------------------------
-- Publications
-- ---------------------------------------------------------------------------
ALTER PUBLICATION sluice_rls ADD TABLE
  public.notes, public.posts, public.invoices, public.team_members;

ALTER PUBLICATION sluice_issuer ADD TABLE
  public.project_docs, public.project_members;

COMMIT;

-- Production-readiness audit fixtures.
-- Broad RLS / SQL surface covering Tier A, B, and C classification edges.
-- Idempotent. Applied on a live harness; Sluice catalog refreshes within ~30s.

\set ON_ERROR_STOP on
BEGIN;

-- ---------------------------------------------------------------------------
-- Helpers used by Tier-C policies (non-whitelisted / subquery forces).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.audit_team_of(uid uuid)
RETURNS int LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$
  SELECT team_id FROM public.memberships WHERE user_id = uid LIMIT 1
$$;
GRANT EXECUTE ON FUNCTION public.audit_team_of(uuid) TO authenticated, anon, service_role, sluice_authz;

CREATE OR REPLACE FUNCTION public.audit_always_true()
RETURNS boolean LANGUAGE sql STABLE AS $$ SELECT true $$;
GRANT EXECUTE ON FUNCTION public.audit_always_true() TO authenticated, anon, service_role, sluice_authz;

-- ===========================================================================
-- TIER A spectrum
-- ===========================================================================

DROP TABLE IF EXISTS public.a_owner CASCADE;
CREATE TABLE public.a_owner (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  title text NOT NULL,
  tags text[] NOT NULL DEFAULT '{}',
  meta jsonb NOT NULL DEFAULT '{}',
  amount numeric(12,2) NOT NULL DEFAULT 0,
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX a_owner_ri ON public.a_owner (owner_id, id);
ALTER TABLE public.a_owner REPLICA IDENTITY USING INDEX a_owner_ri;
ALTER TABLE public.a_owner ENABLE ROW LEVEL SECURITY;
CREATE POLICY a_owner_p ON public.a_owner FOR SELECT TO authenticated
  USING (owner_id = auth.uid());
GRANT SELECT, INSERT, UPDATE, DELETE ON public.a_owner TO authenticated;
GRANT ALL ON public.a_owner TO service_role;

DROP TABLE IF EXISTS public.a_role CASCADE;
CREATE TABLE public.a_role (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  label text NOT NULL
);
CREATE UNIQUE INDEX a_role_ri ON public.a_role (owner_id, id);
ALTER TABLE public.a_role REPLICA IDENTITY USING INDEX a_role_ri;
ALTER TABLE public.a_role ENABLE ROW LEVEL SECURITY;
CREATE POLICY a_role_p ON public.a_role FOR SELECT TO authenticated
  USING (auth.role() = 'authenticated'::text AND owner_id = auth.uid());
GRANT SELECT ON public.a_role TO authenticated;
GRANT ALL ON public.a_role TO service_role;

DROP TABLE IF EXISTS public.a_jwt_claim CASCADE;
CREATE TABLE public.a_jwt_claim (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  note text NOT NULL
);
CREATE UNIQUE INDEX a_jwt_claim_ri ON public.a_jwt_claim (owner_id, id);
ALTER TABLE public.a_jwt_claim REPLICA IDENTITY USING INDEX a_jwt_claim_ri;
ALTER TABLE public.a_jwt_claim ENABLE ROW LEVEL SECURITY;
CREATE POLICY a_jwt_claim_p ON public.a_jwt_claim FOR SELECT TO authenticated
  USING (owner_id = ((auth.jwt() ->> 'sub'::text))::uuid);
GRANT SELECT ON public.a_jwt_claim TO authenticated;
GRANT ALL ON public.a_jwt_claim TO service_role;

DROP TABLE IF EXISTS public.a_coalesce CASCADE;
CREATE TABLE public.a_coalesce (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid,
  title text NOT NULL
);
CREATE UNIQUE INDEX a_coalesce_ri ON public.a_coalesce (id);
ALTER TABLE public.a_coalesce REPLICA IDENTITY USING INDEX a_coalesce_ri;
ALTER TABLE public.a_coalesce ENABLE ROW LEVEL SECURITY;
CREATE POLICY a_coalesce_p ON public.a_coalesce FOR SELECT TO authenticated
  USING (coalesce(owner_id, '00000000-0000-4000-8000-000000000000'::uuid) = auth.uid());
GRANT SELECT ON public.a_coalesce TO authenticated;
GRANT ALL ON public.a_coalesce TO service_role;

DROP TABLE IF EXISTS public.a_email_lower CASCADE;
CREATE TABLE public.a_email_lower (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  email text NOT NULL,
  body text NOT NULL
);
CREATE UNIQUE INDEX a_email_lower_ri ON public.a_email_lower (email, id);
ALTER TABLE public.a_email_lower REPLICA IDENTITY USING INDEX a_email_lower_ri;
ALTER TABLE public.a_email_lower ENABLE ROW LEVEL SECURITY;
CREATE POLICY a_email_lower_p ON public.a_email_lower FOR SELECT TO authenticated
  USING (lower(email) = lower(auth.email()));
GRANT SELECT ON public.a_email_lower TO authenticated;
GRANT ALL ON public.a_email_lower TO service_role;

DROP TABLE IF EXISTS public.a_is_null CASCADE;
CREATE TABLE public.a_is_null (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  deleted_at timestamptz
);
CREATE UNIQUE INDEX a_is_null_ri ON public.a_is_null (owner_id, id);
ALTER TABLE public.a_is_null REPLICA IDENTITY USING INDEX a_is_null_ri;
ALTER TABLE public.a_is_null ENABLE ROW LEVEL SECURITY;
CREATE POLICY a_is_null_p ON public.a_is_null FOR SELECT TO authenticated
  USING (owner_id = auth.uid() AND deleted_at IS NULL);
GRANT SELECT ON public.a_is_null TO authenticated;
GRANT ALL ON public.a_is_null TO service_role;

DROP TABLE IF EXISTS public.a_in_list CASCADE;
CREATE TABLE public.a_in_list (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  status text NOT NULL
);
CREATE UNIQUE INDEX a_in_list_ri ON public.a_in_list (owner_id, id);
ALTER TABLE public.a_in_list REPLICA IDENTITY USING INDEX a_in_list_ri;
ALTER TABLE public.a_in_list ENABLE ROW LEVEL SECURITY;
CREATE POLICY a_in_list_p ON public.a_in_list FOR SELECT TO authenticated
  USING (owner_id = auth.uid() AND status IN ('open'::text, 'pending'::text));
GRANT SELECT ON public.a_in_list TO authenticated;
GRANT ALL ON public.a_in_list TO service_role;

DROP TABLE IF EXISTS public.a_any_array CASCADE;
CREATE TABLE public.a_any_array (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  visibility text NOT NULL
);
CREATE UNIQUE INDEX a_any_array_ri ON public.a_any_array (owner_id, id);
ALTER TABLE public.a_any_array REPLICA IDENTITY USING INDEX a_any_array_ri;
ALTER TABLE public.a_any_array ENABLE ROW LEVEL SECURITY;
CREATE POLICY a_any_array_p ON public.a_any_array FOR SELECT TO authenticated
  USING (owner_id = auth.uid() AND visibility = ANY (ARRAY['public'::text, 'unlisted'::text]));
GRANT SELECT ON public.a_any_array TO authenticated;
GRANT ALL ON public.a_any_array TO service_role;

DROP TABLE IF EXISTS public.a_between CASCADE;
CREATE TABLE public.a_between (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  score int NOT NULL
);
CREATE UNIQUE INDEX a_between_ri ON public.a_between (owner_id, id);
ALTER TABLE public.a_between REPLICA IDENTITY USING INDEX a_between_ri;
ALTER TABLE public.a_between ENABLE ROW LEVEL SECURITY;
CREATE POLICY a_between_p ON public.a_between FOR SELECT TO authenticated
  USING (owner_id = auth.uid() AND score BETWEEN 1 AND 100);
GRANT SELECT ON public.a_between TO authenticated;
GRANT ALL ON public.a_between TO service_role;

DROP TABLE IF EXISTS public.a_jsonb_path CASCADE;
CREATE TABLE public.a_jsonb_path (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  meta jsonb NOT NULL DEFAULT '{}'
);
CREATE UNIQUE INDEX a_jsonb_path_ri ON public.a_jsonb_path (owner_id, id);
ALTER TABLE public.a_jsonb_path REPLICA IDENTITY USING INDEX a_jsonb_path_ri;
ALTER TABLE public.a_jsonb_path ENABLE ROW LEVEL SECURITY;
CREATE POLICY a_jsonb_path_p ON public.a_jsonb_path FOR SELECT TO authenticated
  USING (owner_id = auth.uid() AND (meta ->> 'kind'::text) = 'ok'::text);
GRANT SELECT ON public.a_jsonb_path TO authenticated;
GRANT ALL ON public.a_jsonb_path TO service_role;

DROP TABLE IF EXISTS public.a_norls CASCADE;
CREATE TABLE public.a_norls (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name text NOT NULL,
  value double precision NOT NULL
);
GRANT SELECT ON public.a_norls TO anon, authenticated;
GRANT ALL ON public.a_norls TO service_role;

-- ===========================================================================
-- TIER B spectrum (compilable, row-dependent)
-- ===========================================================================

DROP TABLE IF EXISTS public.b_or_public CASCADE;
CREATE TABLE public.b_or_public (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  visibility text NOT NULL,
  title text NOT NULL
);
ALTER TABLE public.b_or_public REPLICA IDENTITY FULL;
ALTER TABLE public.b_or_public ENABLE ROW LEVEL SECURITY;
CREATE POLICY b_or_public_p ON public.b_or_public FOR SELECT TO authenticated
  USING (visibility = 'public'::text OR owner_id = auth.uid());
GRANT SELECT ON public.b_or_public TO authenticated;
GRANT ALL ON public.b_or_public TO service_role;

DROP TABLE IF EXISTS public.b_threshold CASCADE;
CREATE TABLE public.b_threshold (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  score int NOT NULL,
  title text NOT NULL
);
ALTER TABLE public.b_threshold REPLICA IDENTITY FULL;
ALTER TABLE public.b_threshold ENABLE ROW LEVEL SECURITY;
CREATE POLICY b_threshold_p ON public.b_threshold FOR SELECT TO authenticated
  USING (score >= 50 OR owner_id = auth.uid());
GRANT SELECT ON public.b_threshold TO authenticated;
GRANT ALL ON public.b_threshold TO service_role;

DROP TABLE IF EXISTS public.b_not_and CASCADE;
CREATE TABLE public.b_not_and (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  hidden boolean NOT NULL DEFAULT false,
  visibility text NOT NULL
);
ALTER TABLE public.b_not_and REPLICA IDENTITY FULL;
ALTER TABLE public.b_not_and ENABLE ROW LEVEL SECURITY;
CREATE POLICY b_not_and_p ON public.b_not_and FOR SELECT TO authenticated
  USING ((NOT hidden) AND (visibility = 'public'::text OR owner_id = auth.uid()));
GRANT SELECT ON public.b_not_and TO authenticated;
GRANT ALL ON public.b_not_and TO service_role;

DROP TABLE IF EXISTS public.b_like CASCADE;
CREATE TABLE public.b_like (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  title text NOT NULL
);
ALTER TABLE public.b_like REPLICA IDENTITY FULL;
ALTER TABLE public.b_like ENABLE ROW LEVEL SECURITY;
CREATE POLICY b_like_p ON public.b_like FOR SELECT TO authenticated
  USING (title ILIKE 'public-%'::text OR owner_id = auth.uid());
GRANT SELECT ON public.b_like TO authenticated;
GRANT ALL ON public.b_like TO service_role;

DROP TABLE IF EXISTS public.b_is_distinct CASCADE;
CREATE TABLE public.b_is_distinct (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid,
  flag text
);
ALTER TABLE public.b_is_distinct REPLICA IDENTITY FULL;
ALTER TABLE public.b_is_distinct ENABLE ROW LEVEL SECURITY;
CREATE POLICY b_is_distinct_p ON public.b_is_distinct FOR SELECT TO authenticated
  USING (owner_id IS NOT DISTINCT FROM auth.uid() OR flag = 'open'::text);
GRANT SELECT ON public.b_is_distinct TO authenticated;
GRANT ALL ON public.b_is_distinct TO service_role;

DROP TABLE IF EXISTS public.b_nullif CASCADE;
CREATE TABLE public.b_nullif (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  status text
);
ALTER TABLE public.b_nullif REPLICA IDENTITY FULL;
ALTER TABLE public.b_nullif ENABLE ROW LEVEL SECURITY;
CREATE POLICY b_nullif_p ON public.b_nullif FOR SELECT TO authenticated
  USING (owner_id = auth.uid() OR nullif(status, 'draft'::text) IS NOT NULL);
GRANT SELECT ON public.b_nullif TO authenticated;
GRANT ALL ON public.b_nullif TO service_role;

DROP TABLE IF EXISTS public.b_arith CASCADE;
CREATE TABLE public.b_arith (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  a int NOT NULL,
  b int NOT NULL
);
ALTER TABLE public.b_arith REPLICA IDENTITY FULL;
ALTER TABLE public.b_arith ENABLE ROW LEVEL SECURITY;
CREATE POLICY b_arith_p ON public.b_arith FOR SELECT TO authenticated
  USING (owner_id = auth.uid() OR (a + b) > 100);
GRANT SELECT ON public.b_arith TO authenticated;
GRANT ALL ON public.b_arith TO service_role;

DROP TABLE IF EXISTS public.b_concat CASCADE;
CREATE TABLE public.b_concat (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  prefix text NOT NULL,
  suffix text NOT NULL
);
ALTER TABLE public.b_concat REPLICA IDENTITY FULL;
ALTER TABLE public.b_concat ENABLE ROW LEVEL SECURITY;
CREATE POLICY b_concat_p ON public.b_concat FOR SELECT TO authenticated
  USING (owner_id = auth.uid() OR (prefix || suffix) = 'okok'::text);
GRANT SELECT ON public.b_concat TO authenticated;
GRANT ALL ON public.b_concat TO service_role;

DROP TABLE IF EXISTS public.b_length CASCADE;
CREATE TABLE public.b_length (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  body text NOT NULL
);
ALTER TABLE public.b_length REPLICA IDENTITY FULL;
ALTER TABLE public.b_length ENABLE ROW LEVEL SECURITY;
CREATE POLICY b_length_p ON public.b_length FOR SELECT TO authenticated
  USING (owner_id = auth.uid() OR length(body) > 10);
GRANT SELECT ON public.b_length TO authenticated;
GRANT ALL ON public.b_length TO service_role;

DROP TABLE IF EXISTS public.b_volatile_now CASCADE;
CREATE TABLE public.b_volatile_now (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  expires_at timestamptz NOT NULL
);
ALTER TABLE public.b_volatile_now REPLICA IDENTITY FULL;
ALTER TABLE public.b_volatile_now ENABLE ROW LEVEL SECURITY;
CREATE POLICY b_volatile_now_p ON public.b_volatile_now FOR SELECT TO authenticated
  USING (owner_id = auth.uid() OR expires_at > now());
GRANT SELECT ON public.b_volatile_now TO authenticated;
GRANT ALL ON public.b_volatile_now TO service_role;

-- ===========================================================================
-- TIER C spectrum (must fall to impersonation; diagnostics must report)
-- ===========================================================================

DROP TABLE IF EXISTS public.c_exists CASCADE;
CREATE TABLE public.c_exists (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  team_id int NOT NULL,
  note text NOT NULL
);
CREATE UNIQUE INDEX c_exists_ri ON public.c_exists (team_id, id);
ALTER TABLE public.c_exists REPLICA IDENTITY USING INDEX c_exists_ri;
ALTER TABLE public.c_exists ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_exists_p ON public.c_exists FOR SELECT TO authenticated
  USING (EXISTS (
    SELECT 1 FROM public.memberships m
     WHERE m.team_id = c_exists.team_id AND m.user_id = auth.uid()));
GRANT SELECT ON public.c_exists TO authenticated;
GRANT ALL ON public.c_exists TO service_role;

DROP TABLE IF EXISTS public.c_in_select CASCADE;
CREATE TABLE public.c_in_select (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  team_id int NOT NULL,
  note text NOT NULL
);
CREATE UNIQUE INDEX c_in_select_ri ON public.c_in_select (team_id, id);
ALTER TABLE public.c_in_select REPLICA IDENTITY USING INDEX c_in_select_ri;
ALTER TABLE public.c_in_select ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_in_select_p ON public.c_in_select FOR SELECT TO authenticated
  USING (team_id IN (SELECT m.team_id FROM public.memberships m WHERE m.user_id = auth.uid()));
GRANT SELECT ON public.c_in_select TO authenticated;
GRANT ALL ON public.c_in_select TO service_role;

DROP TABLE IF EXISTS public.c_any_select CASCADE;
CREATE TABLE public.c_any_select (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  team_id int NOT NULL,
  note text NOT NULL
);
CREATE UNIQUE INDEX c_any_select_ri ON public.c_any_select (team_id, id);
ALTER TABLE public.c_any_select REPLICA IDENTITY USING INDEX c_any_select_ri;
ALTER TABLE public.c_any_select ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_any_select_p ON public.c_any_select FOR SELECT TO authenticated
  USING (team_id = ANY (SELECT m.team_id FROM public.memberships m WHERE m.user_id = auth.uid()));
GRANT SELECT ON public.c_any_select TO authenticated;
GRANT ALL ON public.c_any_select TO service_role;

DROP TABLE IF EXISTS public.c_udf CASCADE;
CREATE TABLE public.c_udf (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  team_id int NOT NULL,
  note text NOT NULL
);
CREATE UNIQUE INDEX c_udf_ri ON public.c_udf (team_id, id);
ALTER TABLE public.c_udf REPLICA IDENTITY USING INDEX c_udf_ri;
ALTER TABLE public.c_udf ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_udf_p ON public.c_udf FOR SELECT TO authenticated
  USING (team_id = public.audit_team_of(auth.uid()));
GRANT SELECT ON public.c_udf TO authenticated;
GRANT ALL ON public.c_udf TO service_role;

DROP TABLE IF EXISTS public.c_case CASCADE;
CREATE TABLE public.c_case (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  status text NOT NULL
);
CREATE UNIQUE INDEX c_case_ri ON public.c_case (owner_id, id);
ALTER TABLE public.c_case REPLICA IDENTITY USING INDEX c_case_ri;
ALTER TABLE public.c_case ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_case_p ON public.c_case FOR SELECT TO authenticated
  USING (CASE WHEN status = 'open'::text THEN owner_id = auth.uid() ELSE false END);
GRANT SELECT ON public.c_case TO authenticated;
GRANT ALL ON public.c_case TO service_role;

DROP TABLE IF EXISTS public.c_current_user CASCADE;
CREATE TABLE public.c_current_user (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  note text NOT NULL
);
CREATE UNIQUE INDEX c_current_user_ri ON public.c_current_user (owner_id, id);
ALTER TABLE public.c_current_user REPLICA IDENTITY USING INDEX c_current_user_ri;
ALTER TABLE public.c_current_user ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_current_user_p ON public.c_current_user FOR SELECT TO authenticated
  USING (current_user = 'authenticated'::name AND owner_id = auth.uid());
GRANT SELECT ON public.c_current_user TO authenticated;
GRANT ALL ON public.c_current_user TO service_role;

DROP TABLE IF EXISTS public.c_has_priv CASCADE;
CREATE TABLE public.c_has_priv (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  note text NOT NULL
);
CREATE UNIQUE INDEX c_has_priv_ri ON public.c_has_priv (owner_id, id);
ALTER TABLE public.c_has_priv REPLICA IDENTITY USING INDEX c_has_priv_ri;
ALTER TABLE public.c_has_priv ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_has_priv_p ON public.c_has_priv FOR SELECT TO authenticated
  USING (has_table_privilege('authenticated'::text, 'public.c_has_priv'::text, 'SELECT'::text)
         AND owner_id = auth.uid());
GRANT SELECT ON public.c_has_priv TO authenticated;
GRANT ALL ON public.c_has_priv TO service_role;

DROP TABLE IF EXISTS public.c_scalar_subq CASCADE;
CREATE TABLE public.c_scalar_subq (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  team_id int NOT NULL,
  note text NOT NULL
);
CREATE UNIQUE INDEX c_scalar_subq_ri ON public.c_scalar_subq (team_id, id);
ALTER TABLE public.c_scalar_subq REPLICA IDENTITY USING INDEX c_scalar_subq_ri;
ALTER TABLE public.c_scalar_subq ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_scalar_subq_p ON public.c_scalar_subq FOR SELECT TO authenticated
  USING (team_id = (SELECT m.team_id FROM public.memberships m WHERE m.user_id = auth.uid() LIMIT 1));
GRANT SELECT ON public.c_scalar_subq TO authenticated;
GRANT ALL ON public.c_scalar_subq TO service_role;

DROP TABLE IF EXISTS public.c_all_array CASCADE;
CREATE TABLE public.c_all_array (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  score int NOT NULL
);
CREATE UNIQUE INDEX c_all_array_ri ON public.c_all_array (owner_id, id);
ALTER TABLE public.c_all_array REPLICA IDENTITY USING INDEX c_all_array_ri;
ALTER TABLE public.c_all_array ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_all_array_p ON public.c_all_array FOR SELECT TO authenticated
  USING (owner_id = auth.uid() AND score = ALL (ARRAY[1,1,1]));
GRANT SELECT ON public.c_all_array TO authenticated;
GRANT ALL ON public.c_all_array TO service_role;

DROP TABLE IF EXISTS public.c_regexp CASCADE;
CREATE TABLE public.c_regexp (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  code text NOT NULL
);
CREATE UNIQUE INDEX c_regexp_ri ON public.c_regexp (owner_id, id);
ALTER TABLE public.c_regexp REPLICA IDENTITY USING INDEX c_regexp_ri;
ALTER TABLE public.c_regexp ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_regexp_p ON public.c_regexp FOR SELECT TO authenticated
  USING (owner_id = auth.uid() AND code ~ '^[A-Z]+$'::text);
GRANT SELECT ON public.c_regexp TO authenticated;
GRANT ALL ON public.c_regexp TO service_role;

DROP TABLE IF EXISTS public.c_extract CASCADE;
CREATE TABLE public.c_extract (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  ts timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX c_extract_ri ON public.c_extract (owner_id, id);
ALTER TABLE public.c_extract REPLICA IDENTITY USING INDEX c_extract_ri;
ALTER TABLE public.c_extract ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_extract_p ON public.c_extract FOR SELECT TO authenticated
  USING (owner_id = auth.uid() AND extract(epoch FROM ts) > 0);
GRANT SELECT ON public.c_extract TO authenticated;
GRANT ALL ON public.c_extract TO service_role;

DROP TABLE IF EXISTS public.c_greatest CASCADE;
CREATE TABLE public.c_greatest (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  a int NOT NULL,
  b int NOT NULL
);
CREATE UNIQUE INDEX c_greatest_ri ON public.c_greatest (owner_id, id);
ALTER TABLE public.c_greatest REPLICA IDENTITY USING INDEX c_greatest_ri;
ALTER TABLE public.c_greatest ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_greatest_p ON public.c_greatest FOR SELECT TO authenticated
  USING (owner_id = auth.uid() AND greatest(a, b) > 0);
GRANT SELECT ON public.c_greatest TO authenticated;
GRANT ALL ON public.c_greatest TO service_role;

DROP TABLE IF EXISTS public.c_restrictive_combo CASCADE;
CREATE TABLE public.c_restrictive_combo (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  team_id int NOT NULL,
  amount numeric(12,2) NOT NULL,
  note text NOT NULL
);
CREATE UNIQUE INDEX c_restrictive_combo_ri ON public.c_restrictive_combo (team_id, id);
ALTER TABLE public.c_restrictive_combo REPLICA IDENTITY USING INDEX c_restrictive_combo_ri;
ALTER TABLE public.c_restrictive_combo ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_restrictive_combo_team ON public.c_restrictive_combo FOR SELECT TO authenticated
  USING (EXISTS (
    SELECT 1 FROM public.memberships m
     WHERE m.team_id = c_restrictive_combo.team_id AND m.user_id = auth.uid()));
CREATE POLICY c_restrictive_combo_amt ON public.c_restrictive_combo
  AS RESTRICTIVE FOR SELECT TO authenticated
  USING (amount > 0);
GRANT SELECT ON public.c_restrictive_combo TO authenticated;
GRANT ALL ON public.c_restrictive_combo TO service_role;

-- Multi-permissive policies (OR semantics) — still Tier C due to EXISTS.
DROP TABLE IF EXISTS public.c_multi_perm CASCADE;
CREATE TABLE public.c_multi_perm (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  team_id int NOT NULL,
  note text NOT NULL
);
CREATE UNIQUE INDEX c_multi_perm_ri ON public.c_multi_perm (owner_id, id);
ALTER TABLE public.c_multi_perm REPLICA IDENTITY USING INDEX c_multi_perm_ri;
ALTER TABLE public.c_multi_perm ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_multi_own ON public.c_multi_perm FOR SELECT TO authenticated
  USING (owner_id = auth.uid());
CREATE POLICY c_multi_team ON public.c_multi_perm FOR SELECT TO authenticated
  USING (EXISTS (
    SELECT 1 FROM public.memberships m
     WHERE m.team_id = c_multi_perm.team_id AND m.user_id = auth.uid()));
GRANT SELECT ON public.c_multi_perm TO authenticated;
GRANT ALL ON public.c_multi_perm TO service_role;

-- ===========================================================================
-- Edge / rare schema shapes that still generate WAL
-- ===========================================================================

DROP TABLE IF EXISTS public.e_composite_pk CASCADE;
CREATE TABLE public.e_composite_pk (
  a int NOT NULL,
  b int NOT NULL,
  owner_id uuid NOT NULL,
  payload text NOT NULL,
  PRIMARY KEY (a, b)
);
CREATE UNIQUE INDEX e_composite_pk_ri ON public.e_composite_pk (owner_id, a, b);
ALTER TABLE public.e_composite_pk REPLICA IDENTITY USING INDEX e_composite_pk_ri;
ALTER TABLE public.e_composite_pk ENABLE ROW LEVEL SECURITY;
CREATE POLICY e_composite_pk_p ON public.e_composite_pk FOR SELECT TO authenticated
  USING (owner_id = auth.uid());
GRANT SELECT ON public.e_composite_pk TO authenticated;
GRANT ALL ON public.e_composite_pk TO service_role;

DROP TABLE IF EXISTS public.e_generated CASCADE;
CREATE TABLE public.e_generated (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  price numeric NOT NULL,
  qty int NOT NULL,
  total numeric GENERATED ALWAYS AS (price * qty) STORED
);
CREATE UNIQUE INDEX e_generated_ri ON public.e_generated (owner_id, id);
ALTER TABLE public.e_generated REPLICA IDENTITY USING INDEX e_generated_ri;
ALTER TABLE public.e_generated ENABLE ROW LEVEL SECURITY;
CREATE POLICY e_generated_p ON public.e_generated FOR SELECT TO authenticated
  USING (owner_id = auth.uid());
GRANT SELECT ON public.e_generated TO authenticated;
GRANT ALL ON public.e_generated TO service_role;

DROP TABLE IF EXISTS public.e_toast CASCADE;
CREATE TABLE public.e_toast (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  views int NOT NULL DEFAULT 0,
  body text
);
CREATE UNIQUE INDEX e_toast_ri ON public.e_toast (owner_id, id);
ALTER TABLE public.e_toast REPLICA IDENTITY USING INDEX e_toast_ri;
ALTER TABLE public.e_toast ENABLE ROW LEVEL SECURITY;
CREATE POLICY e_toast_p ON public.e_toast FOR SELECT TO authenticated
  USING (owner_id = auth.uid());
GRANT SELECT ON public.e_toast TO authenticated;
GRANT ALL ON public.e_toast TO service_role;

DROP TABLE IF EXISTS public.e_no_pk CASCADE;
CREATE TABLE public.e_no_pk (
  owner_id uuid NOT NULL,
  note text NOT NULL
);
ALTER TABLE public.e_no_pk REPLICA IDENTITY FULL;
ALTER TABLE public.e_no_pk ENABLE ROW LEVEL SECURITY;
CREATE POLICY e_no_pk_p ON public.e_no_pk FOR SELECT TO authenticated
  USING (owner_id = auth.uid());
GRANT SELECT ON public.e_no_pk TO authenticated;
GRANT ALL ON public.e_no_pk TO service_role;

DROP TABLE IF EXISTS public.e_partitioned CASCADE;
CREATE TABLE public.e_partitioned (
  id bigint GENERATED ALWAYS AS IDENTITY,
  owner_id uuid NOT NULL,
  region text NOT NULL,
  note text NOT NULL,
  PRIMARY KEY (id, region)
) PARTITION BY LIST (region);
CREATE TABLE public.e_partitioned_eu PARTITION OF public.e_partitioned FOR VALUES IN ('eu');
CREATE TABLE public.e_partitioned_us PARTITION OF public.e_partitioned FOR VALUES IN ('us');
ALTER TABLE public.e_partitioned ENABLE ROW LEVEL SECURITY;
CREATE POLICY e_partitioned_p ON public.e_partitioned FOR SELECT TO authenticated
  USING (owner_id = auth.uid());
GRANT SELECT ON public.e_partitioned TO authenticated;
GRANT ALL ON public.e_partitioned TO service_role;

-- UNLOGGED: must NOT appear in logical decoding even if someone tries to publish.
DROP TABLE IF EXISTS public.e_unlogged CASCADE;
CREATE UNLOGGED TABLE public.e_unlogged (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  note text NOT NULL
);

-- ===========================================================================
-- RLS best-practice spellings.
--
-- Supabase's RLS performance guide says to wrap per-query functions in a scalar
-- subquery -- `(select auth.uid())` -- so the planner evaluates them once as an
-- InitPlan instead of once per scanned row, and to always name the roles with
-- TO so an ineligible role is rejected before the predicate runs.
--
-- PostgreSQL stores the wrapper as `( SELECT auth.uid() AS uid)`, i.e. as a
-- subquery node. These tables exist to prove that the recommended spelling is
-- classified EXACTLY like the direct one: same tier, same routing key, same
-- delivery. If wrapping a call demoted a policy to Tier C, every tuned schema
-- in the ecosystem would land on the slowest path Sluice has.
-- ===========================================================================

DROP TABLE IF EXISTS public.w_owner CASCADE;
CREATE TABLE public.w_owner (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  title text NOT NULL
);
CREATE UNIQUE INDEX w_owner_ri ON public.w_owner (owner_id, id);
ALTER TABLE public.w_owner REPLICA IDENTITY USING INDEX w_owner_ri;
ALTER TABLE public.w_owner ENABLE ROW LEVEL SECURITY;
CREATE POLICY w_owner_p ON public.w_owner FOR SELECT TO authenticated
  USING (owner_id = (select auth.uid()));
GRANT SELECT ON public.w_owner TO authenticated;
GRANT ALL ON public.w_owner TO service_role;

DROP TABLE IF EXISTS public.w_role CASCADE;
CREATE TABLE public.w_role (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  title text NOT NULL
);
CREATE UNIQUE INDEX w_role_ri ON public.w_role (owner_id, id);
ALTER TABLE public.w_role REPLICA IDENTITY USING INDEX w_role_ri;
ALTER TABLE public.w_role ENABLE ROW LEVEL SECURITY;
CREATE POLICY w_role_p ON public.w_role FOR SELECT TO authenticated
  USING ((select auth.role()) = 'authenticated' AND owner_id = (select auth.uid()));
GRANT SELECT ON public.w_role TO authenticated;
GRANT ALL ON public.w_role TO service_role;

DROP TABLE IF EXISTS public.w_jwt CASCADE;
CREATE TABLE public.w_jwt (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  title text NOT NULL
);
CREATE UNIQUE INDEX w_jwt_ri ON public.w_jwt (owner_id, id);
ALTER TABLE public.w_jwt REPLICA IDENTITY USING INDEX w_jwt_ri;
ALTER TABLE public.w_jwt ENABLE ROW LEVEL SECURITY;
CREATE POLICY w_jwt_p ON public.w_jwt FOR SELECT TO authenticated
  USING (owner_id = ((select auth.jwt() ->> 'sub'))::uuid);
GRANT SELECT ON public.w_jwt TO authenticated;
GRANT ALL ON public.w_jwt TO service_role;

-- Wrapped, and row-dependent: must still be Tier B, not Tier C.
DROP TABLE IF EXISTS public.w_or_public CASCADE;
CREATE TABLE public.w_or_public (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  visibility text NOT NULL,
  title text NOT NULL
);
CREATE INDEX w_or_public_owner ON public.w_or_public (owner_id);
ALTER TABLE public.w_or_public REPLICA IDENTITY FULL;
ALTER TABLE public.w_or_public ENABLE ROW LEVEL SECURITY;
CREATE POLICY w_or_public_p ON public.w_or_public FOR SELECT TO authenticated
  USING (visibility = 'public' OR owner_id = (select auth.uid()));
GRANT SELECT ON public.w_or_public TO authenticated;
GRANT ALL ON public.w_or_public TO service_role;

-- `IN (select ...)` and `= ANY (select ...)` over a FROM-less select are single
-- values, not joins, and must not be confused with the subquery forms above.
DROP TABLE IF EXISTS public.w_in_scalar CASCADE;
CREATE TABLE public.w_in_scalar (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  title text NOT NULL
);
CREATE UNIQUE INDEX w_in_scalar_ri ON public.w_in_scalar (owner_id, id);
ALTER TABLE public.w_in_scalar REPLICA IDENTITY USING INDEX w_in_scalar_ri;
ALTER TABLE public.w_in_scalar ENABLE ROW LEVEL SECURITY;
CREATE POLICY w_in_scalar_p ON public.w_in_scalar FOR SELECT TO authenticated
  USING (owner_id IN (select auth.uid()));
GRANT SELECT ON public.w_in_scalar TO authenticated;
GRANT ALL ON public.w_in_scalar TO service_role;

DROP TABLE IF EXISTS public.w_any_scalar CASCADE;
CREATE TABLE public.w_any_scalar (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  title text NOT NULL
);
CREATE UNIQUE INDEX w_any_scalar_ri ON public.w_any_scalar (owner_id, id);
ALTER TABLE public.w_any_scalar REPLICA IDENTITY USING INDEX w_any_scalar_ri;
ALTER TABLE public.w_any_scalar ENABLE ROW LEVEL SECURITY;
CREATE POLICY w_any_scalar_p ON public.w_any_scalar FOR SELECT TO authenticated
  USING (owner_id = ANY (select auth.uid()));
GRANT SELECT ON public.w_any_scalar TO authenticated;
GRANT ALL ON public.w_any_scalar TO service_role;

-- Deliberately violates both rules, so /diagnostics has something to report:
-- no TO clause, an unwrapped call, and no index on the policy column.
DROP TABLE IF EXISTS public.w_bad_practice CASCADE;
CREATE TABLE public.w_bad_practice (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  owner_id uuid NOT NULL,
  amount int NOT NULL DEFAULT 0
);
ALTER TABLE public.w_bad_practice REPLICA IDENTITY FULL;
ALTER TABLE public.w_bad_practice ENABLE ROW LEVEL SECURITY;
CREATE POLICY w_bad_practice_p ON public.w_bad_practice FOR SELECT
  USING (owner_id = auth.uid());
GRANT SELECT ON public.w_bad_practice TO authenticated;
GRANT ALL ON public.w_bad_practice TO service_role;

-- ===========================================================================
-- Publication membership
-- ===========================================================================
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'public.a_owner','public.a_role','public.a_jwt_claim','public.a_coalesce',
    'public.a_email_lower','public.a_is_null','public.a_in_list','public.a_any_array',
    'public.a_between','public.a_jsonb_path','public.a_norls',
    'public.b_or_public','public.b_threshold','public.b_not_and','public.b_like',
    'public.b_is_distinct','public.b_nullif','public.b_arith','public.b_concat',
    'public.b_length','public.b_volatile_now',
    'public.c_exists','public.c_in_select','public.c_any_select','public.c_udf',
    'public.c_case','public.c_current_user','public.c_has_priv','public.c_scalar_subq',
    'public.c_all_array','public.c_regexp','public.c_extract','public.c_greatest',
    'public.c_restrictive_combo','public.c_multi_perm',
    'public.e_composite_pk','public.e_generated','public.e_toast','public.e_no_pk',
    'public.e_partitioned',
    'public.w_owner','public.w_role','public.w_jwt','public.w_or_public',
    'public.w_in_scalar','public.w_any_scalar','public.w_bad_practice'
  ] LOOP
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
         WHERE pubname = 'sluice' AND schemaname || '.' || tablename = t
      ) THEN
        EXECUTE format('ALTER PUBLICATION sluice ADD TABLE %s', t);
      END IF;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'skip publish %: %', t, SQLERRM;
    END;
  END LOOP;
END $$;

COMMIT;
NOTIFY pgrst, 'reload schema';

SELECT c.oid::regclass AS relation,
       c.relrowsecurity AS rls,
       c.relreplident AS replica_identity,
       (SELECT count(*) FROM pg_policy p WHERE p.polrelid = c.oid) AS policies
FROM pg_publication_tables pt
JOIN pg_class c ON c.oid = (pt.schemaname || '.' || pt.tablename)::regclass
WHERE pt.pubname = 'sluice'
ORDER BY 1;

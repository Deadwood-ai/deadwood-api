-- Trusted internal SQL inspection. Password provisioning is separate from DDL.
BEGIN;
-- Roles are cluster-wide and survive Supabase's database reset.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'analyst') THEN
    CREATE ROLE analyst NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'team_analyst') THEN
    CREATE ROLE team_analyst NOLOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
      NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 20;
  END IF;
END
$$;
GRANT analyst TO team_analyst;
ALTER ROLE team_analyst SET default_transaction_read_only = on;
ALTER ROLE team_analyst SET statement_timeout = '30s';
ALTER ROLE team_analyst SET lock_timeout = '3s';
GRANT USAGE ON SCHEMA public TO analyst;

-- Reviewed application inventory. No blanket/default SELECT on future tables,
-- API views, extension relations, newsletter subscriptions or auth internals.
DO $$
DECLARE
  table_name text;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'collaborators', 'data_publication', 'dataset_audit',
    'dataset_flag_status_history', 'dataset_flags', 'jt_data_publication_datasets',
    'jt_data_publication_user_info', 'map_flags', 'prepackaged_dataset_definitions',
    'prepackaged_dataset_versions', 'presentations', 'privileged_users',
    'priwa_befallsgruppe_flights', 'priwa_befallsgruppe_members', 'priwa_befallsgruppen',
    'priwa_kaeferbaeume', 'priwa_project_flights', 'priwa_project_memberships',
    'priwa_projects', 'priwa_warnkarte_archive_events', 'priwa_warnkarte_polygons',
    'priwa_warnkarte_publications', 'priwa_warnkarte_versions',
    'processing_notification_events', 'projects', 'public_tree_observations',
    'publications', 'reference_datasets', 'reference_patch_deadwood_geometries',
    'reference_patch_forest_cover_geometries', 'reference_patches', 'user_info',
    'user_notification_preferences', 'v2_aois', 'v2_cogs', 'v2_dataset_edit_history',
    'v2_datasets', 'v2_deadwood_geometries', 'v2_forest_cover_geometries',
    'v2_geometry_corrections', 'v2_labels', 'v2_logs', 'v2_metadata',
    'v2_model_preferences', 'v2_orthos', 'v2_orthos_processed', 'v2_queue',
    'v2_raw_images', 'v2_search_queries', 'v2_statuses', 'v2_thumbnails',
    'v2_tile_aoi_membership', 'v2_tile_embeddings'
  ] LOOP
    EXECUTE format('GRANT SELECT ON public.%I TO analyst', table_name);
    EXECUTE format(
      'CREATE POLICY analyst_select ON public.%I FOR SELECT TO analyst USING (true)', table_name
    );
  END LOOP;
END
$$;

-- Grant lifecycle and ownership only; never token_hash or unstructured extra.
GRANT SELECT (id, created_at, expires_at, last_validated_at, revoked_at,
  version_id, user_id, validation_count)
  ON public.prepackaged_dataset_download_grants TO analyst;
CREATE POLICY analyst_select ON public.prepackaged_dataset_download_grants
  FOR SELECT TO analyst USING (true);

-- A narrow owner identity surface, outside the schemas exposed by PostgREST.
-- The view intentionally uses its owner's auth.users access, with fixed columns.
CREATE SCHEMA analyst_access AUTHORIZATION postgres;
REVOKE ALL ON SCHEMA analyst_access FROM PUBLIC;
CREATE VIEW analyst_access.accounts WITH (security_barrier = true) AS
  SELECT id, email FROM auth.users;
REVOKE ALL ON analyst_access.accounts FROM PUBLIC, anon, authenticated, service_role;
GRANT USAGE ON SCHEMA analyst_access TO analyst;
GRANT SELECT ON analyst_access.accounts TO analyst;

-- Every SQL login inherits PUBLIC. Preserve existing API/monitor execution
-- while removing the new analyst's route into privileged application routines,
-- including definer triggers attached to analyst-owned temporary tables.
DO $$
DECLARE
  routine record;
  existing_login record;
BEGIN
  FOR routine IN
    SELECT p.oid::regprocedure AS signature
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public' AND p.prosecdef
      AND NOT EXISTS (
        SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_proc'::regclass
          AND d.objid = p.oid AND d.deptype = 'e'
      )
      AND EXISTS (
        SELECT 1 FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
        WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE'
      )
  LOOP
    EXECUTE format(
      'GRANT EXECUTE ON FUNCTION %s TO anon, authenticated, service_role, deadtrees_operator_status',
      routine.signature
    );
    -- Preserve other already-provisioned SQL identities as well as API roles.
    FOR existing_login IN
      SELECT rolname FROM pg_roles WHERE rolcanlogin
        AND rolname NOT IN ('analyst', 'team_analyst')
    LOOP
      EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO %I', routine.signature, existing_login.rolname);
    END LOOP;
    EXECUTE format('REVOKE EXECUTE ON FUNCTION %s FROM PUBLIC', routine.signature);
  END LOOP;
END
$$;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
-- Extension-owned PUBLIC grants (notably pg_net) are deliberately unchanged.
-- This trusted role is not a database-wide read-only guarantee: BEGIN READ ONLY
-- is mandatory for inspections, including through a transaction pooler.
COMMIT;

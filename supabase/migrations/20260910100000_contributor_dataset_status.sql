-- Contributor status is independent of public-gallery curation.
ALTER TABLE public.v2_statuses ADD COLUMN error_stage text;
COMMENT ON COLUMN public.v2_statuses.error_stage IS
    'Durable stage of the latest processing failure; null for historical/unknown failures and after retry.';

-- Explicit owner authorization permits safe review details without granting access
-- to the auditor read models (which include identities and email addresses).
CREATE FUNCTION public.get_dataset_status_details(p_dataset_id bigint)
RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path = '' AS $$
    SELECT jsonb_build_object(
        'dataset_id', a.dataset_id,
        'final_assessment', a.final_assessment,
        'deadwood_quality', a.deadwood_quality,
        'forest_cover_quality', a.forest_cover_quality,
        'has_major_issue', a.has_major_issue,
        'audit_date', a.audit_date,
        'has_valid_phenology', a.has_valid_phenology,
        'has_valid_acquisition_date', a.has_valid_acquisition_date,
        'is_georeferenced', a.is_georeferenced,
        'acquisition_date_notes', a.acquisition_date_notes,
        'phenology_notes', a.phenology_notes,
        'deadwood_notes', a.deadwood_notes,
        'forest_cover_notes', a.forest_cover_notes,
        'has_cog_issue', a.has_cog_issue,
        'cog_issue_notes', a.cog_issue_notes,
        'has_thumbnail_issue', a.has_thumbnail_issue,
        'thumbnail_issue_notes', a.thumbnail_issue_notes,
        'notes', a.notes
    )
    FROM public.dataset_audit a
    JOIN public.v2_datasets d ON d.id = a.dataset_id
    WHERE d.id = p_dataset_id AND d.user_id = auth.uid()
$$;
REVOKE ALL ON FUNCTION public.get_dataset_status_details(bigint) FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.get_dataset_status_details(bigint) TO authenticated;

CREATE VIEW public.v2_full_dataset_view_owner
WITH (security_invoker = true, security_barrier = true) AS
SELECT
    base.id,
    base.user_id,
    base.created_at,
    base.file_name,
    base.license,
    base.platform,
    base.project_id,
    base.authors,
    base.aquisition_year,
    base.aquisition_month,
    base.aquisition_day,
    base.additional_information,
    base.data_access,
    base.citation_doi,
    base.archived,
    base.ortho_file_name,
    base.ortho_file_size,
    base.bbox,
    base.sha256,
    base.current_status,
    base.is_upload_done,
    base.is_ortho_done,
    base.is_cog_done,
    base.is_thumbnail_done,
    base.is_deadwood_done,
    base.is_forest_cover_done,
    base.is_metadata_done,
    base.is_odm_done,
    base.has_error,
    base.cog_file_name,
    base.cog_path,
    base.cog_file_size,
    base.thumbnail_file_name,
    base.thumbnail_path,
    base.admin_level_1,
    base.admin_level_2,
    base.admin_level_3,
    base.biome_name,
    base.has_labels,
    base.has_deadwood_prediction,
    base.freidata_doi,
    base.has_ml_tiles,
    base.ml_tiles_completed_at,
    base.pending_corrections_count,
    base.approved_corrections_count,
    base.rejected_corrections_count,
    base.total_corrections_count,
    base.is_combined_model_done,
    base.is_aoi_done,
    base.is_aoi_required,
    base.phenology_probability,
    review.details IS NOT NULL AS is_audited,
    (review.details ->> 'final_assessment') AS final_assessment,
    (review.details ->> 'deadwood_quality') AS deadwood_quality,
    (review.details ->> 'forest_cover_quality') AS forest_cover_quality,
    (review.details ->> 'has_major_issue')::boolean AS has_major_issue,
    (review.details ->> 'audit_date')::timestamptz AS audit_date,
    (review.details ->> 'has_valid_phenology')::boolean AS has_valid_phenology,
    (review.details ->> 'has_valid_acquisition_date')::boolean AS has_valid_acquisition_date,
    coalesce(review.details ->> 'deadwood_quality' IN ('great', 'sentinel_ok'), false) AS show_deadwood_predictions,
    coalesce(review.details ->> 'forest_cover_quality' IN ('great', 'sentinel_ok'), false) AS show_forest_cover_predictions,
    status.error_stage,
    EXISTS (
        SELECT 1 FROM public.v2_labels l
        WHERE l.dataset_id = base.id AND l.label_source = 'model_prediction'
        AND l.label_data = 'forest_cover'
    ) AS has_forest_cover_prediction,
    status.is_in_audit,
    (review.details ->> 'has_cog_issue')::boolean AS has_cog_issue,
    (review.details ->> 'has_thumbnail_issue')::boolean AS has_thumbnail_issue,
    (review.details ->> 'is_georeferenced')::boolean AS is_georeferenced
FROM public.v2_full_dataset_view base
LEFT JOIN public.v2_statuses status ON status.dataset_id = base.id
LEFT JOIN LATERAL public.get_dataset_status_details(base.id) review(details) ON true
WHERE base.user_id = auth.uid();
REVOKE ALL ON public.v2_full_dataset_view_owner FROM PUBLIC, anon;
GRANT SELECT ON public.v2_full_dataset_view_owner TO authenticated;
COMMENT ON VIEW public.v2_full_dataset_view_owner IS
    'Owner account read model, including excluded and archived datasets, without raw processor diagnostics.';

-- The public view uses invoker RLS: a hidden audit row must not turn an
-- excluded dataset into an apparently unreviewed public dataset.
create or replace view "public"."v2_full_dataset_view_public" as
select
  base.id,
  base.user_id,
  base.created_at,
  base.file_name,
  base.license,
  base.platform,
  base.project_id,
  base.authors,
  base.aquisition_year,
  base.aquisition_month,
  base.aquisition_day,
  base.additional_information,
  base.data_access,
  base.citation_doi,
  base.archived,
  base.ortho_file_name,
  base.ortho_file_size,
  base.bbox,
  base.sha256,
  base.current_status,
  base.is_upload_done,
  base.is_ortho_done,
  base.is_cog_done,
  base.is_thumbnail_done,
  base.is_deadwood_done,
  base.is_forest_cover_done,
  base.is_metadata_done,
  base.is_odm_done,
  base.is_audited,
  base.has_error,
  base.error_message,
  base.cog_file_name,
  base.cog_path,
  base.cog_file_size,
  base.thumbnail_file_name,
  base.thumbnail_path,
  base.admin_level_1,
  base.admin_level_2,
  base.admin_level_3,
  base.biome_name,
  base.has_labels,
  base.has_deadwood_prediction,
  base.freidata_doi,
  base.has_ml_tiles,
  base.ml_tiles_completed_at,
  base.pending_corrections_count,
  base.approved_corrections_count,
  base.rejected_corrections_count,
  base.total_corrections_count,
  audit_data.final_assessment,
  audit_data.deadwood_quality,
  audit_data.forest_cover_quality,
  audit_data.has_major_issue,
  audit_data.audit_date,
  audit_data.has_valid_phenology,
  audit_data.has_valid_acquisition_date,
  case
    when audit_data.deadwood_quality in ('great', 'sentinel_ok') then true
    else false
  end as show_deadwood_predictions,
  case
    when audit_data.forest_cover_quality in ('great', 'sentinel_ok') then true
    else false
  end as show_forest_cover_predictions,
  base.is_combined_model_done,
  base.is_aoi_done,
  base.is_aoi_required
from v2_full_dataset_view base
left join (
  select
    da.dataset_id,
    da.final_assessment,
    da.deadwood_quality::text,
    da.forest_cover_quality::text,
    da.has_major_issue,
    da.audit_date,
    da.has_valid_phenology,
    da.has_valid_acquisition_date
  from dataset_audit da
) audit_data on audit_data.dataset_id = base.id
where (
  (audit_data.final_assessment is null)
  or (audit_data.final_assessment <> 'exclude_completely'::text)
)
and base.archived = false
and not internal.is_dataset_excluded_from_public_surface(base.id);


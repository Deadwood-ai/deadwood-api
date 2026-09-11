import { Alert, Spin } from "antd";
import { useQuery } from "@tanstack/react-query";
import { supabase } from "../../hooks/useSupabase";
import { useAuth } from "../../hooks/useAuthProvider";
import type { DatasetAuditUserInfo } from "../../hooks/useDatasetAudit";

type OwnerReview = Pick<
  DatasetAuditUserInfo,
  | "audit_date"
  | "notes"
  | "is_georeferenced"
  | "has_valid_acquisition_date"
  | "acquisition_date_notes"
  | "has_valid_phenology"
  | "phenology_notes"
  | "deadwood_quality"
  | "deadwood_notes"
  | "forest_cover_quality"
  | "forest_cover_notes"
  | "has_cog_issue"
  | "cog_issue_notes"
  | "has_thumbnail_issue"
  | "thumbnail_issue_notes"
>;
export default function ReviewFindings({ datasetId }: { datasetId: number }) {
  const { user } = useAuth();
  const { data, isPending, isError } = useQuery({
    queryKey: ["owner-review-details", user?.id, datasetId],
    enabled: !!user,
    queryFn: async () => {
      const result = await supabase.rpc("get_dataset_status_details", {
        p_dataset_id: datasetId,
      });
      if (result.error) throw result.error;
      return result.data as OwnerReview | null;
    },
    staleTime: 15_000,
  });
  if (isPending)
    return <Spin size="small" aria-label="Loading review details" />;
  if (isError)
    return (
      <Alert
        type="warning"
        message="Review details could not be loaded. Please reopen to try again."
      />
    );
  if (!data)
    return (
      <p className="text-slate-500">
        A reviewer has not recorded an assessment yet.
      </p>
    );
  const findings = [
    {
      label: "Georeferencing",
      value:
        data.is_georeferenced === false
          ? "Needs attention"
          : data.is_georeferenced
            ? "Valid"
            : "Not assessed",
    },
    {
      label: "Acquisition date",
      value:
        data.has_valid_acquisition_date === false
          ? "Needs attention"
          : data.has_valid_acquisition_date
            ? "Valid"
            : "Not assessed",
      note: data.acquisition_date_notes,
    },
    {
      label: "Season / phenology",
      value:
        data.has_valid_phenology === false
          ? "Needs attention"
          : data.has_valid_phenology
            ? "Suitable"
            : "Not assessed",
      note: data.phenology_notes,
    },
    {
      label: "Deadwood prediction",
      value:
        data.deadwood_quality === "bad"
          ? "Quality issues · hidden from public display"
          : data.deadwood_quality
            ? "Accepted"
            : "Not assessed",
      note: data.deadwood_notes,
    },
    {
      label: "Tree cover prediction",
      value:
        data.forest_cover_quality === "bad"
          ? "Quality issues · hidden from public display"
          : data.forest_cover_quality
            ? "Accepted"
            : "Not assessed",
      note: data.forest_cover_notes,
    },
    {
      label: "Map image",
      value: data.has_cog_issue ? "Needs attention" : "No issue recorded",
      note: data.cog_issue_notes,
    },
    {
      label: "Preview image",
      value: data.has_thumbnail_issue ? "Needs attention" : "No issue recorded",
      note: data.thumbnail_issue_notes,
    },
  ];
  return (
    <div>
      {data.notes && (
        <p className="mb-4 whitespace-pre-wrap rounded-lg bg-amber-50 p-3 text-sm text-slate-700">
          {data.notes}
        </p>
      )}
      <dl className="space-y-3">
        {findings.map(({ label, value, note }) => (
          <div key={label}>
            <dt className="text-xs font-medium text-slate-500">{label}</dt>
            <dd className="m-0 text-sm text-slate-800">
              {value}
              {note && (
                <p className="mb-0 mt-1 whitespace-pre-wrap text-slate-500">
                  {note}
                </p>
              )}
            </dd>
          </div>
        ))}
      </dl>
    </div>
  );
}

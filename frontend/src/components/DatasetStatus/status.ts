import dayjs from "dayjs";
import type { DatasetProgress } from "../../utils/processingSteps";
import { isDatasetProcessingComplete } from "../../utils/processingSteps";
import type { QueueInfo } from "../../hooks/useQueuePositions";

export interface ContributorDataset extends DatasetProgress {
  id: number;
  file_name: string;
  data_access?: "public" | "private" | "viewonly";
  error_stage?: string | null;
  cog_path?: string | null;
  thumbnail_path?: string | null;
  has_deadwood_prediction?: boolean | null;
  has_forest_cover_prediction?: boolean | null;
  archived?: boolean;
  is_in_audit?: boolean;
  has_cog_issue?: boolean | null;
  has_thumbnail_issue?: boolean | null;
  is_georeferenced?: boolean | null;
}

export type QueueState =
  | { state: "loading" }
  | { state: "error" }
  | { state: "loaded"; item?: QueueInfo };

// Sentence-case stage names; they are read mid-sentence ("stopped during ...").
const stages: Record<string, string> = {
  geotiff: "image preparation",
  geotiff_dependency: "image preparation",
  ortho_processing: "image preparation",
  metadata: "metadata extraction",
  metadata_processing: "metadata extraction",
  cog: "map image preparation",
  cog_processing: "map image preparation",
  thumbnail: "preview generation",
  thumbnail_processing: "preview generation",
  odm_processing: "orthomosaic generation",
  deadwood_segmentation: "deadwood analysis",
  treecover_segmentation: "tree cover analysis",
  forest_cover_segmentation: "tree cover analysis",
  deadwood_treecover_combined_segmentation: "combined AI analysis",
  deadwood_treecover_combined_v2: "combined AI analysis",
  aoi_segmentation: "area selection",
  embedding_processing: "search indexing",
};

export function stageLabel(stage: string | null | undefined): string | undefined {
  return stage ? stages[stage] : undefined;
}

export interface GeneratedOutputs {
  map: boolean;
  preview: boolean;
  deadwood: boolean;
  treecover: boolean;
}

// "Generated" means the file or prediction rows exist. It says nothing about quality.
export function generatedOutputs(dataset: ContributorDataset): GeneratedOutputs {
  return {
    map: !!(dataset.is_cog_done && dataset.cog_path),
    preview: !!dataset.thumbnail_path,
    deadwood: !!dataset.has_deadwood_prediction,
    treecover: !!dataset.has_forest_cover_prediction,
  };
}

export function canOpenOwnerMap(dataset: ContributorDataset): boolean {
  return generatedOutputs(dataset).map;
}

export type TagColor = "error" | "warning" | "processing" | "success" | "default";

export interface ProcessingOutcome {
  kind: "failed" | "running" | "checking" | "unavailable" | "queued" | "complete" | "incomplete";
  label: string;
  color: TagColor;
  detail: string;
}

// Only allow-listed stage names reach user copy; the stored stage is an
// internal identifier and is never echoed.
function failurePlace(dataset: ContributorDataset): string {
  const stage = stageLabel(dataset.error_stage);
  if (stage) return `during ${stage}`;
  if (dataset.error_stage) return "at a step this page cannot describe";
  return "at a step that was not recorded";
}

export function processingStatus(dataset: ContributorDataset, queue: QueueState): ProcessingOutcome {
  const mapAvailable = canOpenOwnerMap(dataset);
  if (dataset.has_error) {
    const place = failurePlace(dataset);
    return mapAvailable
      ? {
          kind: "failed",
          label: "Stopped · map available",
          color: "warning",
          detail: `Processing stopped ${place}. The map image was generated before that; the results list shows which analysis layers exist.`,
        }
      : {
          kind: "failed",
          label: "Processing failed",
          color: "error",
          detail: `Processing stopped ${place}. No map image is available.`,
        };
  }
  if (dataset.current_status && !["idle", "audit_in_progress"].includes(dataset.current_status)) {
    const stage = stageLabel(dataset.current_status);
    return {
      kind: "running",
      label: "Processing",
      color: "processing",
      detail: stage ? `Current step: ${stage}.` : "Processing is running.",
    };
  }
  if (queue.state === "loading") {
    return { kind: "checking", label: "Checking status", color: "default", detail: "Checking the processing queue." };
  }
  if (queue.state === "error") {
    return {
      kind: "unavailable",
      label: "Status unavailable",
      color: "default",
      detail: "The processing queue could not be checked. Refresh to try again.",
    };
  }
  if (queue.item) {
    return {
      kind: "queued",
      label: "Queued",
      color: "processing",
      detail: queue.item.current_position
        ? `Position ${queue.item.current_position} in the processing queue.`
        : "Waiting for a processing worker.",
    };
  }
  // A review lock does not undo completed processing.
  const complete = isDatasetProcessingComplete({
    ...dataset,
    current_status: dataset.current_status === "audit_in_progress" ? "idle" : dataset.current_status,
  });
  if (complete) {
    return { kind: "complete", label: "Results ready", color: "success", detail: "All processing steps completed." };
  }
  return {
    kind: "incomplete",
    label: mapAvailable ? "Incomplete · map available" : "Incomplete",
    color: "warning",
    detail: dataset.is_upload_done
      ? "Some processing steps did not complete and nothing is queued for this dataset."
      : "The upload did not complete and nothing is queued for this dataset.",
  };
}

const GREEN = "#287254";
const AMBER = "#b45309";
const SLATE = "#64748b";

export interface ReviewOutcome {
  kind: "none" | "in_progress" | "excluded" | "fixable" | "findings" | "accepted";
  label: string;
  color: string;
  /** Date the review was recorded. It does not say which processing run it looked at. */
  date: string | null;
}

export function reviewStatus(dataset: ContributorDataset): ReviewOutcome {
  const date = dataset.audit_date ? dayjs(dataset.audit_date).format("D MMM YYYY") : null;
  if (dataset.is_in_audit || dataset.current_status === "audit_in_progress") {
    return { kind: "in_progress", label: "Review in progress", color: SLATE, date };
  }
  const assessment = dataset.final_assessment === "ready" ? "no_issues" : dataset.final_assessment;
  if (assessment === "exclude_completely") {
    return { kind: "excluded", label: "Excluded from public map", color: AMBER, date };
  }
  if (assessment === "fixable_issues") {
    return { kind: "fixable", label: "Reviewed · fixable issues", color: AMBER, date };
  }
  if (!assessment) {
    return { kind: "none", label: "Not yet reviewed", color: SLATE, date: null };
  }
  const deadwoodHidden = dataset.deadwood_quality === "bad";
  const treecoverHidden = dataset.forest_cover_quality === "bad";
  const otherFindings =
    !!dataset.has_cog_issue ||
    !!dataset.has_thumbnail_issue ||
    dataset.is_georeferenced === false ||
    dataset.has_valid_phenology === false ||
    dataset.has_valid_acquisition_date === false;
  const label =
    deadwoodHidden && treecoverHidden
      ? "Reviewed · analysis layers hidden"
      : deadwoodHidden
        ? "Reviewed · deadwood layer hidden"
        : treecoverHidden
          ? "Reviewed · tree cover layer hidden"
          : otherFindings
            ? "Reviewed · see findings"
            : "Reviewed · accepted";
  const findings = deadwoodHidden || treecoverHidden || otherFindings;
  return { kind: findings ? "findings" : "accepted", label, color: findings ? AMBER : GREEN, date };
}

/** True when the contributor should read the details before relying on this dataset. */
export function needsAttention(processing: ProcessingOutcome, review: ReviewOutcome): boolean {
  return (
    processing.kind === "failed" ||
    processing.kind === "incomplete" ||
    review.kind === "excluded" ||
    review.kind === "fixable"
  );
}

export interface StatusSummary {
  /** One or two sentences answering "what happened" and "what exists". */
  headline: string;
  /** What the contributor can do now, or null when nothing is expected of them. */
  next: string | null;
}

// The summary only states what the stored fields prove. It never promises a
// retry, a follow-up, or that a review covered the current outputs.
export function statusSummary(dataset: ContributorDataset, queue: QueueState): StatusSummary {
  const processing = processingStatus(dataset, queue);
  const review = reviewStatus(dataset);
  const contact = `If you need help with this dataset, contact the DeadTrees team and include dataset ${dataset.id}.`;
  const reviewClause =
    review.kind === "excluded"
      ? ` A reviewer excluded it from the public map${review.date ? ` on ${review.date}` : ""}; it stays in your account.`
      : review.kind === "in_progress"
        ? " A quality review is in progress."
        : review.kind === "none"
          ? " No quality review has been recorded yet."
          : ` A quality review was recorded${review.date ? ` on ${review.date}` : ""}.`;

  switch (processing.kind) {
    case "failed":
    case "incomplete":
      return { headline: processing.detail + reviewClause, next: contact };
    case "running":
      return { headline: processing.detail + reviewClause, next: "Nothing to do while it runs." };
    case "queued":
      return { headline: processing.detail + reviewClause, next: "Nothing to do while it waits." };
    case "checking":
      return { headline: processing.detail, next: null };
    case "unavailable":
      return { headline: processing.detail, next: null };
    case "complete":
      return {
        headline: processing.detail + reviewClause,
        next:
          review.kind === "excluded" || review.kind === "fixable" || review.kind === "findings"
            ? "The review findings below explain what a reviewer flagged."
            : null,
      };
  }
}

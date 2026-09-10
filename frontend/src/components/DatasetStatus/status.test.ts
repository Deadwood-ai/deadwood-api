import { describe, expect, it } from "vitest";
import {
  canOpenOwnerMap,
  generatedOutputs,
  needsAttention,
  processingStatus,
  reviewStatus,
  statusSummary,
  type ContributorDataset,
  type QueueState,
} from "./status";

const complete: ContributorDataset = {
  id: 12,
  file_name: "survey.tif",
  current_status: "idle",
  is_upload_done: true,
  is_ortho_done: true,
  is_metadata_done: true,
  is_cog_done: true,
  is_thumbnail_done: true,
  is_deadwood_done: true,
  is_forest_cover_done: true,
  cog_path: "12/ortho.tif",
  thumbnail_path: "12/thumb.png",
  has_deadwood_prediction: true,
  has_forest_cover_prediction: true,
  final_assessment: "no_issues",
  audit_date: "2026-08-09T10:00:00Z",
};
const emptyQueue: QueueState = { state: "loaded" };

describe("processing outcome", () => {
  it("labels a failure after a usable map as stopped, not failed", () => {
    const state = processingStatus(
      { ...complete, has_error: true, error_stage: "deadwood_treecover_combined_v2" },
      emptyQueue,
    );
    expect(state.label).toBe("Stopped · map available");
    expect(state.color).toBe("warning");
    expect(state.detail).toContain("stopped during combined AI analysis");
    expect(state.detail).not.toContain("No map image");
  });

  it("labels a failure without a map as failed and says no map exists", () => {
    const state = processingStatus(
      { ...complete, has_error: true, error_stage: "cog_processing", cog_path: null, is_cog_done: false },
      emptyQueue,
    );
    expect(state.label).toBe("Processing failed");
    expect(state.color).toBe("error");
    expect(state.detail).toContain("map image preparation");
    expect(state.detail).toContain("No map image is available");
  });

  it("never echoes an unrecognized stage identifier into user copy", () => {
    const state = processingStatus({ ...complete, has_error: true, error_stage: "internal_step_x" }, emptyQueue);
    expect(state.detail).not.toContain("internal_step_x");
    expect(state.detail).toContain("cannot describe");
  });

  it("does not infer a failed stage from incomplete flags", () => {
    const state = processingStatus({ ...complete, has_error: true, is_thumbnail_done: false }, emptyQueue);
    expect(state.detail).toContain("not recorded");
  });

  it("recognizes a queued rerun even when previous outputs are complete", () => {
    expect(
      processingStatus(complete, {
        state: "loaded",
        item: { dataset_id: 12, current_position: 4, estimated_time: null, task_types: ["cog_processing"] },
      }).label,
    ).toBe("Queued");
  });

  it.each(["loading", "error"] as const)("does not treat %s queue data as an empty queue", (state) => {
    expect(processingStatus(complete, { state }).label).not.toBe("Results ready");
    expect(processingStatus(complete, { state }).kind).toBe(state === "loading" ? "checking" : "unavailable");
  });

  it("does not declare readiness when thumbnail or required AOI is missing", () => {
    expect(processingStatus({ ...complete, is_thumbnail_done: false }, emptyQueue).label).toBe(
      "Incomplete · map available",
    );
    expect(
      processingStatus({ ...complete, is_aoi_required: true, is_aoi_done: false, cog_path: null }, emptyQueue).label,
    ).toBe("Incomplete");
  });

  it("preserves legacy readiness but respects active final stages", () => {
    expect(processingStatus(complete, emptyQueue).label).toBe("Results ready");
    const running = processingStatus({ ...complete, current_status: "embedding_processing" }, emptyQueue);
    expect(running.label).toBe("Processing");
    expect(running.detail).toContain("search indexing");
  });

  it("keeps completed processing while a review lock is active", () => {
    expect(processingStatus({ ...complete, is_in_audit: true }, emptyQueue).label).toBe("Results ready");
    expect(processingStatus({ ...complete, current_status: "audit_in_progress" }, emptyQueue).label).toBe(
      "Results ready",
    );
  });
});

describe("generated outputs", () => {
  it("reports each artifact independently and never treats the map as proof of predictions", () => {
    const dataset = { ...complete, has_deadwood_prediction: false, has_forest_cover_prediction: null };
    expect(generatedOutputs(dataset)).toEqual({ map: true, preview: true, deadwood: false, treecover: false });
    expect(canOpenOwnerMap(dataset)).toBe(true);
    expect(canOpenOwnerMap({ ...dataset, cog_path: null })).toBe(false);
  });
});

describe("review outcome", () => {
  it("names the hidden layer instead of a generic findings label", () => {
    expect(reviewStatus({ ...complete, deadwood_quality: "bad" }).label).toBe("Reviewed · deadwood layer hidden");
    expect(reviewStatus({ ...complete, forest_cover_quality: "bad" }).label).toBe(
      "Reviewed · tree cover layer hidden",
    );
    expect(reviewStatus({ ...complete, deadwood_quality: "bad", forest_cover_quality: "bad" }).label).toBe(
      "Reviewed · analysis layers hidden",
    );
  });

  it("surfaces image findings even when the dataset assessment was accepted", () => {
    expect(reviewStatus({ ...complete, has_cog_issue: true }).label).toBe("Reviewed · see findings");
    expect(reviewStatus({ ...complete, is_georeferenced: false }).label).toBe("Reviewed · see findings");
    expect(reviewStatus(complete).label).toBe("Reviewed · accepted");
  });

  it("keeps review findings visible when later processing fails", () => {
    const dataset = { ...complete, has_error: true, error_stage: "cog_processing", deadwood_quality: "bad" as const };
    expect(reviewStatus(dataset).kind).toBe("findings");
    expect(reviewStatus(dataset).date).toBe("9 Aug 2026");
  });

  it("separates exclusion from completed processing", () => {
    const dataset = { ...complete, final_assessment: "exclude_completely" as const };
    expect(processingStatus(dataset, emptyQueue).label).toBe("Results ready");
    expect(reviewStatus(dataset).label).toBe("Excluded from public map");
  });

  it("recognizes the current audit lock and the legacy ready value", () => {
    expect(reviewStatus({ ...complete, is_in_audit: true }).label).toBe("Review in progress");
    expect(reviewStatus({ ...complete, final_assessment: "ready" }).label).toBe("Reviewed · accepted");
    expect(reviewStatus({ ...complete, final_assessment: null }).label).toBe("Not yet reviewed");
  });
});

describe("attention and summary", () => {
  it("asks for attention only where the details change what the contributor should rely on", () => {
    const review = reviewStatus(complete);
    expect(needsAttention(processingStatus(complete, emptyQueue), review)).toBe(false);
    expect(needsAttention(processingStatus({ ...complete, has_error: true }, emptyQueue), review)).toBe(true);
    expect(needsAttention(processingStatus(complete, { state: "loading" }), review)).toBe(false);
    expect(
      needsAttention(processingStatus(complete, emptyQueue), reviewStatus({ ...complete, final_assessment: "exclude_completely" })),
    ).toBe(true);
    expect(
      needsAttention(processingStatus(complete, emptyQueue), reviewStatus({ ...complete, deadwood_quality: "bad" })),
    ).toBe(false);
  });

  it("never promises a retry, a follow-up, or that the review covered the failed run", () => {
    const summary = statusSummary(
      { ...complete, has_error: true, error_stage: "deadwood_treecover_combined_v2" },
      emptyQueue,
    );
    expect(summary.headline).toContain("stopped during combined AI analysis");
    expect(summary.headline).toContain("A quality review was recorded on 9 Aug 2026.");
    expect(summary.next).toContain("include dataset 12");
    for (const banned of ["retry", "working on it", "will follow up", "No action"]) {
      expect(`${summary.headline} ${summary.next}`.toLowerCase()).not.toContain(banned.toLowerCase());
    }
  });

  it("does not expect anything from the contributor while processing runs or waits", () => {
    expect(statusSummary({ ...complete, current_status: "cog_processing" }, emptyQueue).next).toBe(
      "Nothing to do while it runs.",
    );
    expect(
      statusSummary(complete, {
        state: "loaded",
        item: { dataset_id: 12, current_position: 2, estimated_time: null, task_types: null },
      }).next,
    ).toBe("Nothing to do while it waits.");
    expect(statusSummary(complete, emptyQueue).next).toBeNull();
  });

  it("explains exclusion without naming who can fix it", () => {
    const summary = statusSummary({ ...complete, final_assessment: "exclude_completely" }, emptyQueue);
    expect(summary.headline).toContain("excluded it from the public map on 9 Aug 2026; it stays in your account");
    expect(summary.next).toBe("The review findings below explain what a reviewer flagged.");
  });
});

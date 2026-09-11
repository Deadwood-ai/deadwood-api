import { Alert, Button, Drawer, Tag } from "antd";
import { useIsMobile } from "../../hooks/useIsMobile";
import ReviewFindings from "./ReviewFindings";
import {
  canOpenOwnerMap,
  generatedOutputs,
  processingStatus,
  reviewStatus,
  statusSummary,
  type ContributorDataset,
  type QueueState,
} from "./status";

interface StatusDrawerProps {
  dataset: ContributorDataset;
  queue: QueueState;
  /**
   * Called as soon as the user asks to close. The caller unmounts the drawer
   * and restores focus. Waiting for the close animation is deliberately avoided:
   * an Escape during the opening motion would otherwise never complete the close.
   */
  onClose: () => void;
  onViewMap: (id: number) => void;
}

export default function StatusDrawer({ dataset, queue, onClose, onViewMap }: StatusDrawerProps) {
  const mobile = useIsMobile();
  const processing = processingStatus(dataset, queue);
  const review = reviewStatus(dataset);
  const summary = statusSummary(dataset, queue);
  const outputs = generatedOutputs(dataset);
  const mapAvailable = canOpenOwnerMap(dataset);
  const hiddenNote = (quality: ContributorDataset["deadwood_quality"]) =>
    quality === "bad" ? " · hidden from the public map" : "";
  const results = [
    ["Map image", outputs.map, ""],
    ["Preview image", outputs.preview, ""],
    ["Deadwood prediction", outputs.deadwood, hiddenNote(dataset.deadwood_quality)],
    ["Tree cover prediction", outputs.treecover, hiddenNote(dataset.forest_cover_quality)],
  ] as const;

  return (
    <Drawer open title="Dataset status" width={mobile ? "100%" : 460} onClose={onClose} destroyOnHidden>
      <p className="mb-1 text-xs font-medium uppercase tracking-wide text-slate-500">Dataset {dataset.id}</p>
      <h2 className="mb-4 break-words text-lg font-semibold text-slate-900">{dataset.file_name}</h2>

      {dataset.archived && (
        <Alert
          className="mb-4"
          type="info"
          message="Archived dataset"
          description="This dataset is hidden from your active list. Its status remains available here."
        />
      )}

      <section className="mb-6 rounded-lg bg-slate-50 p-4" aria-label="Summary">
        <div className="mb-2 flex flex-wrap items-center gap-2">
          <Tag color={processing.color} className="dt-status-tag m-0">
            {processing.label}
          </Tag>
          <span className="text-sm font-medium" style={{ color: review.color }}>
            {review.label}
          </span>
        </div>
        <p className="m-0 text-sm text-slate-800">{summary.headline}</p>
        {summary.next && <p className="mb-0 mt-2 text-sm text-slate-600">{summary.next}</p>}
      </section>

      <section className="mb-6" aria-label="Generated results">
        <h3 className="mb-3 text-sm font-semibold">Generated results</h3>
        <dl className="m-0 space-y-2">
          {results.map(([label, generated, note]) => (
            <div key={label} className="flex justify-between gap-3 text-sm">
              <dt className="text-slate-600">{label}</dt>
              <dd className="m-0 text-right" style={{ color: generated ? "#287254" : "#64748b" }}>
                {generated ? `Generated${note}` : "Not available"}
              </dd>
            </div>
          ))}
        </dl>
        <p className="mb-0 mt-3 text-xs text-slate-500">
          Generated means the file exists. Whether it is good enough is judged in the quality review.
        </p>
        {mapAvailable && (
          <Button className="mt-3" onClick={() => onViewMap(dataset.id)}>
            View map
          </Button>
        )}
      </section>

      <section className="border-t border-slate-100 pt-5" aria-label="Quality review">
        <h3 className="mb-2 text-sm font-semibold">Quality review</h3>
        <p className="mb-1 text-sm font-medium" style={{ color: review.color }}>
          {review.label}
        </p>
        {review.date && (
          <p className="mb-3 text-xs text-slate-500">
            Recorded {review.date}. The date does not indicate which processing run was reviewed.
          </p>
        )}
        {review.kind === "excluded" && (
          <Alert
            className="mb-4"
            type="warning"
            message="Visible to you, excluded from the public map"
            description="Your dataset remains in your account so you can read the findings and contact us."
          />
        )}
        <ReviewFindings datasetId={dataset.id} />
      </section>

      <p className="mb-0 mt-7 border-t border-slate-100 pt-4 text-sm text-slate-500">
        Need help?{" "}
        <a href={`mailto:info@deadtrees.earth?subject=Dataset%20${dataset.id}`}>Contact the DeadTrees team</a> and
        include dataset {dataset.id}.
      </p>
    </Drawer>
  );
}

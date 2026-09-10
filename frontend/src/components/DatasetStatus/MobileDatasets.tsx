import { Button, Empty, Spin } from "antd";
import { EnvironmentOutlined } from "@ant-design/icons";
import StatusCell from "./StatusCell";
import {
  canOpenOwnerMap,
  needsAttention,
  processingStatus,
  reviewStatus,
  type ContributorDataset,
  type QueueState,
} from "./status";

interface MobileDatasetsProps {
  datasets: ContributorDataset[];
  loading: boolean;
  queueFor: (id: number) => QueueState;
  onDetails: (id: number, trigger: HTMLElement) => void;
  onViewMap: (id: number) => void;
}

/**
 * Read-only compact list for narrow screens. Each row carries the same status
 * grammar and the same two controls as the desktop table; management tools stay
 * desktop-only. Rows themselves are not clickable, so the two buttons are the
 * only interactive elements and keep full-size touch targets.
 */
export default function MobileDatasets({ datasets, loading, queueFor, onDetails, onViewMap }: MobileDatasetsProps) {
  if (loading) return <Spin aria-label="Loading datasets" />;
  if (!datasets.length) return <Empty description="No datasets yet" />;
  return (
    <ul
      className="m-0 list-none divide-y divide-slate-200 rounded-xl border border-slate-200 bg-white p-0"
      aria-label="My datasets"
    >
      {datasets.map((dataset) => {
        const queue = queueFor(dataset.id);
        const attention = needsAttention(processingStatus(dataset, queue), reviewStatus(dataset));
        return (
          <li key={dataset.id} className="flex items-start gap-3 px-4 py-3">
            <div className="min-w-0 flex-1">
              <p className="mb-1 truncate text-sm font-medium text-slate-800" title={dataset.file_name}>
                {dataset.file_name}
              </p>
              <StatusCell dataset={dataset} queue={queue} />
            </div>
            <div className="flex shrink-0 flex-col items-stretch gap-2">
              {canOpenOwnerMap(dataset) && (
                <Button
                  icon={<EnvironmentOutlined />}
                  className="!h-10"
                  onClick={() => onViewMap(dataset.id)}
                  aria-label={`View map for dataset ${dataset.id}`}
                >
                  View map
                </Button>
              )}
              <Button
                className={`!h-10${attention ? " !font-semibold" : ""}`}
                onClick={(event) => onDetails(dataset.id, event.currentTarget)}
                aria-label={`View status details for dataset ${dataset.id}`}
              >
                {attention ? "Check details" : "Details"}
              </Button>
            </div>
          </li>
        );
      })}
    </ul>
  );
}

import { Button, Tag } from "antd";
import {
  CheckCircleOutlined,
  ClockCircleOutlined,
  ExclamationCircleOutlined,
  SyncOutlined,
} from "@ant-design/icons";
import {
  needsAttention,
  processingStatus,
  reviewStatus,
  type ContributorDataset,
  type QueueState,
  type ReviewOutcome,
} from "./status";

interface StatusCellProps {
  dataset: ContributorDataset;
  queue: QueueState;
  /** When omitted, no inline Details link is rendered (the caller provides its own). */
  onDetails?: (trigger: HTMLElement) => void;
}

function reviewIcon(review: ReviewOutcome) {
  switch (review.kind) {
    case "in_progress":
      return <SyncOutlined spin />;
    case "none":
      return <ClockCircleOutlined />;
    case "accepted":
      return <CheckCircleOutlined />;
    default:
      return <ExclamationCircleOutlined />;
  }
}

/**
 * Two-line status: the processing outcome on top, the quality review below it.
 * Slot order carries the distinction visually; the sr-only labels carry it for
 * assistive technology.
 */
export default function StatusCell({ dataset, queue, onDetails }: StatusCellProps) {
  const processing = processingStatus(dataset, queue);
  const review = reviewStatus(dataset);
  const attention = needsAttention(processing, review);
  return (
    <div className="py-0.5" data-testid={`dataset-status-${dataset.id}`}>
      <div data-testid={`dataset-processing-${dataset.id}`}>
        <span className="sr-only">Processing: </span>
        <Tag color={processing.color} className="dt-status-tag m-0">
          {processing.label}
        </Tag>
      </div>
      <div className="mt-1.5 flex flex-wrap items-center gap-x-2 gap-y-0.5 text-xs">
        <span
          className="inline-flex items-center gap-1"
          style={{ color: review.color }}
          data-testid={`dataset-review-${dataset.id}`}
        >
          {reviewIcon(review)}
          <span>
            <span className="sr-only">Quality review: </span>
            {review.label}
          </span>
        </span>
        {onDetails && (
          <Button
            type="link"
            size="small"
            className={`!h-6 !px-0 !text-xs${attention ? " !font-semibold" : ""}`}
            onClick={(event) => onDetails(event.currentTarget)}
            aria-label={`View status details for dataset ${dataset.id}`}
          >
            {attention ? "Check details" : "Details"}
          </Button>
        )}
      </div>
    </div>
  );
}

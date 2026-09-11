import { useQuery } from "@tanstack/react-query";
import { supabase } from "./useSupabase";

export interface QueueInfo {
  dataset_id: number;
  current_position: number | null;
  estimated_time: number | null;
  task_types: string[] | null;
}

export type QueueInfoByDatasetId = Record<number, QueueInfo>;

export function useQueuePositions(datasetIds: number[] | undefined) {
  return useQuery<QueueInfoByDatasetId>({
    queryKey: ["queue-positions", Array.isArray(datasetIds) ? [...new Set(datasetIds)].sort((a, b) => a - b) : []],
    enabled: !!datasetIds && datasetIds.length > 0,
    queryFn: async ({ signal }) => {
      if (!datasetIds || datasetIds.length === 0) {
        return {};
      }

      const uniqueSortedIds = [...new Set(datasetIds)].sort((a, b) => a - b);

      // Chunk to avoid very long URLs in PostgREST GET requests
      const chunkSize = 200;
      const chunks: number[][] = [];
      for (let i = 0; i < uniqueSortedIds.length; i += chunkSize) {
        chunks.push(uniqueSortedIds.slice(i, i + chunkSize));
      }

      const results = await Promise.all(
        chunks.map(async (ids) => {
          const { data, error } = await supabase
            .from("v2_queue_positions")
            .select("dataset_id,current_position,estimated_time,task_types")
            .in("dataset_id", ids)
            .abortSignal(signal)
            .retry(false);
          if (error) throw error;
          return data as unknown as QueueInfo[];
        }),
      );

      const byId: QueueInfoByDatasetId = {};
      for (const group of results) {
        for (const row of group) {
          if (row && typeof row.dataset_id === "number") {
            byId[row.dataset_id] = row;
          }
        }
      }
      return byId;
    },
    // React Query owns retries; avoid nesting the SDK retry loop inside it.
    retry: 1,
    staleTime: 15 * 1000,
    gcTime: 60 * 1000,
    refetchInterval: 15 * 1000,
  });
}

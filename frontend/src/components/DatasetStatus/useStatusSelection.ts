import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { supabase } from "../../hooks/useSupabase";
import { useAuth } from "../../hooks/useAuthProvider";
import { Settings } from "../../config";
import type { ContributorDataset } from "./status";

export function useStatusSelection(datasets: ContributorDataset[]) {
  const [params, setParams] = useSearchParams();
  const { user } = useAuth();
  const value = Number(params.get("dataset"));
  const datasetId =
    Number.isSafeInteger(value) && value > 0 ? value : undefined;
  const listed = datasets.find((dataset) => dataset.id === datasetId);
  const detail = useQuery({
    queryKey: ["owner-dataset-status", user?.id, datasetId],
    enabled: !!user && !!datasetId && !listed,
    queryFn: async () => {
      const result = await supabase
        .from(Settings.DATA_TABLE_OWNER)
        .select("*")
        .eq("id", datasetId)
        .maybeSingle();
      if (result.error) throw result.error;
      return result.data as ContributorDataset | null;
    },
    staleTime: 15_000,
  });
  const select = (id?: number) =>
    setParams(
      (current) => {
        const next = new URLSearchParams(current);
        if (id) next.set("dataset", String(id));
        else next.delete("dataset");
        return next;
      },
      { replace: true },
    );
  return {
    dataset: listed ?? detail.data,
    datasetId,
    select,
    isError: !!datasetId && !listed && detail.isError,
    isMissing: !!datasetId && !listed && detail.isSuccess && !detail.data,
  };
}

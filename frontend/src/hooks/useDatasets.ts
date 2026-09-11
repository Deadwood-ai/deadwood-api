import { useQuery } from "@tanstack/react-query";
import { useAuth } from "./useAuthProvider";
import { supabase } from "./useSupabase";
import { Settings } from "../config";
import { IDataset, IDatasetArchiveItem } from "../types/dataset";
import { fixTextEncoding } from "../utils/textUtils";
import { getPublicDatasetByIdQueryKey } from "../utils/datasetDetailNavigation";

interface DatasetQueryOptions {
  enabled?: boolean;
}

// Base datasets hook - includes ALL datasets (for admin/audit use)
export function useDatasets(options: DatasetQueryOptions = {}) {
  const { status, user } = useAuth();

  return useQuery({
    queryKey: ["datasets", status, user?.id ?? "anonymous"],
    queryFn: async () => {
      const { data, error } = await supabase.from(Settings.DATA_TABLE_FULL).select("*");
      if (error) throw error;
      return data;
    },
    enabled: (options.enabled ?? true) && status !== "checking",
    staleTime: 5 * 60 * 1000, // 5 minutes - data is fresh for 5 minutes
    gcTime: 10 * 60 * 1000, // 10 minutes - keep in cache for 10 minutes
  });
}

// Public datasets hook - excludes datasets marked as "exclude_completely"
export function usePublicDatasets(options: DatasetQueryOptions = {}) {
  const { status, user } = useAuth();

  return useQuery({
    queryKey: ["public-datasets", status, user?.id ?? "anonymous"],
    queryFn: async () => {
      const { data, error } = await supabase.from(Settings.DATA_TABLE_PUBLIC).select("*");
      if (error) throw error;
      return data;
    },
    enabled: (options.enabled ?? true) && status !== "checking",
    staleTime: 5 * 60 * 1000, // 5 minutes - data is fresh for 5 minutes
    gcTime: 10 * 60 * 1000, // 10 minutes - keep in cache for 10 minutes
  });
}

// Public archive hook - narrow rows for the /dataset archive list, timeline, filters, and map
export function usePublicDatasetArchiveItems(options: DatasetQueryOptions = {}) {
  const { status, user } = useAuth();

  return useQuery({
    queryKey: ["public-dataset-archive-items", status, user?.id ?? "anonymous"],
    queryFn: async () => {
      const { data, error } = await supabase
        .from(Settings.DATASET_ARCHIVE_ITEMS_VIEW)
        .select("*")
        .order("id", { ascending: false });
      if (error) throw error;
      return data as IDatasetArchiveItem[];
    },
    enabled: (options.enabled ?? true) && status !== "checking",
    staleTime: 5 * 60 * 1000,
    gcTime: 10 * 60 * 1000,
  });
}

// Public single dataset hook - optimized for dataset details page
export function usePublicDatasetById(datasetId: number | undefined) {
  const { status, user } = useAuth();

	return useQuery({
		queryKey: getPublicDatasetByIdQueryKey(datasetId, status, user?.id),
		enabled: !!datasetId && status !== "checking",
		queryFn: async () => {
			if (!datasetId) return null;
			const { data, error } = await supabase
				.from(Settings.DATA_TABLE_PUBLIC)
				.select("*")
				.eq("id", datasetId)
				.maybeSingle();
			if (error) throw error;
			if (user && (!data || data.user_id === user.id)) {
				const owner = await supabase.from(Settings.DATA_TABLE_OWNER).select("*").eq("id", datasetId).maybeSingle();
				if (owner.error) throw owner.error;
				if (owner.data) return owner.data as IDataset;
			}
			return (data as IDataset | null) ?? null;
		},
		staleTime: 5 * 60 * 1000,
		gcTime: 10 * 60 * 1000,
	});
}

// Fetch a single dataset by id; minimal fields are enough for Tiles page
export function useDatasetById(datasetId: number | undefined) {
  const { status, user } = useAuth();

  return useQuery({
    queryKey: ["dataset-by-id", datasetId, status, user?.id ?? "anonymous"],
    enabled: !!datasetId && status !== "checking",
    queryFn: async () => {
      if (!datasetId) return null;
      const { data, error } = await supabase.from(Settings.DATA_TABLE_FULL).select("*").eq("id", datasetId).single();
      if (error) throw error;
      return data as IDataset;
    },
  });
}

// Owner account includes excluded datasets; archived rows remain accessible by direct status link.
export function useUserDatasets(options: DatasetQueryOptions = {}) {
  const { session, status } = useAuth();

  return useQuery({
    queryKey: ["userDatasets", session?.user?.id],
    queryFn: async () => {
      const { data, error } = await supabase
        .from(Settings.DATA_TABLE_OWNER)
        .select("*")
        .eq("user_id", session?.user.id)
        .eq("archived", false);
      if (error) throw error;
      return data;
    },
    enabled: (options.enabled ?? true) && status === "authenticated" && !!session?.user?.id,
    staleTime: 5 * 60 * 1000, // 5 minutes - data is fresh for 5 minutes
    gcTime: 10 * 60 * 1000, // 10 minutes - keep in cache for 10 minutes
  });
}

// Authors list - based on public datasets only
export function useAuthors(options: DatasetQueryOptions = {}) {
  const { status, user } = useAuth();
  const { data: datasets } = usePublicDatasetArchiveItems({ enabled: options.enabled });

  return useQuery({
    queryKey: ["authors", status, user?.id ?? "anonymous"],
    enabled: (options.enabled ?? true) && !!datasets,
    queryFn: () => {
      // Flatten all authors arrays and remove duplicates
      const allAuthors = datasets
        ?.flatMap((item) => item.authors || [])
        .filter(Boolean)
        .map((author) => fixTextEncoding(author).replace(/\s+/g, " ").trim())
        .filter(Boolean);

      const authorsUnique = [...new Set(allAuthors)].sort((a, b) => a.localeCompare(b));

      return authorsUnique.map((author) => ({
        label: author,
        value: author,
      }));
    },
  });
}

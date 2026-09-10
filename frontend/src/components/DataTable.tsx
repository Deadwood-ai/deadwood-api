import React, { useState, useEffect, useMemo, useRef } from "react";

import { Button, Table, Tag, Tooltip, Dropdown, MenuProps, Modal, message, Alert } from "antd";
import type { ColumnsType } from "antd/es/table";
import type { SortOrder } from "antd/es/table/interface";
import { useNavigate } from "react-router-dom";
import { useUserDatasets } from "../hooks/useDatasets";
import {
  SyncOutlined,
  EnvironmentOutlined,
  PlusOutlined,
  DownOutlined,
  EditOutlined,
  MinusOutlined,
  DeleteOutlined,
  EyeOutlined,
  LockOutlined,
} from "@ant-design/icons";
import { supabase } from "../hooks/useSupabase";
import { useAuth } from "../hooks/useAuthProvider";
import EditDatasetModal from "./EditDatasetModal";
import StatusCell from "./DatasetStatus/StatusCell";
import StatusDrawer from "./DatasetStatus/StatusDrawer";
import MobileDatasets from "./DatasetStatus/MobileDatasets";
import { useStatusSelection } from "./DatasetStatus/useStatusSelection";
import {
  canOpenOwnerMap,
  processingStatus,
  type ContributorDataset,
  type QueueState,
} from "./DatasetStatus/status";
import { isGeonadirDataset } from "../utils/datasetUtils";
import { fixAuthorNamesEncoding, sanitizeText } from "../utils/textUtils";
import { IDataset } from "../types/dataset";
import { useQueuePositions } from "../hooks/useQueuePositions";
import { useQueryClient } from "@tanstack/react-query";
import { useIsMobile } from "../hooks/useIsMobile";
import { useCanUploadPrivate } from "../hooks/useUserPrivileges";
import { openDatasetDetail } from "../utils/datasetDetailNavigation";

interface Dataset extends ContributorDataset {
  id: number;
  file_name: string;
  aquisition_day?: number;
  aquisition_month?: number;
  aquisition_year?: number;
  platform?: string;
  authors?: string[];
  additional_information?: string;
  citation_doi?: string;
  freidata_doi?: string;
  admin_level_1: string | null;
  admin_level_2: string | null;
  admin_level_3: string | null;
  current_status?: string;
  has_error: boolean;
  error_stage?: string | null;
  cog_path?: string | null;
  thumbnail_path?: string | null;
  has_deadwood_prediction?: boolean;
  has_forest_cover_prediction?: boolean;
  is_upload_done: boolean;
  is_ortho_done: boolean;
  is_cog_done: boolean;
  is_thumbnail_done: boolean;
  is_metadata_done: boolean;
  is_deadwood_done: boolean;
  is_forest_cover_done: boolean;
  is_combined_model_done: boolean;
  is_aoi_done: boolean;
  is_aoi_required: boolean;
  isInPublication?: boolean; // Track if dataset is in publication process
  data_access?: "public" | "private" | "viewonly";
  archived?: boolean;
  final_assessment?: "ready" | "fixable_issues" | "no_issues" | "exclude_completely" | null;
  audit_date?: string | null;
  deadwood_quality?: "great" | "sentinel_ok" | "bad" | null;
  forest_cover_quality?: "great" | "sentinel_ok" | "bad" | null;
  has_valid_phenology?: boolean | null;
  has_valid_acquisition_date?: boolean | null;
}

interface DataTableProps {
  onSelectedRowsChange?: (selectedRows: Dataset[]) => void;
  resetSelection?: boolean; // Flag to reset selection
  onResetSelectionComplete?: () => void; // Callback when reset is complete
}

const DataTable: React.FC<DataTableProps> = ({
  onSelectedRowsChange,
  resetSelection = false,
  onResetSelectionComplete,
}) => {
  const [selectedRowKeys, setSelectedRowKeys] = useState<React.Key[]>([]);
  const { data: userData, isLoading: isLoadingData, isError: isDataError } = useUserDatasets();
  const { status, user } = useAuth();
  const [datasetsInPublication, setDatasetsInPublication] = useState<number[]>([]);

  // State for edit modal
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [selectedDatasetForEdit, setSelectedDatasetForEdit] = useState<Dataset | null>(null);

  // State for archive confirmation modal
  const [archiveModalVisible, setArchiveModalVisible] = useState(false);
  const [datasetToArchive, setDatasetToArchive] = useState<Dataset | null>(null);
  const [isArchiving, setIsArchiving] = useState(false);

  const nav = useNavigate();
  const queryClient = useQueryClient();
  const isMobile = useIsMobile();
  const { canUpload: canUploadPrivate } = useCanUploadPrivate();

  // Sort datasets by ID descending (newest first) for initial render
  const sortedUserData = useMemo(
    () => (userData ? [...(userData as Dataset[])].sort((a, b) => b.id - a.id) : []),
    [userData]
  );

  // Queue positions for user datasets
  const datasetIds = useMemo(() => (userData ? (userData as Dataset[]).map((d) => d.id) : []), [userData]);
  const selection = useStatusSelection(sortedUserData);
  const queue = useQueuePositions(selection.datasetId ? [...datasetIds, selection.datasetId] : datasetIds);
  const queueFor = (id: number): QueueState => queue.isError ? { state: "error" } : queue.isPending ? { state: "loading" } : { state: "loaded", item: queue.data[id] };

  // Focus returns to the control that opened the drawer. Email deep links open
  // without a trigger, so nothing is focused for them.
  const detailsTrigger = useRef<HTMLElement | null>(null);
  const openDetails = (id: number, trigger?: HTMLElement) => {
    detailsTrigger.current = trigger ?? null;
    selection.select(id);
  };
  const closeDetails = () => {
    selection.select();
    const trigger = detailsTrigger.current;
    detailsTrigger.current = null;
    if (trigger?.isConnected) trigger.focus();
  };

  // Shared with the menu item: refreshes the detail query before navigating.
  const viewMap = (datasetId: number) =>
    openDatasetDetail({ queryClient, navigate: nav, datasetId, authStatus: status, userId: user?.id });

  // Effect to reset selection when requested
  useEffect(() => {
    if (resetSelection) {
      setSelectedRowKeys([]);
      if (onSelectedRowsChange) {
        onSelectedRowsChange([]);
      }
      if (onResetSelectionComplete) {
        onResetSelectionComplete();
      }
    }
  }, [resetSelection, onSelectedRowsChange, onResetSelectionComplete]);

  // Fetch datasets that are in publication process
  useEffect(() => {
    const fetchDatasetsInPublication = async () => {
      if (!user) return;

      try {
        // Get all publications by this user that don't have a DOI yet
        const { data: publications } = await supabase.from("data_publication").select("id, doi").eq("user_id", user.id);

        if (!publications || publications.length === 0) return;

        // Filter publications that don't have a DOI yet
        const pendingPublicationIds = publications.filter((pub) => !pub.doi).map((pub) => pub.id);

        if (pendingPublicationIds.length === 0) return;

        // Get all datasets linked to these pending publications
        const { data: linkedDatasets } = await supabase
          .from("jt_data_publication_datasets")
          .select("dataset_id")
          .in("publication_id", pendingPublicationIds);

        if (!linkedDatasets) return;

        // Extract the dataset IDs
        const pendingDatasetIds = linkedDatasets.map((item) => item.dataset_id);
        setDatasetsInPublication(pendingDatasetIds);
      } catch (error) {
        console.error("Error fetching datasets in publication:", error);
      }
    };

    fetchDatasetsInPublication();
  }, [user]);

  // Dataset is eligible for publishing when processing artifacts and metadata are ready (predictions not required)
  const isDatasetPublishEligible = (record: Dataset): boolean => {
    return !!(
      record.final_assessment !== "exclude_completely" &&
      !record.has_error &&
      record.is_upload_done &&
      record.is_ortho_done &&
      record.is_cog_done &&
      record.is_thumbnail_done &&
      record.is_metadata_done
    );
  };

  const handleAddToSelection = (record: Dataset) => {
    const newKeys = [...selectedRowKeys, record.id];
    setSelectedRowKeys(newKeys);

    if (onSelectedRowsChange && userData) {
      const selectedRows = userData.filter((item) => newKeys.includes(item.id));
      onSelectedRowsChange(selectedRows as Dataset[]);
    }
  };

  const handleRemoveFromSelection = (record: Dataset) => {
    const newKeys = selectedRowKeys.filter((key) => key !== record.id);
    setSelectedRowKeys(newKeys);

    if (onSelectedRowsChange && userData) {
      const selectedRows = userData.filter((item) => newKeys.includes(item.id));
      onSelectedRowsChange(selectedRows as Dataset[]);
    }
  };

  const handleEditDataset = (record: Dataset) => {
    setSelectedDatasetForEdit(record);
    setEditModalVisible(true);
  };

  const handleCloseEditModal = () => {
    setEditModalVisible(false);
    setSelectedDatasetForEdit(null);
  };

  // Archive dataset handlers
  const handleArchiveClick = (record: Dataset) => {
    setDatasetToArchive(record);
    setArchiveModalVisible(true);
  };

  const handleArchiveConfirm = async () => {
    if (!datasetToArchive) return;

    setIsArchiving(true);
    try {
      const { error } = await supabase
        .from("v2_datasets")
        .update({ archived: true })
        .eq("id", datasetToArchive.id);

      if (error) throw error;

      message.success(`Dataset "${datasetToArchive.file_name}" has been archived`);
      // Invalidate queries to refresh the data
      await queryClient.invalidateQueries({ queryKey: ["userDatasets"] });
    } catch (error) {
      console.error("Error archiving dataset:", error);
      message.error("Failed to archive dataset");
    } finally {
      setIsArchiving(false);
      setArchiveModalVisible(false);
      setDatasetToArchive(null);
    }
  };

  const handleArchiveCancel = () => {
    setArchiveModalVisible(false);
    setDatasetToArchive(null);
  };

  const handleUpdateVisibility = async (record: Dataset, dataAccess: "public" | "private") => {
    try {
      const { error } = await supabase
        .from("v2_datasets")
        .update({ data_access: dataAccess })
        .eq("id", record.id);

      if (error) throw error;

      message.success(`Dataset is now ${dataAccess}`);
      await queryClient.invalidateQueries({ queryKey: ["userDatasets"] });
      await queryClient.invalidateQueries({ queryKey: ["public-datasets"] });
    } catch (error) {
      console.error(`Error updating dataset visibility to ${dataAccess}:`, error);
      message.error(`Failed to make dataset ${dataAccess}`);
    }
  };

  const handleMakePublic = async (record: Dataset) => {
    Modal.confirm({
      title: "Make dataset public?",
      content: "This dataset will become visible on the public platform once it meets the normal display requirements.",
      okText: "Make Public",
      onOk: () => handleUpdateVisibility(record, "public"),
    });
  };

  const handleMakePrivate = async (record: Dataset) => {
    Modal.confirm({
      title: "Make dataset private?",
      content: "This dataset will no longer be publicly visible and will remain available only to you.",
      okText: "Make Private",
      okButtonProps: { danger: true },
      onOk: () => handleUpdateVisibility(record, "private"),
    });
  };

  const getActionMenuItems = (record: Dataset): MenuProps["items"] => {
    const canView = canOpenOwnerMap(record);
    const canPublish = isDatasetPublishEligible(record);
    const isSelected = selectedRowKeys.includes(record.id);
    const isPublished = !!record.freidata_doi || !!record.citation_doi;

    const publishAction = isSelected
      ? {
        key: "remove-publish",
        label: "Remove from Publication",
        icon: <MinusOutlined />,
        onClick: () => handleRemoveFromSelection(record),
      }
      : {
        key: "publish",
        label: "Quick Publish",
        icon: <PlusOutlined />,
        disabled: !canPublish || isPublished,
        onClick: () => handleAddToSelection(record),
      };

    // Visibility actions:
    // - all owners keep the existing "Make Public" action for private datasets
    // - only privileged users get "Make Private" for public datasets
    const isPrivate = record.data_access === "private";
    const isPublic = record.data_access === "public";
    const visibilityAction = isPrivate
      ? {
        key: "visibility",
        label: "Make Public",
        icon: <EyeOutlined />,
        onClick: () => handleMakePublic(record),
      }
      : canUploadPrivate && isPublic
        ? {
          key: "visibility",
          label: "Make Private",
          icon: <LockOutlined />,
          onClick: () => handleMakePrivate(record),
        }
      : null;

    // Archive action
    const archiveAction = {
      key: "archive",
      label: "Archive Dataset",
      icon: <DeleteOutlined />,
      danger: true,
      onClick: () => handleArchiveClick(record),
    };

    return [
      {
        key: "view",
        label: "View Map",
        icon: <EnvironmentOutlined />,
        disabled: !canView,
        onClick: () => viewMap(record.id),
      },
      {
        key: "edit",
        label: "Edit Metadata",
        icon: <EditOutlined />,
        onClick: () => handleEditDataset(record),
      },
      // Only show "Make Public" for private datasets
      ...(visibilityAction ? [visibilityAction] : []),
      // Only show publish/remove action if not already published
      ...(!isPublished ? [publishAction] : []),
      { type: "divider" as const },
      archiveAction,
    ];
  };

  const columns: ColumnsType<Dataset> = [
    {
      title: "File Name",
      dataIndex: "file_name",
      key: "file_name",
      width: 165,
      // First and pinned so the row stays identifiable when narrow screens scroll the middle columns.
      fixed: "left",
      ellipsis: true,
      sorter: (a: Dataset, b: Dataset) => {
        // Case-insensitive string comparison
        const fileNameA = a.file_name?.toLowerCase() || "";
        const fileNameB = b.file_name?.toLowerCase() || "";
        return fileNameA.localeCompare(fileNameB);
      },
      render: (fileName: string, record: Dataset) => {
        const info = sanitizeText(record.additional_information || "");
        return (
          <div>
            <Tooltip title={fileName}>
              <span className="block max-w-[145px] truncate">{fileName}</span>
            </Tooltip>
            {info && <Tooltip title={info}><span className="block max-w-[145px] truncate text-xs text-slate-500">{info}</span></Tooltip>}
          </div>
        );
      },
    },
    {
      title: "ID",
      dataIndex: "id",
      key: "id",
      defaultSortOrder: "descend" as const,
      sortDirections: ["descend", "ascend"] as SortOrder[],
      sorter: (a: Dataset, b: Dataset) => a.id - b.id,
      width: 70,
      responsive: ["lg"] as const,
    },
    {
      title: "Date",
      dataIndex: "aquisition_day",
      key: "aquisition_day",
      responsive: ["lg"] as const,
      width: 95,
      sorter: (a: Dataset, b: Dataset) => {
        // Create comparable date values (YYYYMMDD format for sorting)
        const dateA = (a.aquisition_year || 0) * 10000 + (a.aquisition_month || 0) * 100 + (a.aquisition_day || 0);
        const dateB = (b.aquisition_year || 0) * 10000 + (b.aquisition_month || 0) * 100 + (b.aquisition_day || 0);
        return dateA - dateB;
      },
      render: (_: unknown, record: Dataset) => (
        <span>
          {record.aquisition_day && record.aquisition_day + "/"}
          {record.aquisition_month && record.aquisition_month + "/"}
          {record.aquisition_year}
        </span>
      ),
    },
    {
      title: "Authors",
      dataIndex: "authors",
      key: "authors",
      responsive: ["lg"] as const,
      width: 150,
      render: (authors: string[] | undefined, record: Dataset) => {
        if (!authors || authors.length === 0) return null;

        // Clean author names to fix encoding issues
        const cleanedAuthors = fixAuthorNamesEncoding(authors);
        if (cleanedAuthors.length === 0) return null;

        const isFromGeonadir = isGeonadirDataset(record as unknown as IDataset);
        const maxVisible = 2;
        const visibleAuthors = cleanedAuthors.slice(0, maxVisible);
        const remainingCount = cleanedAuthors.length - maxVisible;

        // Cap overly long author strings to avoid oversized tags
        const MAX_AUTHOR_CHARS = 30;
        const truncateAuthorName = (name: string) =>
          name.length > MAX_AUTHOR_CHARS ? name.slice(0, MAX_AUTHOR_CHARS - 1) + "…" : name;

        return (
          <div className="flex flex-wrap gap-1">
            {visibleAuthors.map((author, index) => (
              <Tooltip key={index} title={author}>
                <Tag color="geekblue" className="text-xs">
                  {truncateAuthorName(author)}
                </Tag>
              </Tooltip>
            ))}
            {remainingCount > 0 && (
              <Tooltip title={`Additional authors: ${cleanedAuthors.slice(maxVisible).join(", ")}`}>
                <Tag color="default" className="text-xs">
                  +{remainingCount} more
                </Tag>
              </Tooltip>
            )}
            {isFromGeonadir && (
              <Tag color="orange" className="text-xs">
                via GeoNadir
              </Tag>
            )}
          </div>
        );
      },
    },
    {
      title: "Access",
      dataIndex: "data_access",
      key: "data_access",
      responsive: ["lg"] as const,
      width: 85,
      filters: [
        { text: "Public", value: "public" },
        { text: "Private", value: "private" },
      ],
      onFilter: (value: unknown, record: Dataset) => record.data_access === value,
      render: (access: string | undefined) => {
        if (access === "public") return <Tag color="green">Public</Tag>;
        if (access === "private") return <Tag color="default">Private</Tag>;
        if (access === "viewonly") return <Tag color="orange">View Only</Tag>;
        return <Tag color="default">Private</Tag>;
      },
    },
    {
      title: "Publication",
      dataIndex: "freidata_doi",
      key: "publication_status",
      responsive: ["lg"] as const,
      width: 145,
      render: (freidataDoiValue: string | undefined, record: Dataset) => {
        // Dataset has a FreiDATA DOI
        if (freidataDoiValue) {
          return (
            <Tooltip title="View publication">
              <a href={`https://doi.org/${freidataDoiValue}`} target="_blank" rel="noopener noreferrer">
                <img src={`https://freidata.uni-freiburg.de/badge/DOI/${freidataDoiValue}.svg`} alt="FreiDATA badge" />
              </a>
            </Tooltip>
          );
        }

        // Dataset has a regular DOI (already published elsewhere)
        if (record.citation_doi) {
          return (
            <Tooltip title="View publication">
              <Button
                type="link"
                size="small"
                className="m-0 p-0"
                href={`https://doi.org/${record.citation_doi}`}
                target="_blank"
                rel="noopener noreferrer"
              >
                <img src={`https://zenodo.org/badge/DOI/${record.citation_doi}.svg`} alt="Zenodo badge" />
              </Button>
            </Tooltip>
          );
        }

        // Dataset is in publication process but doesn't have a DOI yet
        if (datasetsInPublication.includes(record.id)) {
          return (
            <Tooltip title="Publication in review">
              <Tag color="orange" icon={<SyncOutlined spin />}>
                In Review
              </Tag>
            </Tooltip>
          );
        }

        // Dataset has no DOI, show add/remove button based on selection state
        const isSelected = selectedRowKeys.includes(record.id);
        const canPublish = isDatasetPublishEligible(record);

        if (isSelected) {
          // Show remove button for selected datasets
          return (
            <Button
              type="default"
              size="small"
              icon={<MinusOutlined />}
              onClick={(e) => {
                e.stopPropagation();
                handleRemoveFromSelection(record);
              }}
            >
              Remove
            </Button>
          );
        } else {
          // Show add button for unselected datasets
          return (
            <Button
              type="primary"
              size="small"
              icon={<PlusOutlined />}
              onClick={(e) => {
                e.stopPropagation();
                handleAddToSelection(record);
              }}
              disabled={!canPublish}
            >
              request DOI
            </Button>
          );
        }
      },
    },
    {
      title: "Status",
      key: "status",
      width: 205,
      fixed: "right",
      render: (_: unknown, record: Dataset) => (
        <StatusCell
          dataset={record}
          queue={queueFor(record.id)}
          onDetails={(trigger) => openDetails(record.id, trigger)}
        />
      ),
    },
    {
      title: "Map",
      key: "map",
      width: 120,
      fixed: "right",
      align: "left",
      render: (_: unknown, record: Dataset) => {
        if (canOpenOwnerMap(record)) {
          return (
            <Button
              size="small"
              icon={<EnvironmentOutlined />}
              onClick={() => viewMap(record.id)}
              aria-label={`View map for dataset ${record.id}`}
            >
              View map
            </Button>
          );
        }
        const processing = processingStatus(record, queueFor(record.id)).kind;
        const settled = processing === "failed" || processing === "incomplete" || processing === "complete";
        return (
          <span className="whitespace-nowrap text-xs text-slate-400" aria-hidden={!settled}>
            {settled ? "No map" : ""}
          </span>
        );
      },
    },
    {
      title: "Actions",
      dataIndex: "id",
      key: "actions",
      width: 105,
      fixed: "right",
      align: "left",
      render: (_: number, record: Dataset) => (
        <Dropdown menu={{ items: getActionMenuItems(record) }} trigger={["click"]} placement="bottomRight">
          <Button size="small">
            Actions <DownOutlined />
          </Button>
        </Dropdown>
      ),
    },
  ];

  return (
    <>
      {isDataError && <Alert className="mb-4" type="error" message="Your datasets could not be loaded. Please refresh to try again." />}
      {(selection.isError || selection.isMissing) && <Alert className="mb-4" type="warning" message={selection.isError ? "Dataset status could not be loaded. Please refresh to try again." : "This dataset is not available in your account."} closable onClose={() => selection.select()} />}
      {selection.dataset && (
        <StatusDrawer
          key={selection.dataset.id}
          dataset={selection.dataset}
          queue={queueFor(selection.dataset.id)}
          onClose={closeDetails}
          onViewMap={viewMap}
        />
      )}
      {isMobile ? <MobileDatasets datasets={sortedUserData} loading={isLoadingData} queueFor={queueFor} onDetails={openDetails} onViewMap={viewMap} /> : <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
        <Table
          rowKey={"id"}
          dataSource={sortedUserData}
          columns={columns}
          scroll={{ x: "max-content" }}
          pagination={{ pageSize: 50 }}
          loading={isLoadingData}
          rowClassName={(record) => {
            const isSelected = selectedRowKeys.includes(record.id);
            return isSelected ? "bg-blue-50 hover:bg-blue-100" : "";
          }}
        />
      </div>}

      {selectedDatasetForEdit && (
        <EditDatasetModal visible={editModalVisible} onClose={handleCloseEditModal} dataset={selectedDatasetForEdit} />
      )}

      {/* Archive Confirmation Modal */}
      <Modal
        title="Archive Dataset"
        open={archiveModalVisible}
        onOk={handleArchiveConfirm}
        onCancel={handleArchiveCancel}
        okText="Archive"
        okButtonProps={{ danger: true, loading: isArchiving }}
        cancelButtonProps={{ disabled: isArchiving }}
      >
        <p>
          Are you sure you want to archive <strong>{datasetToArchive?.file_name}</strong>?
        </p>
        <p className="text-gray-500 text-sm mt-2">
          This will hide the dataset from your profile. It will no longer be visible on the public map or used for
          analysis. The data will be preserved and can be restored by contacting support.
        </p>
      </Modal>
    </>
  );
};

export default DataTable;

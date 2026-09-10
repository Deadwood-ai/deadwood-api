import { useState } from "react";
import { Avatar, Badge, Button, Segmented, Typography, Table, Tag, Tooltip } from "antd";
import { useAuth } from "../hooks/useAuthProvider";
import DataTable from "../components/DataTable";
import UploadButton from "../components/Upload/UploadButton";
import { useNavigate, Link } from "react-router-dom";
// import { useUserDatasets } from "../hooks/useDatasets";
import { useMyFlags } from "../hooks/useDatasetFlags";
import type { DatasetFlag } from "../types/flags";
import { FileOutlined } from "@ant-design/icons";
import PublicationModal from "../components/PublicationModal";
import PublicationsTable from "../components/PublicationsTable";
import { useIsMobile } from "../hooks/useIsMobile";
import { useAnalytics } from "../hooks/useAnalytics";
import ProcessingEmailPreference from "../components/ProcessingEmailPreference";

interface ProfileAvatarProps {
  email: string;
  size?: number;
}

interface DatasetType {
  id: number;
  file_name: string;
  data_access?: "public" | "private" | "viewonly";
  aquisition_year?: number;
  citation_doi?: string;
  freidata_doi?: string;
  current_status?: string;
  is_upload_done?: boolean;
  is_ortho_done?: boolean;
  is_cog_done?: boolean;
  is_thumbnail_done?: boolean;
  is_metadata_done?: boolean;
}

enum ActiveTab {
  MyDatasets = "My Datasets",
  Publications = "Published Datasets",
  MyIssues = "My Issues",
}

export function ProfileAvatar({ email, size = 84 }: ProfileAvatarProps) {
  // Create a consistent hash from email for the seed
  const emailHash = email.toLowerCase().trim();

  // Use DiceBear API with the email hash as seed
  const avatarUrl = `https://api.dicebear.com/7.x/initials/svg?seed=${emailHash}`;

  return <Avatar size={size} src={avatarUrl} alt={`Avatar for ${email}`} className="bg-primary/10" />;
}

export default function ProfilePage() {
  const { session, user } = useAuth();
  const navigate = useNavigate();
  const isMobile = useIsMobile();
  const { track } = useAnalytics("profile");

  // const { data: userData } = useUserDatasets();
  const { data: myFlags = [] } = useMyFlags();

  const [activeTab, setActiveTab] = useState<ActiveTab>(ActiveTab.MyDatasets);
  const [selectedDatasets, setSelectedDatasets] = useState<DatasetType[]>([]);
  const [isPublicationModalVisible, setIsPublicationModalVisible] = useState(false);
  const [resetSelectionFlag, setResetSelectionFlag] = useState(false);

  const handleSelectedRowsChange = (rows: DatasetType[]) => {
    setSelectedDatasets(rows);
  };

  const showPublicationModal = () => {
    track("publish_started", {
      dataset_count: selectedDatasets.length,
    });
    setIsPublicationModalVisible(true);
  };

  const handlePublicationModalCancel = () => {
    setIsPublicationModalVisible(false);
  };

  const handlePublicationSuccess = () => {
    // Close the modal
    setIsPublicationModalVisible(false);
    // Trigger selection reset
    setResetSelectionFlag(true);
  };

  const handleResetComplete = () => {
    setResetSelectionFlag(false);
  };

  if (!session) {
    return null;
  } else {
    if (isMobile) {
      return (
        <div className="min-h-[calc(100vh-64px)] w-full bg-[#F8FAF9] pb-16 pt-24">
          <div className="mx-auto max-w-3xl px-4">
            <div className="mb-3 flex items-center gap-3 rounded-xl border border-slate-200 bg-white px-4 py-3">
              <Badge count={myFlags.length} color="red">
                <ProfileAvatar email={user?.email ?? ""} size={44} />
              </Badge>
              <div className="min-w-0">
                <Typography.Title level={5} style={{ margin: 0 }}>
                  My Account
                </Typography.Title>
                <Typography.Text className="block truncate text-xs" type="secondary">
                  {user?.email}
                </Typography.Text>
              </div>
            </div>

            <ProcessingEmailPreference userId={user?.id} />

            <section aria-label="My datasets">
              <h2 className="mb-1 text-lg font-semibold">My datasets</h2>
              <p className="mb-3 text-xs text-slate-500" data-testid="mobile-desktop-only-note">
                Uploads, publishing and dataset management are available on a desktop browser.
              </p>
              <DataTable />
            </section>
          </div>
        </div>
      );
    }

    return (
      <div className="w-full bg-[#F8FAF9] min-h-[calc(100vh-64px)] pb-24 pt-24 md:pt-28">
        <div className="mx-auto max-w-[1920px] px-4 md:px-8 xl:px-12">
          <div className="flex flex-col lg:flex-row items-start lg:items-center justify-between gap-6 lg:gap-8 pb-8 lg:pb-12">
            <div className="flex items-center gap-6">
              <Badge count={myFlags.length} color="red">
                <ProfileAvatar email={user?.email ?? ""} size={96} />
              </Badge>
              <div className="flex min-w-0 flex-col">
                <Typography.Title level={2} style={{ margin: 0, fontWeight: 700 }}>
                  My Account
                </Typography.Title>
                <Typography.Text className="text-lg font-medium break-all" type="secondary">
                  {user?.email}
                </Typography.Text>
              </div>
            </div>
            <div className="w-full lg:w-auto lg:max-w-2xl">
              <div className="rounded-2xl border border-blue-100 bg-blue-50/50 p-6 shadow-sm">
                <div className="flex items-start gap-3">
                  <div className="text-xl">💡</div>
                  <div>
                    <h3 className="mb-2 text-base font-semibold text-blue-900">Upload and Publish Your Data</h3>
                    <div className="space-y-2 text-sm text-blue-800/80">
                      <p className="m-0">
                        Upload and visualize your data on the platform. Publish datasets via{" "}
                        <a href="https://freidata.uni-freiburg.de/" target="_blank" rel="noopener noreferrer" className="font-semibold underline">
                          FreiDATA
                        </a>{" "}
                        to get a DOI.
                      </p>
                      <ul className="m-0 space-y-1 pl-0" style={{ listStyleType: "none" }}>
                        <li>
                          <span className="font-medium text-blue-900">Formats:</span> Standalone GeoTIFF (max 20GB) or ZIP with raw drone images - JPEG, JPG (max 30GB)
                        </li>
                        <li>
                          <span className="font-medium text-blue-900">Raw Images:</span> For orthomosaic generation, use the{" "}
                          <Link to="/releases/drone-mapping-guide" className="font-semibold underline">
                            drone mapping guide
                          </Link>
                        </li>
                        <li>
                          <span className="font-medium text-blue-900">Requirements:</span> RGB/NIRRGB, {"<"}10cm resolution, any coordinate reference system
                        </li>
                      </ul>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <ProcessingEmailPreference userId={user?.id} />
          <div className="w-full">
            <div className="mb-6 flex flex-col gap-3 md:flex-row md:justify-between md:items-center">
              <div className="w-full md:w-auto overflow-x-auto">
                <Segmented
                  options={["My Datasets", "Published Datasets", "My Issues"]}
                  size={isMobile ? "middle" : "large"}
                  value={activeTab}
                  onChange={(value) => {
                    setActiveTab(value as ActiveTab);
                  }}
                  className="shadow-sm border border-gray-200/50"
                />
              </div>
              <div className="flex w-full justify-end gap-2 md:w-auto">
                {activeTab === ActiveTab.MyDatasets ? (
                  <>
                    {selectedDatasets.length > 0 ? (
                      <Button size="large" type="primary" icon={<FileOutlined />} onClick={showPublicationModal} className="shadow-sm">
                        Publish Data ({selectedDatasets.length})
                      </Button>
                    ) : (
                      <UploadButton />
                    )}
                  </>
                ) : null}
              </div>
            </div>

            <div className="rounded-2xl border border-gray-200/60 bg-white p-6 shadow-sm">
              {activeTab === ActiveTab.MyDatasets ? (
                <DataTable
                  onSelectedRowsChange={handleSelectedRowsChange}
                  resetSelection={resetSelectionFlag}
                  onResetSelectionComplete={handleResetComplete}
                />
              ) : activeTab === ActiveTab.Publications ? (
                <PublicationsTable />
              ) : (
                <div>
                  {myFlags.length === 0 ? (
                    <div className="my-12 flex flex-col items-center justify-center text-center">
                      <Typography.Title level={4} className="mb-2">No issues yet</Typography.Title>
                      <Typography.Text type="secondary" className="text-base">
                        Report an issue from any dataset’s details page to see it here.
                      </Typography.Text>
                    </div>
                  ) : (
                    <>
                      <div className="mb-6">
                        <Typography.Title level={4} style={{ margin: 0 }}>My Issues</Typography.Title>
                        <Typography.Text type="secondary">
                          User-reported issues you've filed. Only you and auditors can view them.
                        </Typography.Text>
                      </div>
                      <div className="overflow-hidden rounded-xl border border-gray-100">
                        <Table
                          rowKey="id"
                          dataSource={myFlags}
                          columns={[
                          {
                            title: "Dataset ID",
                            dataIndex: "dataset_id",
                            key: "dataset_id",
                            responsive: ["xs"],
                            render: (id: number) => (
                              <Link to={`/dataset/${id}`} className="font-medium text-[#1B5E35] hover:underline">
                                {id}
                              </Link>
                            ),
                          },
                          {
                            title: "Description",
                            key: "description",
                            responsive: ["sm"],
                            render: (_: unknown, f: DatasetFlag) => (
                              <Tooltip title={f.description}>
                                <span className="text-gray-600">{(f.description || "").slice(0, 120) + (f.description.length > 120 ? "…" : "")}</span>
                              </Tooltip>
                            ),
                          },
                          {
                            title: "Categories",
                            key: "categories",
                            responsive: ["md"],
                            render: (_: unknown, f: DatasetFlag) => (
                              <div className="flex gap-1">
                                {f.is_ortho_mosaic_issue && <Tag color="orange" className="m-0 border-none bg-orange-50 font-medium">Orthomosaic</Tag>}
                                {f.is_prediction_issue && <Tag color="blue" className="m-0 border-none bg-blue-50 font-medium">Segmentation</Tag>}
                              </div>
                            ),
                          },
                          {
                            title: "Status",
                            dataIndex: "status",
                            key: "status",
                            responsive: ["xs"],
                            render: (status: string) => (
                              <Tag 
                                className="m-0 border-none font-medium capitalize"
                                color={status === "open" ? "red" : status === "acknowledged" ? "gold" : "green"}
                              >
                                {status}
                              </Tag>
                            ),
                          },
                          {
                            title: "Created",
                            dataIndex: "created_at",
                            key: "created_at",
                            responsive: ["sm"],
                            render: (iso: string) => <span className="text-gray-500">{new Date(iso).toLocaleString()}</span>,
                          },
                          // Removed last status change per requirements
                          {
                            title: "Actions",
                            key: "actions",
                            responsive: ["xs"],
                            render: (_: unknown, f: DatasetFlag) => (
                              <Button size="small" onClick={() => navigate(`/dataset/${f.dataset_id}`)}>
                                View Map
                              </Button>
                            ),
                          },
                          ]}
                          pagination={{ pageSize: 10 }}
                          scroll={{ x: isMobile ? 560 : "max-content" }}
                        />
                      </div>
                    </>
                  )}
                </div>
              )}
            </div>

            <PublicationModal
              visible={isPublicationModalVisible}
              onCancel={handlePublicationModalCancel}
              datasets={selectedDatasets}
              onSuccess={handlePublicationSuccess}
            />
          </div>
        </div>
      </div>
    );
  }
}

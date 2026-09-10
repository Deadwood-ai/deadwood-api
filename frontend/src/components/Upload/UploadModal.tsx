import { useState, useRef } from "react";
import {
  Button,
  Form,
  Radio,
  Space,
  Upload,
  Modal,
  Alert,
  Input,
  Select,
  Tooltip,
  Checkbox,
  Typography,
  Collapse,
  message,
} from "antd";
import { InfoCircleOutlined, InboxOutlined, LockOutlined } from "@ant-design/icons";
import { useAuth } from "../../hooks/useAuthProvider";
import { ILicense, IPlatform, UploadType } from "../../types/dataset";
import { useFileUpload } from "../../hooks/useFileUpload";
import { useUploadNotification } from "../../hooks/useUploadNotification";
import PickerWithType from "./PickerWithType";
import uploadOrtho from "../../api/uploadOrtho";
import { useData } from "../../hooks/useDataProvider";
import addProcess from "../../api/addProcess";
import uploadLabelObject from "../../api/uploadLabelObject";
import useLabelsFileUpload from "../../hooks/useLabelsFileUpload";
import { useCanUploadPrivate } from "../../hooks/useUserPrivileges";
import {
  detectUploadType,
  validateFileSize,
  validateGeoTiffAiEligibility,
  validateZipCompressionMethods,
} from "../../utils/fileValidation";
import { isInvalidSessionError } from "../../utils/authSession";

import { isTokenExpiringSoon } from "../../utils/isTokenExpiringSoon";
import { clearLocalSupabaseSession, supabase } from "../../hooks/useSupabase";
import { RcFile } from "antd/es/upload";
import type { Dayjs } from "dayjs";
import { useAnalytics } from "../../hooks/useAnalytics";
import { Link } from "react-router-dom";
// New interfaces
interface IFormValues {
  license: ILicense;
  platform: IPlatform;
  spectral_properties: string;
  aquisition_date: Dayjs | null;
  author: string[];
  doi: string;
  additional_information: string;
  labels_description: string;
  is_private: boolean;
}

interface UploadModalProps {
  isVisible: boolean;
  onClose: () => void;
  uploadKey: string;
}

// Add these types near the top of the file
interface UploadResponse {
  id: string;
}

interface UploadMetadata {
  license: string;
  platform: string;
  authors: string[];
  upload_type: string;
  project_id?: string;
  aquisition_year?: number;
  aquisition_month?: number;
  aquisition_day?: number;
  additional_information?: string;
  data_access?: string;
  citation_doi?: string;
}

function createLabelObjectFormData(
  datasetId: string,
  userId: string,
  labelFile: RcFile,
  labelDescription: string,
): FormData {
  const formData = new FormData();
  formData.append("dataset_id", datasetId);
  formData.append("user_id", userId);
  formData.append("file", labelFile);
  formData.append("file_alias", labelFile.name.split(".")[0]);
  formData.append("label_description", labelDescription);
  formData.append("file_type", labelFile.name.split(".")[1]);
  return formData;
}

const TermsLink = () => (
  <Typography.Link href="/terms-of-service" target="_blank">
    terms of service
  </Typography.Link>
);

const PrivacyLink = () => (
  <Typography.Link href="/datenschutzerklaerung" target="_blank">
    privacy policy
  </Typography.Link>
);

const PREDICTION_PROCESSING_STEPS = [
  "deadwood_v1",
  "treecover_v1",
  "deadwood_treecover_combined_v2",
];

const GEOTIFF_PROCESSING_STEPS = [
  "geotiff",
  "cog",
  "thumbnail",
  "metadata",
  "aoi_v1",
  ...PREDICTION_PROCESSING_STEPS,
  // Open-vocabulary tile embeddings run last (needs the standardized ortho).
  "embeddings_v1",
];

const RAW_IMAGES_PROCESSING_STEPS = [
  "odm_processing",
  ...GEOTIFF_PROCESSING_STEPS,
];

const UploadModal: React.FC<UploadModalProps> = ({ isVisible, onClose, uploadKey }) => {
  const pickerTypeOptions = ["Year/Month/Day", "Year/Month", "Year"];
  const [form] = Form.useForm();
  const agreementAccepted = Form.useWatch("agreement", form);

  const { fileList, fileName, onFileChange, beforeUpload } = useFileUpload();
  const { labelsFileList, onLabelsFileChange, beforeLabelsUpload } = useLabelsFileUpload();

  const { session } = useAuth();
  const [pickerType, setPickerType] = useState(pickerTypeOptions[0]);
  const [isUploading, setIsUploading] = useState(false);
  const abortControllerRef = useRef<AbortController | null>(null);

  const { authors } = useData();
  // console.log("authors in upload modal", authors);
  const {
    showUploadingNotification,
    updateUploadProgress,
    showSuccessNotification,
    showErrorNotification,
    closeNotification,
  } = useUploadNotification(uploadKey, fileName);

  const [uploadValidationError, setUploadValidationError] = useState<string | null>(null);
  const { track } = useAnalytics("profile");

  const { canUpload: canUploadPrivate } = useCanUploadPrivate();

  const handleBeforeUpload = async (file: RcFile) => {
    try {
      const uploadType = detectUploadType(file.name);
      validateFileSize(file, uploadType);
      if (uploadType === UploadType.RAW_IMAGES_ZIP) {
        await validateZipCompressionMethods(file);
      } else {
        await validateGeoTiffAiEligibility(file);
      }

      setUploadValidationError(null);
      return beforeUpload(file, []);
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "File validation failed. Please choose a different file.";
      setUploadValidationError(errorMessage);
      message.error("File could not be added. See the details below the file picker.");
      return Upload.LIST_IGNORE;
    }
  };

  const uploadOrthophoto = async (file: RcFile, metadata: UploadMetadata): Promise<UploadResponse> => {
    return new Promise((resolve, reject) => {
      // Create a new AbortController for this upload
      abortControllerRef.current = new AbortController();

      uploadOrtho({
        uploadId: uploadKey,
        file,
        session,
        metadata,
        signal: abortControllerRef.current.signal,
        onSuccess: (response) => {
          resolve(response);
        },
        onError: (error) => {
          reject(error);
        },
        onProgress: (event) => {
          const percent = Math.round(event.percent);
          updateUploadProgress(percent, cancelUpload);
        },
      });
    });
  };

  const getValidAccessToken = async (): Promise<string> => {
    if (isTokenExpiringSoon(session)) {
      const { data, error } = await supabase.auth.refreshSession();
      if (error) {
        if (isInvalidSessionError(error)) {
          await clearLocalSupabaseSession();
          throw new Error("Session expired. Please sign in again.");
        }

        throw error;
      }
      return data.session!.access_token;
    }
    return session!.access_token;
  };

  const processDataset = async (datasetId: number, token: string, processingSteps: string[]) => {
    await addProcess(datasetId, processingSteps, token);
  };

  const cancelUpload = () => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      abortControllerRef.current = null;
      closeNotification();
      setIsUploading(false);
    }
  };

  const handleUpload = async (values: IFormValues) => {
    setIsUploading(true);
    showUploadingNotification(cancelUpload);

    try {
      const uploadFile = fileList[0];
      if (!uploadFile?.originFileObj) {
        throw new Error("No file selected for upload.");
      }

      // Detect file type and validate file size
      const uploadType = detectUploadType(uploadFile.name);
      const hasLabelsFile = labelsFileList.length > 0;
      track("upload_started", {
        upload_type: uploadType,
        has_labels_file: hasLabelsFile,
      });
      validateFileSize(uploadFile.originFileObj, uploadType);
      if (uploadType === UploadType.GEOTIFF) {
        await validateGeoTiffAiEligibility(uploadFile.originFileObj);
      }

      // console.log("values.author", values.author);
      // Create metadata object
      const metadata: UploadMetadata = {
        license: values.license,
        platform: String(values.platform),
        authors: values.author,
        upload_type: uploadType,
        project_id: undefined,
        aquisition_year: values.aquisition_date?.year(),
        aquisition_month: values.aquisition_date ? values.aquisition_date.month() + 1 : undefined,
        aquisition_day: values.aquisition_date?.date(),
        additional_information: values.additional_information,
        data_access: values.is_private ? "private" : "public",
        citation_doi: values.doi,
      };
      // console.log("metadata", metadata);

      // Upload orthophoto with metadata
      const uploadResponse = await uploadOrthophoto(uploadFile.originFileObj, metadata);
      if (!uploadResponse.id) {
        throw new Error("Upload failed to return an ID.");
      }

      // Get valid access token
      const validAccessToken = await getValidAccessToken();

      // Upload labels if present
      if (labelsFileList.length > 0) {
          const labelFile = labelsFileList[0]?.originFileObj;
        if (labelFile) {
          const labelFormData = createLabelObjectFormData(
            uploadResponse.id.toString(),
            session!.user.id,
            labelFile,
            values.labels_description,
          );
          await uploadLabelObject(labelFormData, validAccessToken);
        }
      }

      // Process dataset with appropriate steps based on upload type
      const processingSteps =
        uploadType === UploadType.RAW_IMAGES_ZIP
          ? RAW_IMAGES_PROCESSING_STEPS
          : GEOTIFF_PROCESSING_STEPS;

      await processDataset(Number(uploadResponse.id), validAccessToken, processingSteps);
      track("upload_completed", {
        dataset_id: Number(uploadResponse.id),
        upload_type: uploadType,
        has_labels_file: hasLabelsFile,
      });

      showSuccessNotification();
    } catch (error) {
      // Check if the error was caused by user abortion
      if (error instanceof DOMException && error.name === "AbortError") {
        // console.log("Upload was cancelled by the user");
      } else {
        console.error("Upload error:", error);
        track("upload_failed", {
          upload_type:
            fileList[0]?.name ? detectUploadType(fileList[0].name) : undefined,
          failure_reason: error instanceof Error ? error.message : "unknown_error",
        });
        showErrorNotification();
      }
    } finally {
      setIsUploading(false);
    }
  };

  const onFormFinish = (values: IFormValues) => {
    onClose(); // Close the modal immediately
    handleUpload(values); // Start the upload process
  };

  const handleModalClose = () => {
    if (isUploading) {
      cancelUpload();
    }
    setUploadValidationError(null);
    onClose();
  };

  return (
    <Modal
      open={isVisible}
      centered
      onCancel={handleModalClose}
      footer={null}
      style={{ padding: 0, margin: 0 }}
      maskClosable={false}
      width={720}
    >
      <div className="m-0 px-4 py-0" data-testid="contributor-upload-modal">
        <Typography.Title style={{ margin: 0, paddingBottom: 16 }} level={4}>
          Orthophoto Upload
        </Typography.Title>
        <Form
          layout="vertical"
          onFinish={onFormFinish}
          initialValues={{ platform: "drone", agreement: false, is_private: false }}
          variant="filled"
          form={form}
        >
          <div className="flex flex-col w-full gap-1">
            <Form.Item label="Orthophoto" rules={[{ required: true, message: "Please upload a GeoTIFF file" }]}>
              <Upload.Dragger
                data-testid="contributor-upload-dropzone"
                fileList={fileList}
                onChange={onFileChange}
                beforeUpload={handleBeforeUpload}
                accept=".tif,.tiff,.zip"
                maxCount={1}
                className="w-full"
              >
                <div className="flex">
                  <p className="ant-upload-drag-icon px-8">
                    <InboxOutlined />
                  </p>
                  <div className="text-start">
                    <p className="ant-upload-text mb-0">Click or drag file to this area</p>
                    <p className="ant-upload-hint mb-0 text-xs">
                      GeoTIFF (.tif, .tiff) max 20GB or ZIP with raw drone images (.zip) max 30GB, orthomosaic
                      processing. For raw images, see the{" "}
                      <Link
                        to="/releases/drone-mapping-guide"
                        onClick={(event) => event.stopPropagation()}
                      >
                        drone mapping guide
                      </Link>
                      .
                    </p>
                  </div>
                </div>
              </Upload.Dragger>
            </Form.Item>
            {uploadValidationError ? (
              <Alert
                type="error"
                showIcon
                message="File could not be added"
                description={uploadValidationError}
                className="mb-3"
              />
            ) : null}

            <Form.Item
              rules={[
                { required: true, message: "Please enter the authors" },
                {
                  validator: (_, value) => {
                    if (!value) return Promise.resolve();
                    const hasAndPattern = value.some(
                      (author: string) =>
                        author.toLowerCase().includes(" and ") || author.toLowerCase().includes(","),
                    );
                    if (hasAndPattern) {
                      return Promise.reject(
                        'Please add authors individually instead of using "and" or ",". Use separate entries for each author.',
                      );
                    }
                    return Promise.resolve();
                  },
                },
              ]}
              label={
                <div>
                  <Tooltip title="Provide the names of individuals or organizations responsible for capturing the orthophoto. Add authors individually - one entry per author.">
                    <InfoCircleOutlined className="mr-2" />
                  </Tooltip>
                  Authors of the Orthophoto
                </div>
              }
              name="author"
            >
              {authors?.at(0)?.label ? (
                <Select
                  data-testid="contributor-upload-author-select"
                  mode="tags"
                  style={{ width: "100%" }}
                  options={authors}
                  placeholder="Enter author names separately (e.g. 'John Smith')"
                />
              ) : (
                <Select
                  data-testid="contributor-upload-author-select"
                  mode="tags"
                  style={{ width: "100%" }}
                  placeholder="Enter authors (one author per entry)"
                />
              )}
            </Form.Item>

            <Form.Item
              className="mb-1 w-full"
              label={
                <div>
                  <Tooltip title="Specify the acquisition date of the orthophoto. If you're unsure of the exact date, you can provide a broader timeframe (e.g., month or year).">
                    <InfoCircleOutlined className="mr-2" />
                  </Tooltip>
                  Acquisition Date of the Orthophoto
                </div>
              }
              name="aquisition_date"
              rules={[{ required: true, message: "Please select a date" }]}
              extra="If you're unsure of the exact date, provide a broader timeframe (e.g., month or year)."
            >
              <PickerWithType
                pickerTypeOptions={pickerTypeOptions}
                pickerType={pickerType}
                setPickerType={setPickerType}
                onChange={(date) => form.setFieldsValue({ aquisition_date: date })}
                datePickerTestId="contributor-upload-acquisition-date"
                pickerTypeTestId="contributor-upload-acquisition-date-type"
              />
            </Form.Item>

            <Form.Item
              label={
                <div>
                  <Tooltip title="Select the platform used for capturing the orthophoto (Drone or Airborne). Satellite imagery is not supported in this upload.">
                    <InfoCircleOutlined className="mr-2" />
                  </Tooltip>
                  Platform
                </div>
              }
              name="platform"
            >
              <Radio.Group>
                <Radio value="drone">Drone</Radio>
                <Radio value="airborne">Airborne</Radio>
              </Radio.Group>
            </Form.Item>

            <Form.Item
              label="DOI"
              name="doi"
              rules={[
                {
                  type: "url",
                  message: "Please enter a valid URL",
                },
              ]}
            >
              <Input placeholder="Enter DOI, URL, or publication reference (if applicable)" />
            </Form.Item>

            <Form.Item label="Additional Information" name="additional_information">
              <Input.TextArea
                placeholder="Enter project or data information (e.g., project name, data collection context, processing details)"
                autoSize={{ minRows: 2, maxRows: 6 }}
              />
            </Form.Item>

            <Collapse
              ghost
              className="mb-4 rounded-xl border border-gray-200 bg-gray-50/50"
              items={[
                {
                  key: "labels",
                  label: <span className="font-medium text-gray-700">Add Labels / Annotations (Optional)</span>,
                  children: (
                    <div className="pt-2">
                      <Form.Item
                        label="Labels File"
                        name="labels_file"
                        rules={[
                          {
                            required: false,
                            message: "Please upload a labels file in GeoJSON, Shapefile as zip, or GeoPackage format",
                          },
                        ]}
                      >
                        <Upload.Dragger
                          fileList={labelsFileList}
                          onChange={onLabelsFileChange}
                          beforeUpload={beforeLabelsUpload}
                          accept=".geojson,.json,.zip,.gpkg"
                          maxCount={1}
                          className="w-full"
                          style={{ backgroundColor: "white" }}
                        >
                          <div className="flex">
                            <p className="ant-upload-drag-icon px-8 text-center">
                              <InboxOutlined />
                            </p>
                            <div className="text-start">
                              <p className="ant-upload-text mb-0">Click or drag labels file to this area</p>
                              <p className="ant-upload-hint mb-0 text-xs">
                                Upload deadwood cover labels as GeoJSON, Shapefile (zip) or GeoPackage
                              </p>
                            </div>
                          </div>
                        </Upload.Dragger>
                      </Form.Item>
                      <Form.Item
                        label="Labels Description (required when uploading labels)"
                        name="labels_description"
                        className="mb-0"
                        rules={[
                          () => ({
                            validator(_, value) {
                              if (labelsFileList.length > 0 && !value) {
                                return Promise.reject("Please provide a description for the uploaded labels");
                              }
                              return Promise.resolve();
                            },
                          }),
                        ]}
                      >
                        <Input.TextArea
                          autoSize={{ minRows: 3, maxRows: 6 }}
                          placeholder="Example: Type - Forest Boundaries, Source - XYZ Survey 2023"
                          variant="outlined"
                        />
                      </Form.Item>
                    </div>
                  ),
                },
              ]}
            />

            {canUploadPrivate && (
              <Form.Item name="is_private" valuePropName="checked">
                <Checkbox className="mt-2">
                  <span className="flex items-center text-sm text-gray-700">
                    <LockOutlined className="mr-2" />
                    Upload as private data (only available to you)
                  </span>
                </Checkbox>
              </Form.Item>
            )}

            <Form.Item>
              <div className="space-y-4">
                <Form.Item
                  name="agreement"
                  valuePropName="checked"
                  className="mb-0"
                  rules={[
                    {
                      validator: (_, value) =>
                        value
                          ? Promise.resolve()
                          : Promise.reject("Please accept the terms of service and privacy policy"),
                    },
                  ]}
                >
                  <Checkbox className="mt-1 leading-relaxed">
                    <span className="text-sm text-gray-700">
                      I agree to the <TermsLink /> and <PrivacyLink />. I confirm that I have the rights to share this
                      data and agree to make it available under the CC BY license.
                      <span className="ml-1 font-medium text-red-600">(required)</span>
                    </span>
                  </Checkbox>
                </Form.Item>
                <Space className="pt-2">
                  <Button
                    data-testid="contributor-upload-submit"
                    type="primary"
                    htmlType="submit"
                    disabled={fileList.length === 0 || !agreementAccepted}
                  >
                    Upload
                  </Button>
                  <Button type="default" onClick={onClose}>
                    Cancel
                  </Button>
                </Space>
              </div>
            </Form.Item>
          </div>
        </Form>
      </div>
    </Modal>
  );
};

export default UploadModal;

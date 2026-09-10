import { UploadType } from "../types/dataset";
import { unzipRaw } from "unzipit";
import { fromBlob } from "geotiff";

export const detectUploadType = (fileName: string): UploadType => {
  const ext = fileName.toLowerCase().split(".").pop();
  if (["tif", "tiff"].includes(ext || "")) {
    return UploadType.GEOTIFF;
  } else if (ext === "zip") {
    return UploadType.RAW_IMAGES_ZIP;
  }
  throw new Error(`Unsupported file type: ${ext}`);
};

export const validateFileSize = (file: File, uploadType: UploadType): boolean => {
  const MAX_ZIP_SIZE = 30 * 1024 * 1024 * 1024; // 30GB
  const MAX_GEOTIFF_SIZE = 20 * 1024 * 1024 * 1024; // 20GB

  if (uploadType === UploadType.RAW_IMAGES_ZIP && file.size > MAX_ZIP_SIZE) {
    throw new Error("ZIP files must be smaller than 30GB");
  }
  if (uploadType === UploadType.GEOTIFF && file.size > MAX_GEOTIFF_SIZE) {
    throw new Error("GeoTIFF files must be smaller than 20GB");
  }
  return true;
};

const RGB_PHOTOMETRIC_INTERPRETATION = 2;
const YCBCR_PHOTOMETRIC_INTERPRETATION = 6;

const getNumericTagValue = (value: unknown): number | null => {
  if (typeof value === "number") {
    return value;
  }

  if (ArrayBuffer.isView(value) && value.byteLength > 0 && "0" in value) {
    return Number(value[0]);
  }

  return null;
};

const describeColorModel = (samplesPerPixel: number, photometricInterpretation: number | null): string => {
  if (photometricInterpretation === RGB_PHOTOMETRIC_INTERPRETATION && samplesPerPixel === 3) {
    return "RGB";
  }

  if (photometricInterpretation === RGB_PHOTOMETRIC_INTERPRETATION && samplesPerPixel >= 4) {
    return "RGBA";
  }

  if (photometricInterpretation === YCBCR_PHOTOMETRIC_INTERPRETATION && samplesPerPixel >= 3) {
    return "RGB-compatible YCbCr";
  }

  if (photometricInterpretation === 1 && samplesPerPixel === 1) {
    return "single-band grayscale";
  }

  if (photometricInterpretation === 1 && samplesPerPixel === 2) {
    return "single-band imagery with alpha";
  }

  return `${samplesPerPixel}-sample imagery`;
};

const getBandDescriptions = (image: {
  getGDALMetadata?: (sample?: number) => Record<string, string> | null;
}, samplesPerPixel: number): string[] => {
  const descriptions: string[] = [];

  if (!image.getGDALMetadata) {
    return descriptions;
  }

  for (let sampleIndex = 0; sampleIndex < samplesPerPixel; sampleIndex += 1) {
    const metadata = image.getGDALMetadata(sampleIndex);
    const description = metadata?.DESCRIPTION ?? metadata?.COLORINTERP;
    if (description) {
      descriptions.push(description.trim().toLowerCase());
    }
  }

  return descriptions;
};

export interface GeoTiffAiEligibility {
  supportsAiSegmentation: boolean;
  samplesPerPixel: number;
  photometricInterpretation: number | null;
  colorModel: string;
}

export const inspectGeoTiffAiEligibility = async (file: Blob): Promise<GeoTiffAiEligibility> => {
  try {
    // This inspects TIFF header/IFD metadata only. It does not read raster pixels into memory.
    const tiff = await fromBlob(file);
    const image = await tiff.getImage();
    const samplesPerPixel = image.getSamplesPerPixel();
    const photometricInterpretation = getNumericTagValue(image.fileDirectory.PhotometricInterpretation);
    const bandDescriptions = getBandDescriptions(image, samplesPerPixel);
    const hasRgbBandDescriptions =
      bandDescriptions[0] === "red" && bandDescriptions[1] === "green" && bandDescriptions[2] === "blue";
    const supportsAiSegmentation =
      samplesPerPixel >= 3 &&
      (
        photometricInterpretation === RGB_PHOTOMETRIC_INTERPRETATION ||
        photometricInterpretation === YCBCR_PHOTOMETRIC_INTERPRETATION ||
        hasRgbBandDescriptions
      );

    return {
      supportsAiSegmentation,
      samplesPerPixel,
      photometricInterpretation,
      colorModel: describeColorModel(samplesPerPixel, photometricInterpretation),
    };
  } catch {
    throw new Error("We could not inspect this GeoTIFF. Please choose a valid GeoTIFF file.");
  }
};

export const validateGeoTiffAiEligibility = async (file: Blob): Promise<GeoTiffAiEligibility> => {
  const inspection = await inspectGeoTiffAiEligibility(file);

  if (!inspection.supportsAiSegmentation) {
    throw new Error(
      `We cannot process this dataset yet. Deadwood cover and tree cover currently require an RGB orthomosaic, but this file looks like ${inspection.colorModel}.`,
    );
  }

  return inspection;
};

const ALLOWED_ZIP_METHODS = new Set([0, 8]); // stored, deflate
const ALLOWED_ZIP_METHODS_TEXT = "stored (method 0), deflate (method 8)";

type ZipEntryWithCompressionMethod = {
  compressionMethod?: number;
};

const methodName = (method: number): string => {
  switch (method) {
    case 0:
      return "stored";
    case 8:
      return "deflate";
    case 9:
      return "deflate64";
    case 12:
      return "bzip2";
    case 14:
      return "lzma";
    default:
      return `method-${method}`;
  }
};

const zipInspectionErrorMessage = (error: unknown): string => {
  // Keep file paths and arbitrary exception text out of user-facing diagnostics.
  const detail = error instanceof Error ? error.message : "";
  const name = error instanceof Error ? error.name : "";
  const rebuild =
    "Create a new single ZIP from the original files using stored or deflate compression. " +
    "For archives over 4 GB, use a ZIP64-capable archiver. ZIP uploads up to 30 GB are supported.";

  if (["NotReadableError", "SecurityError", "NotAllowedError", "AbortError"].includes(name)) {
    return "Your browser could not read the selected ZIP. Save or download a complete local copy, " +
      "check that it is accessible, then select it again. No upload has started. Diagnostic: ZIP_FILE_READ.";
  }
  if (/^multi-volume zip files are not supported\./.test(detail)) {
    return "This is part of a split ZIP archive, which is not supported. Create a single ZIP from " +
      "the original files instead of uploading individual parts. No upload has started. Diagnostic: ZIP_MULTIVOLUME.";
  }
  if (detail === "strong encryption is not supported") {
    return "This ZIP uses unsupported encryption. Create a new ZIP without a password using stored " +
      "or deflate compression. No upload has started. Diagnostic: ZIP_ENCRYPTED.";
  }
  if (/^(invalid zip64 |expected zip64 |zip64 extended information extra field)/.test(detail)) {
    return "We could not read this archive's ZIP64 metadata. It may be incomplete or incorrectly written. " +
      `${rebuild} No upload has started. Diagnostic: ZIP64_INVALID.`;
  }
  if (detail === "could not find end of central directory. maybe not zip file") {
    return "We could not find the ZIP file index. The file may be incomplete or may not be a ZIP archive. " +
      "Make sure any download or copy has finished. " +
      `${rebuild} No upload has started. Diagnostic: ZIP_INDEX_MISSING.`;
  }
  if (/^(invalid central directory file header signature:|invalid comment length\.|extra field length exceeds|compressed size mismatch for stored file:)/.test(detail)) {
    return "We could not read the ZIP file index because its metadata is inconsistent. " +
      "The archive may be incomplete or incorrectly written; we cannot determine the exact cause here. " +
      `${rebuild} No upload has started. Diagnostic: ZIP_INDEX_INVALID.`;
  }
  return "We could not inspect this ZIP in your browser. The exact cause is unknown. " +
    "Try selecting a complete local copy or recreating the ZIP. If it still fails, contact " +
    "info@deadtrees.earth with your browser, file size, ZIP creation tool and this diagnostic. " +
    "No upload has started. Diagnostic: ZIP_INSPECTION_FAILED.";
};

export const validateZipCompressionMethods = async (file: File): Promise<void> => {
  let entries: ZipEntryWithCompressionMethod[] = [];
  try {
    const zipInfo = await unzipRaw(file);
    entries = zipInfo.entries as ZipEntryWithCompressionMethod[];
  } catch (error) {
    throw Object.assign(new Error(zipInspectionErrorMessage(error)), { cause: error });
  }

  const methodCounts = new Map<number, number>();

  for (const entry of entries) {
    const compressionMethod = entry.compressionMethod;
    if (typeof compressionMethod !== "number") {
      throw new Error("Unable to inspect ZIP compression methods");
    }
    methodCounts.set(compressionMethod, (methodCounts.get(compressionMethod) ?? 0) + 1);
  }

  const unsupported = [...methodCounts.entries()].filter(([method]) => !ALLOWED_ZIP_METHODS.has(method));
  if (unsupported.length > 0) {
    const formatted = unsupported
      .map(([method, count]) => `${methodName(method)} (method ${method}, ${count} file(s))`)
      .join(", ");
    throw new Error(
      `Unsupported ZIP compression method(s): ${formatted}. Please re-compress the ZIP using one of: ${ALLOWED_ZIP_METHODS_TEXT}.`,
    );
  }
};

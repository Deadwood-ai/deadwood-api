import path from "node:path";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { expect, test, type Page, type Route } from "@playwright/test";
import { installLocalSession } from "./support/localAuth";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rgbGeoTiffFixture = path.resolve(
  __dirname,
  "../test/fixtures/geotiff/upload-validation/rgb-real-crop.tif",
);

const localSupabaseUrl =
  process.env.VITE_SUPABASE_URL ||
  process.env.SUPABASE_URL ||
  "http://127.0.0.1:54321";
const localApiUrl =
  process.env.VITE_LOCAL_API_URL || "http://localhost:8080/api/v1";
const contributor = {
  id: "00000000-0000-4000-8000-000000000001",
  email: "contributor-local-e2e@example.com",
};

const installAuthenticatedContributor = async (page: Page) => {
  await installLocalSession(page, {
    user: contributor,
    supabaseUrl: localSupabaseUrl,
    refreshToken: "local-e2e-refresh-token",
  });

  await page.route(`${localSupabaseUrl}/rest/v1/**`, async (route) => {
    const url = new URL(route.request().url());
    const table = url.pathname.split("/").pop();

    if (table === "privileged_users") {
      await route.fulfill({
        contentType: "application/json",
        json: {
          id: 1,
          user_id: contributor.id,
          can_upload_private: false,
          can_audit: false,
          can_view_all_private: false,
          created_at: new Date().toISOString(),
        },
      });
      return;
    }

    await route.fulfill({
      contentType: "application/json",
      headers: { "content-range": "0-0/0" },
      json: [],
    });
  });
};

const extractMultipartText = (route: Route) =>
  route.request().postDataBuffer()?.toString("latin1") ?? "";

test.describe("contributor local e2e", () => {
  test("authenticated contributor can open the upload workflow", async ({
    page,
  }) => {
    await installAuthenticatedContributor(page);

    await page.goto("/profile");

    await expect(
      page.getByRole("heading", { name: "My Account" }),
    ).toBeVisible();
    await expect(page.getByText(contributor.email)).toBeVisible();

    await page.getByRole("button", { name: "Upload Data" }).click();

    const modal = page.getByTestId("contributor-upload-modal");
    await expect(modal).toBeVisible();
    await expect(modal.getByText("Orthophoto Upload")).toBeVisible();
    await expect(
      page.getByTestId("contributor-upload-dropzone"),
    ).toBeAttached();
    await expect(
      page.getByText("Click or drag file to this area"),
    ).toBeVisible();
    await expect(
      page.getByTestId("contributor-upload-author-select"),
    ).toBeVisible();
    await expect(page.getByTestId("contributor-upload-submit")).toBeDisabled();
  });

  test("ZIP validation explains a broken index before upload and clears after a valid selection", async ({ page }) => {
    await installAuthenticatedContributor(page);
    let uploadRequests = 0;
    await page.route(`${localApiUrl}/datasets/chunk`, async (route) => {
      uploadRequests += 1;
      await route.abort();
    });
    await page.goto("/profile");
    await page.getByRole("button", { name: "Upload Data" }).click();
    const modal = page.getByTestId("contributor-upload-modal");
    const input = page.getByTestId("contributor-upload-dropzone");
    const bytes = await readFile(path.resolve(__dirname, "../test/fixtures/zip/mixed-stored.zip"));
    // Point the ZIP index at the local file header, reproducing an invalid directory offset.
    bytes.writeUInt32LE(0, bytes.length - 6);
    await input.setInputFiles({ name: "broken-index.zip", mimeType: "application/zip", buffer: bytes });
    const error = modal.getByRole("alert");
    await expect(error).toContainText("File could not be added");
    await expect(error).toContainText("ZIP_INDEX_INVALID");
    await expect(error).toContainText("ZIP64-capable archiver");
    await expect(error).toContainText("No upload has started");
    await expect(modal.getByTestId("contributor-upload-submit")).toBeDisabled();
    await expect(modal.locator(".ant-upload-list-item")).toHaveCount(0);

    await input.setInputFiles(path.resolve(__dirname, "../test/fixtures/zip/mixed-zip64.zip"));
    await expect(error).toHaveCount(0);
    await expect(modal.locator(".ant-upload-list-item")).toContainText("mixed-zip64.zip");
    expect(uploadRequests).toBe(0);
  });

  test("GeoTIFF contribution sends upload and processing contracts", async ({
    page,
  }) => {
    await installAuthenticatedContributor(page);

    let uploadMultipartBody = "";
    let processRequestBody: Record<string, unknown> | undefined;

    await page.route(`${localApiUrl}/datasets/chunk`, async (route) => {
      uploadMultipartBody = extractMultipartText(route);
      await route.fulfill({
        contentType: "application/json",
        json: {
          id: 4242,
          file_name: "rgb-real-crop.tif",
          user_id: contributor.id,
          license: "CC BY",
          platform: "drone",
          authors: ["Local E2E Contributor"],
          data_access: "public",
        },
      });
    });

    await page.route(`${localApiUrl}/datasets/4242/process`, async (route) => {
      processRequestBody = route.request().postDataJSON();
      await route.fulfill({
        contentType: "application/json",
        json: {
          id: 99,
          dataset_id: 4242,
          task_types: processRequestBody?.task_types,
          priority: processRequestBody?.priority,
          is_processing: false,
        },
      });
    });

    await page.goto("/profile");
    await page.getByRole("button", { name: "Upload Data" }).click();

    await page
      .getByTestId("contributor-upload-dropzone")
      .setInputFiles(rgbGeoTiffFixture);

    await addUploadAuthor(page, "Local E2E Contributor");

    const dateInput = page.getByPlaceholder("Select date");
    await dateInput.fill("2024-05-06");
    await dateInput.press("Enter");

    await page
      .getByLabel("DOI")
      .fill("https://doi.org/10.1234/deadtrees.local-e2e");
    await page
      .getByLabel("Additional Information")
      .fill("Local contributor smoke metadata");
    await page.getByRole("checkbox", { name: /I agree to the/i }).check();

    await expect(page.getByTestId("contributor-upload-submit")).toBeEnabled();
    await page.getByTestId("contributor-upload-submit").click();

    await expect
      .poll(() => uploadMultipartBody)
      .toContain('name="upload_type"');
    expect(uploadMultipartBody).toContain("\r\n\r\ngeotiff\r\n");
    expect(uploadMultipartBody).toContain('name="platform"');
    expect(uploadMultipartBody).toContain("\r\n\r\ndrone\r\n");
    expect(uploadMultipartBody).toContain('name="authors"');
    expect(uploadMultipartBody).toContain("\r\n\r\nLocal E2E Contributor\r\n");
    expect(uploadMultipartBody).toContain('name="aquisition_year"');
    expect(uploadMultipartBody).toContain("\r\n\r\n2024\r\n");
    expect(uploadMultipartBody).toContain('name="aquisition_month"');
    expect(uploadMultipartBody).toContain("\r\n\r\n5\r\n");
    expect(uploadMultipartBody).toContain('name="aquisition_day"');
    expect(uploadMultipartBody).toContain("\r\n\r\n6\r\n");
    expect(uploadMultipartBody).toContain('name="data_access"');
    expect(uploadMultipartBody).toContain("\r\n\r\npublic\r\n");
    expect(uploadMultipartBody).toContain('name="citation_doi"');
    expect(uploadMultipartBody).toContain(
      "https://doi.org/10.1234/deadtrees.local-e2e",
    );

    await expect
      .poll(() => processRequestBody)
      .toEqual({
        task_types: [
          "geotiff",
          "cog",
          "thumbnail",
          "metadata",
          "aoi_v1",
          "deadwood_v1",
          "treecover_v1",
          "deadwood_treecover_combined_v2",
          "embeddings_v1",
        ],
        priority: 4,
      });
  });
});

async function addUploadAuthor(page: Page, author: string) {
  const authorSelect = page.getByTestId("contributor-upload-author-select");
  await authorSelect.locator("input").fill(author);
  await authorSelect.locator("input").press("Enter");
  await expect(authorSelect).toContainText(author);
}

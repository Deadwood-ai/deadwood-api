import { MailOutlined } from "@ant-design/icons";
import { Switch, Typography } from "antd";

import { useProcessingEmailPreference } from "../hooks/useProcessingEmailPreference";

interface ProcessingEmailPreferenceProps {
  userId?: string;
}

export default function ProcessingEmailPreference({
  userId,
}: ProcessingEmailPreferenceProps) {
  const { enabled, error, isLoading, isSaving, setEnabled } =
    useProcessingEmailPreference(userId);

  return (
    <section
      aria-labelledby="processing-email-preference-title"
      className="mb-4 flex items-center justify-between gap-4 rounded-lg border border-gray-200 bg-white px-4 py-3"
    >
      <div className="flex min-w-0 items-start gap-3">
        <MailOutlined className="mt-0.5 text-base text-gray-600" aria-hidden />
        <div className="min-w-0">
          <Typography.Text
            id="processing-email-preference-title"
            className="block text-sm font-semibold text-gray-900"
          >
            Processing emails
          </Typography.Text>
          <Typography.Text type={error ? "danger" : "secondary"} className="text-xs">
            {error
              ? "Your preference could not be loaded or saved."
              : "Email me when dataset processing completes or fails."}
          </Typography.Text>
        </div>
      </div>
      <Switch
        aria-label="Processing emails"
        checked={enabled}
        disabled={!userId || !!error}
        loading={isLoading || isSaving}
        onChange={(checked) => setEnabled(checked)}
      />
    </section>
  );
}

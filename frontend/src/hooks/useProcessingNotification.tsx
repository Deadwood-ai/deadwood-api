import { notification, Button } from "antd";
import { CheckCircleOutlined, UserOutlined } from "@ant-design/icons";
import { useNavigate, useLocation } from "react-router-dom";
import { palette } from "../theme/palette";

export const useProcessingNotification = () => {
  const navigate = useNavigate();
  const location = useLocation();

  const showProcessingCompleteNotification = (datasetName: string, datasetId: number) => {
    const key = `processing-complete-${datasetId}`;
    const isOnProfilePage = location.pathname === "/profile";

    notification.success({
      key,
      message: "Dataset Processing Complete!",
      description: (
        <div>
          <div className={isOnProfilePage ? "mb-0" : "mb-3"}>
            <strong>{datasetName}</strong> has been successfully processed.
          </div>
          {!isOnProfilePage && (
            <Button
              type="primary"
              size="small"
              icon={<UserOutlined />}
              onClick={() => {
                navigate("/profile");
                notification.destroy(key);
              }}
            >
              View in Account
            </Button>
          )}
        </div>
      ),
      icon: <CheckCircleOutlined style={{ color: palette.state.success }} />,
      duration: 10, // Auto-dismiss after 10 seconds
      placement: "topRight",
    });
  };

  const showProcessingErrorNotification = (datasetName: string, datasetId: number) => {
    notification.error({
      message: "Processing Failed",
      description: (
        <div>
          <div className="mb-2">
            <strong>{datasetName}</strong> failed to process.
          </div>
          <p className="text-sm text-gray-600">The team has recorded the failure.</p>
          <Button size="small" onClick={() => navigate(`/profile?dataset=${datasetId}`)}>View dataset status</Button>
        </div>
      ),
      duration: 8,
      placement: "topRight",
    });
  };

  return {
    showProcessingCompleteNotification,
    showProcessingErrorNotification,
  };
};

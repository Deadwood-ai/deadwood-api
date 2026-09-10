import { MenuOutlined } from "@ant-design/icons";
import { Button, ConfigProvider, Drawer, Layout, Menu } from "antd";
import { useState } from "react";
import { useLocation } from "react-router-dom";
const { Header } = Layout;

import { useAuth } from "../hooks/useAuthProvider";
import { usePriwaProjectMemberships } from "../hooks/usePriwaProjectMemberships";
import { useCanAudit } from "../hooks/useUserPrivileges";
import { useNavigate } from "react-router-dom";
import { palette } from "../theme/palette";

const defaultNavigation = [
  {
    key: "/home",
    label: "Home",
  },
  {
    key: "/deadtrees",
    label: "Satellite Map",
  },
  {
    key: "/dataset",
    label: "Drone Archive",
  },
  {
    key: "/releases",
    label: "Releases",
  },
  {
    key: "/about",
    label: "About",
  },
  {
    key: "/profile",
    label: "My Account",
  },
];

// Additional navigation item for users who can audit
const auditNavigation = {
  key: "/dataset-audit",
  label: "Audit Datasets",
};

export default function Navigation() {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const { session, signOut } = useAuth();
  const { canAudit } = useCanAudit();
  const { data: priwaMemberships = [] } = usePriwaProjectMemberships();
  const nav = useNavigate();
  const location = useLocation();

  // Get the base path for matching navigation
  const currentPath = "/" + location.pathname.split("/")[1];

  // Check if currently in audit detail page
  const isInAuditDetail = location.pathname.match(/^\/dataset-audit\/\d+$/);

  // Add audit navigation if user has audit privileges
  const navigation = [...defaultNavigation];
  if (priwaMemberships.length > 0) {
    navigation.splice(2, 0, {
      key: "/priwa-field",
      label: "PRIWA",
    });
  }
  if (canAudit) {
    // Insert the audit link before the Account link
    navigation.splice(navigation.length - 1, 0, auditNavigation);
  }

  const handleSignOut = async () => {
    if (isInAuditDetail) {
      // Dispatch custom event for audit page to handle
      window.dispatchEvent(
        new CustomEvent("audit-navigation-attempt", {
          detail: { to: "/sign-in", action: "signout" },
        }),
      );
      return;
    }

    await signOut();
    nav("/sign-in");
  };

  const handleLogoClick = (e: React.MouseEvent) => {
    if (isInAuditDetail) {
      e.preventDefault();
      window.dispatchEvent(
        new CustomEvent("audit-navigation-attempt", {
          detail: { to: "/" },
        }),
      );
    } else {
      nav("/");
    }
  };

  const handleMenuClick = (e: { key: string }) => {
    if (isInAuditDetail) {
      // When in audit detail, dispatch custom event instead of navigating
      window.dispatchEvent(
        new CustomEvent("audit-navigation-attempt", {
          detail: { to: e.key === "/home" ? "/" : e.key },
        }),
      );
    } else {
      // Normal navigation - convert menu key to actual path
      const path = e.key === "/home" ? "/" : e.key;
      nav(path);
    }

    setMobileMenuOpen(false);
  };

  const handleAuthClick = () => {
    if (session) {
      handleSignOut();
      setMobileMenuOpen(false);
      return;
    }

    if (isInAuditDetail) {
      window.dispatchEvent(
        new CustomEvent("audit-navigation-attempt", {
          detail: { to: "/sign-in" },
        }),
      );
      return;
    }

    nav("/sign-in");
    setMobileMenuOpen(false);
  };

  return (
    <>
      <div
        className="dt-nav-shell hidden lg:flex justify-center w-full fixed top-0 z-50 pt-4 px-4 pb-2"
      >
        <Header
          className="w-full pointer-events-auto"
          style={{
            display: "flex",
            alignItems: "center",
            background: "rgba(255, 255, 255, 0.90)",
            backdropFilter: "blur(12px)",
            WebkitBackdropFilter: "blur(12px)",
            borderRadius: "1rem",
            border: "1px solid rgba(229, 231, 235, 0.8)",
            boxShadow:
              "0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.03)",
            padding: "0 24px",
            height: "64px",
            lineHeight: "64px",
          }}
        >
          <div className="flex flex-1 items-center justify-center lg:justify-start">
            <img
              src="/assets/logo.png"
              alt="deadtrees.earth"
              onClick={handleLogoClick}
              className="mr-3 h-10 cursor-pointer hover:opacity-80 transition-opacity"
            />
          </div>
          <div style={{ flex: 1, backgroundColor: "transparent" }}>
            <Menu
              mode="horizontal"
              selectedKeys={[currentPath === "/" ? "/home" : currentPath]}
              items={navigation}
              onClick={handleMenuClick}
              style={{
                justifyContent: "end",
                minWidth: 0,
                borderBottom: "none",
                backgroundColor: "transparent",
                width: "100%",
              }}
            />
          </div>
          <Button
            className="ml-8 rounded-full shadow-sm px-6 font-medium"
            type={session ? "default" : "primary"}
            onClick={handleAuthClick}
          >
            {session ? "Sign Out" : "Sign In"}
          </Button>
        </Header>
      </div>

      <div
        className="dt-nav-shell dt-mobile-nav-shell lg:hidden fixed top-0 z-50 w-full px-2 pb-2"
      >
        <Header
          className="pointer-events-auto"
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            background: "rgba(255, 255, 255, 0.94)",
            backdropFilter: "blur(10px)",
            WebkitBackdropFilter: "blur(10px)",
            borderRadius: "0.875rem",
            border: "1px solid rgba(229, 231, 235, 0.8)",
            boxShadow: "0 2px 8px rgba(15, 23, 42, 0.08)",
            padding: "0 12px",
            height: "56px",
            lineHeight: "56px",
          }}
        >
          <img
            src="/assets/logo.png"
            alt="deadtrees.earth"
            onClick={handleLogoClick}
            className="h-8 cursor-pointer hover:opacity-80 transition-opacity"
          />
          <Button
            aria-label="Open navigation menu"
            icon={<MenuOutlined />}
            size="large"
            className="flex h-11 w-11 items-center justify-center rounded-xl"
            style={{ width: 44, minWidth: 44, height: 44 }}
            onClick={() => setMobileMenuOpen(true)}
          />
        </Header>
      </div>

      <Drawer
        title="Navigation"
        placement="right"
        open={mobileMenuOpen}
        width="82vw"
        onClose={() => setMobileMenuOpen(false)}
        rootClassName="dt-mobile-navigation-drawer-root"
        styles={{
          body: { padding: 0, display: "flex", flexDirection: "column" },
        }}
      >
        <div className="flex-1 overflow-y-auto py-2">
          <ConfigProvider
            theme={{
              components: {
                Menu: {
                  itemSelectedBg: palette.primary[50],
                  itemSelectedColor: palette.primary[500],
                  itemHoverBg: palette.neutral[50],
                },
              },
            }}
          >
            <Menu
              mode="inline"
              selectedKeys={[currentPath === "/" ? "/home" : currentPath]}
              items={navigation}
              onClick={handleMenuClick}
              style={{
                borderInlineEnd: "none",
                backgroundColor: "transparent",
              }}
            />
          </ConfigProvider>
        </div>
        <div className="dt-mobile-navigation-drawer-footer p-4 border-t border-gray-100">
          <Button
            className="w-full font-medium"
            type={session ? "default" : "primary"}
            size="large"
            onClick={handleAuthClick}
          >
            {session ? "Sign Out" : "Sign In"}
          </Button>
        </div>
      </Drawer>
    </>
  );
}

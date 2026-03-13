import { createContext, ReactNode, useContext, useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "./api";

type TenantContextValue = {
  tenantId: string;
  tenants: string[];
  setTenantId: (tenantId: string) => void;
};

const TenantContext = createContext<TenantContextValue | null>(null);
const STORAGE_KEY = "fabrik-console:tenant";

function readTenantFromStorage() {
  try {
    const storage = window.localStorage;
    return typeof storage?.getItem === "function" ? storage.getItem(STORAGE_KEY) ?? "" : "";
  } catch {
    return "";
  }
}

function writeTenantToStorage(tenantId: string) {
  try {
    const storage = window.localStorage;
    if (typeof storage?.setItem === "function") {
      storage.setItem(STORAGE_KEY, tenantId);
    }
  } catch {
    // Ignore storage failures in tests and restricted browser contexts.
  }
}

export function TenantProvider({ children }: { children: ReactNode }) {
  const [tenantId, setTenantIdState] = useState(readTenantFromStorage);
  const tenantsQuery = useQuery({
    queryKey: ["tenants"],
    queryFn: api.listTenants
  });

  useEffect(() => {
    const tenants = tenantsQuery.data?.tenants ?? [];
    if (!tenantId && tenants.length > 0) {
      setTenantIdState(tenants[0]);
    }
  }, [tenantId, tenantsQuery.data?.tenants]);

  const value = useMemo<TenantContextValue>(
    () => ({
      tenantId,
      tenants: tenantsQuery.data?.tenants ?? [],
      setTenantId: (next) => {
        setTenantIdState(next);
        writeTenantToStorage(next);
      }
    }),
    [tenantId, tenantsQuery.data?.tenants]
  );

  return <TenantContext.Provider value={value}>{children}</TenantContext.Provider>;
}

export function useTenant() {
  const value = useContext(TenantContext);
  if (!value) {
    throw new Error("Tenant context missing");
  }
  return value;
}

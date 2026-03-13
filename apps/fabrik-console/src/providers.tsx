import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ReactNode, useState } from "react";
import { Toaster } from "sonner";

import { TenantProvider } from "./lib/tenant-context";

export function AppProviders({ children }: { children: ReactNode }) {
  const [client] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            staleTime: 10_000,
            refetchOnWindowFocus: false
          }
        }
      })
  );

  return (
    <QueryClientProvider client={client}>
      <TenantProvider>
        {children}
        <Toaster richColors position="top-right" />
      </TenantProvider>
    </QueryClientProvider>
  );
}

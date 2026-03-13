import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { vi } from "vitest";

import { AppProviders } from "../providers";
import { GlobalSearch } from "./global-search";

vi.stubGlobal(
  "fetch",
  vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/admin/tenants")) {
      return new Response(JSON.stringify({ tenants: ["tenant-a"] }), { status: 200 });
    }
    if (url.includes("/search")) {
      return new Response(
        JSON.stringify({
          tenant_id: "tenant-a",
          items: [{ kind: "run", id: "wf-1:run-1", title: "payment-workflow", subtitle: "running", href: "/runs/wf-1/run-1", tenant_id: "tenant-a" }]
        }),
        { status: 200 }
      );
    }
    return new Response(JSON.stringify({}), { status: 200 });
  })
);

test("shows remote search results", async () => {
  render(
    <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <AppProviders>
        <GlobalSearch open onClose={() => undefined} />
      </AppProviders>
    </MemoryRouter>
  );

  await waitFor(() => expect(screen.getByPlaceholderText(/Search runs/i)).toBeInTheDocument());
  const input = screen.getByPlaceholderText(/Search runs/i);
  await userEvent.type(input, "payment");
  await waitFor(() => expect(screen.getByText("payment-workflow")).toBeInTheDocument());
});

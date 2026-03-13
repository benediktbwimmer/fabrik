import { render, screen } from "@testing-library/react";

import { ExecutionGraph } from "./execution-graph";

test("renders empty state without graph data", () => {
  render(<ExecutionGraph />);
  expect(screen.getByText("No execution graph available.")).toBeInTheDocument();
});

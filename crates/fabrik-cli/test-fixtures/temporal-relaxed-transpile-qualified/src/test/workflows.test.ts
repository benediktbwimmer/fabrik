import { missingHelper } from "missing-helper";

describe("relaxed transpile fixture", () => {
  it("keeps typecheck-only junk out of worker packaging", () => {
    expect(missingHelper()).toBe("unused");
  });
});

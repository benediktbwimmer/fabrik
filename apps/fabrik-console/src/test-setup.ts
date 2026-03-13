import "@testing-library/jest-dom/vitest";

Object.defineProperty(window, "localStorage", {
  value: {
    getItem: () => null,
    setItem: () => undefined
  },
  writable: true
});

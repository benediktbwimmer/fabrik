import "@testing-library/jest-dom/vitest";

Object.defineProperty(window, "localStorage", {
  value: {
    getItem: () => null,
    setItem: () => undefined
  },
  writable: true
});

class ResizeObserverMock {
  observe() {}
  unobserve() {}
  disconnect() {}
}

Object.defineProperty(window, "ResizeObserver", {
  value: ResizeObserverMock,
  writable: true
});

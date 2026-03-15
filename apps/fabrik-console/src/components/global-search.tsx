import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";

import { api } from "../lib/api";
import { useTenant } from "../lib/tenant-context";

type Props = {
  open: boolean;
  onClose: () => void;
};

const NAV_ITEMS = [
  { title: "Home", subtitle: "Operator overview", href: "/" },
  { title: "Workflows", subtitle: "Definition catalog", href: "/workflows" },
  { title: "Runs", subtitle: "Operational run index", href: "/runs" },
  { title: "Stream Jobs", subtitle: "Bridge and materialized state inspection", href: "/stream-jobs" },
  { title: "Task Queues", subtitle: "Queue health and pollers", href: "/task-queues" }
];

export function GlobalSearch({ open, onClose }: Props) {
  const { tenantId } = useTenant();
  const navigate = useNavigate();
  const [query, setQuery] = useState("");
  const [activeIndex, setActiveIndex] = useState(0);
  const searchQuery = useQuery({
    queryKey: ["search", tenantId, query],
    enabled: open && tenantId !== "" && query.trim() !== "",
    queryFn: () => api.search(tenantId, query.trim())
  });

  useEffect(() => {
    if (!open) {
      setQuery("");
      setActiveIndex(0);
    }
  }, [open]);

  const items = useMemo(() => {
    if (query.trim() === "") {
      return NAV_ITEMS.map((item) => ({ ...item, kind: "navigate" }));
    }
    return searchQuery.data?.items ?? [];
  }, [query, searchQuery.data?.items]);

  useEffect(() => {
    if (!open) return;
    const onKey = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        onClose();
      }
      if (event.key === "Escape") onClose();
      if (event.key === "ArrowDown") {
        event.preventDefault();
        setActiveIndex((current) => Math.min(current + 1, Math.max(items.length - 1, 0)));
      }
      if (event.key === "ArrowUp") {
        event.preventDefault();
        setActiveIndex((current) => Math.max(current - 1, 0));
      }
      if (event.key === "Enter" && items[activeIndex]) {
        event.preventDefault();
        navigate(items[activeIndex].href);
        onClose();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [activeIndex, items, navigate, onClose, open]);

  if (!open) return null;

  return (
    <div className="search-overlay" onClick={onClose}>
      <div className="search-modal" onClick={(event) => event.stopPropagation()}>
        <input
          autoFocus
          className="input"
          placeholder={tenantId ? "Search runs, definitions, workflow ids, task queues..." : "Choose a tenant first"}
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
        <div className="search-results">
          {items.map((item, index) => (
            <button
              key={`${item.kind}:${"id" in item ? item.id : item.href}`}
              className={`search-item ${activeIndex === index ? "active" : ""}`}
              onClick={() => {
                navigate(item.href);
                onClose();
              }}
            >
              <strong>{item.title}</strong>
              <span className="muted">{item.subtitle}</span>
            </button>
          ))}
          {items.length === 0 ? <div className="empty">No matches.</div> : null}
        </div>
      </div>
    </div>
  );
}

import { Activity, GitBranch, Home, Radar, Radio, RefreshCcw, Search, Workflow } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";

import { useTenant } from "../lib/tenant-context";
import { GlobalSearch } from "./global-search";
import { ConsistencyBadge } from "./ui";

const NAV_ITEMS = [
  { to: "/", label: "Home", icon: Home, chord: "G H" },
  { to: "/workflows", label: "Workflows", icon: Workflow, chord: "G W" },
  { to: "/runs", label: "Runs", icon: Activity, chord: "G R" },
  { to: "/replay", label: "Replay", icon: RefreshCcw, chord: "G P" },
  { to: "/builds", label: "Builds", icon: GitBranch, chord: "G B" },
  { to: "/conformance", label: "Conformance", icon: Search, chord: "G C" },
  { to: "/task-queues", label: "Task Queues", icon: Radar, chord: "G Q" },
  { to: "/topic-adapters", label: "Topic Adapters", icon: Radio, chord: "G T" }
];

export function AppShell() {
  const { tenantId, tenants, setTenantId } = useTenant();
  const location = useLocation();
  const navigate = useNavigate();
  const [searchOpen, setSearchOpen] = useState(false);
  const chordTimeout = useRef<number | null>(null);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      const inInput = target && ["INPUT", "TEXTAREA", "SELECT"].includes(target.tagName);
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setSearchOpen((open) => !open);
        return;
      }
      if (inInput) return;
      if (event.key === "/") {
        event.preventDefault();
        setSearchOpen(true);
        return;
      }
      if (event.key.toLowerCase() === "g") {
        if (chordTimeout.current) {
          window.clearTimeout(chordTimeout.current);
        }
        const follow = (next: KeyboardEvent) => {
          const nav = {
            h: "/",
            w: "/workflows",
            r: "/runs",
            p: "/replay",
            b: "/builds",
            c: "/conformance",
            q: "/task-queues",
            t: "/topic-adapters",
          }[next.key.toLowerCase()];
          if (nav) navigate(nav);
        };
        const handler = (next: KeyboardEvent) => {
          follow(next);
          window.removeEventListener("keydown", handler);
        };
        window.addEventListener("keydown", handler);
        chordTimeout.current = window.setTimeout(() => {
          window.removeEventListener("keydown", handler);
          chordTimeout.current = null;
        }, 1000);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [navigate]);

  const title = useMemo(() => {
    const item = NAV_ITEMS.find((navItem) => location.pathname === navItem.to || location.pathname.startsWith(`${navItem.to}/`));
    return item?.label ?? "Fabrik";
  }, [location.pathname]);

  return (
    <>
      <div className="shell">
        <aside className="sidebar">
          <div className="brand">
            <span className="brand-mark">F</span>
            <div>
              <div>Fabrik</div>
              <div className="muted">operator console</div>
            </div>
          </div>
          <div className="stack" style={{ marginBottom: 20 }}>
            <label className="muted">Tenant</label>
            {tenants.length > 0 ? (
              <select className="select" value={tenantId} onChange={(event) => setTenantId(event.target.value)}>
                {tenants.map((tenant) => (
                  <option key={tenant} value={tenant}>
                    {tenant}
                  </option>
                ))}
              </select>
            ) : (
              <input className="input" value={tenantId} onChange={(event) => setTenantId(event.target.value)} placeholder="tenant id" />
            )}
          </div>
          <nav className="nav">
            {NAV_ITEMS.map((item) => {
              const Icon = item.icon;
              return (
                <NavLink key={item.to} to={item.to} end>
                  <span className="row">
                    <Icon size={16} />
                    {item.label}
                  </span>
                  <span className="nav-hint">{item.chord}</span>
                </NavLink>
              );
            })}
            <button onClick={() => setSearchOpen(true)}>
              <span className="row">
                <Search size={16} />
                Search
              </span>
              <span className="nav-hint">/</span>
            </button>
          </nav>
        </aside>
        <main className="main">
          <div className="topbar">
            <div>
              <div className="eyebrow">Temporal-compatible control plane</div>
              <strong>{title}</strong>
            </div>
            <div className="topbar-controls">
              <ConsistencyBadge consistency="eventual" source="projection" />
              <span className="keycap">⌘K</span>
              <button className="button ghost" onClick={() => setSearchOpen(true)}>
                Open Search
              </button>
            </div>
          </div>
          <Outlet />
        </main>
      </div>
      <GlobalSearch open={searchOpen} onClose={() => setSearchOpen(false)} />
    </>
  );
}

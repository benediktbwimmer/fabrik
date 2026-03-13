import { PropsWithChildren } from "react";

export function Panel({ children, className = "" }: PropsWithChildren<{ className?: string }>) {
  return <section className={`panel ${className}`.trim()}>{children}</section>;
}

export function Badge({ value }: { value: string | null | undefined }) {
  const tone = (value ?? "unknown").toLowerCase();
  return <span className={`badge ${tone}`}>{value ?? "unknown"}</span>;
}

export function ConsistencyBadge({
  consistency,
  source
}: {
  consistency: string | null | undefined;
  source?: string | null | undefined;
}) {
  return (
    <span className={`badge ${consistency ?? "unknown"}`}>
      {consistency ?? "unknown"}
      {source ? <span className="badge-detail">{source}</span> : null}
    </span>
  );
}

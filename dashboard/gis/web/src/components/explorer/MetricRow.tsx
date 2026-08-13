interface MetricRowProps {
  label: string;
  value: string;
}

export function MetricRow({ label, value }: MetricRowProps) {
  return (
    <div className="metric-row">
      <span className="metric-label">{label}</span>

      <span className="metric-value">{value}</span>
    </div>
  );
}

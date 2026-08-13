interface HazardSignalProps {
  label: string;
  value: number | null;
  tone: "climate" | "hydro" | "wildfire";
}

export function HazardSignal({ label, value, tone }: HazardSignalProps) {
  const normalized = value === null ? 0 : Math.max(0, Math.min(1, value));

  return (
    <div className="hazard-signal">
      <div className="hazard-signal-header">
        <span>{label}</span>

        <span className="hazard-signal-value">
          {value === null ? "-" : value.toFixed(2)}
        </span>
      </div>

      <div className="hazard-track">
        <div
          className={`hazard-fill ${tone}`}
          style={{ width: `${normalized * 100}%` }}
        />
      </div>
    </div>
  );
}

import { useEffect, useState } from "react";

interface TimelineControlProps {
  months: string[];
  referenceMonth: string;
  onMonthChange: (month: string) => void;
}

export function formatMonth(month: string): string {
  const date = new Date(`${month}-01T00:00:00Z`);

  return new Intl.DateTimeFormat("en-CA", {
    month: "short",
    year: "numeric",
    timeZone: "UTC",
  }).format(date);
}

export function TimelineControl({
  months,
  referenceMonth,
  onMonthChange,
}: TimelineControlProps) {
  const committedIndex = Math.max(0, months.indexOf(referenceMonth));
  const [draftIndex, setDraftIndex] = useState(committedIndex);

  useEffect(() => {
    setDraftIndex(committedIndex);
  }, [committedIndex]);

  const progress =
    months.length <= 1 ? 0 : (draftIndex / (months.length - 1)) * 100;
  const draftMonth = months[draftIndex] ?? referenceMonth;
  const startYear = months[0]?.slice(0, 4) ?? "";
  const endYear = months[months.length - 1]?.slice(0, 4) ?? "";

  const commit = () => {
    const month = months[draftIndex];

    if (month && month !== referenceMonth) {
      onMonthChange(month);
    }
  };

  return (
    <div className="timeline-shell glass-panel">
      <span className="timeline-year">{startYear}</span>

      <div className="timeline-control">
        <div className="timeline-track">
          <div className="timeline-progress" style={{ width: `${progress}%` }} />

          <div className="timeline-thumb" style={{ left: `${progress}%` }}>
            <span className="timeline-value">{formatMonth(draftMonth)}</span>
          </div>
        </div>

        <input
          className="timeline-range"
          type="range"
          min={0}
          max={Math.max(0, months.length - 1)}
          step={1}
          value={draftIndex}
          aria-label="Reference month"
          onChange={(event) => {
            setDraftIndex(Number(event.target.value));
          }}
          onPointerUp={commit}
          onTouchEnd={commit}
          onKeyUp={commit}
          onBlur={commit}
        />
      </div>

      <span className="timeline-year">{endYear}</span>
    </div>
  );
}

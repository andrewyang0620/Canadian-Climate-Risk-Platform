type RGB = [number, number, number];

const RISK_STOPS: Array<{
  value: number;
  color: RGB;
}> = [
  {
    value: 0.0,
    color: [244, 234, 200],
  },
  {
    value: 0.15,
    color: [249, 218, 137],
  },
  {
    value: 0.3,
    color: [242, 175, 91],
  },
  {
    value: 0.45,
    color: [225, 112, 65],
  },
  {
    value: 0.6,
    color: [183, 67, 61],
  },
  {
    value: 0.8,
    color: [112, 35, 52],
  },
];

export type RGBA = [number, number, number, number];

function riskAlpha(value: number): number {
  const normalized = Math.max(0, Math.min(1, value / 0.8));

  return Math.round(65 + normalized * 135);
}

export function compositeRiskColor(value: number | null): RGBA {
  if (value === null) {
    return [105, 114, 109, 48];
  }

  const clamped = Math.max(0, Math.min(0.8, value));

  for (let index = 0; index < RISK_STOPS.length - 1; index += 1) {
    const left = RISK_STOPS[index];

    const right = RISK_STOPS[index + 1];

    if (clamped >= left.value && clamped <= right.value) {
      const span = right.value - left.value;

      const progress = span === 0 ? 0 : (clamped - left.value) / span;

      const color = left.color.map((channel, channelIndex) =>
        Math.round(
          channel + (right.color[channelIndex] - channel) * progress,
        ),
      ) as RGB;

      return [
        color[0],
        color[1],
        color[2],

        riskAlpha(value),
      ];
    }
  }

  const last = RISK_STOPS[RISK_STOPS.length - 1].color;

  return [last[0], last[1], last[2], riskAlpha(value)];
}

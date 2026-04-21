import { useMemo } from "react";
import { motion } from "framer-motion";
import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { PatternPlan, QuickPatternPreset } from "../types/order";

interface GrowthGraphProps {
  plan: PatternPlan;
  selectedPreset?: QuickPatternPreset | null;
  onApplyPreset?: (preset: QuickPatternPreset) => void;
  onGenerate?: () => void;
}

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

const presetButtons: Array<{ label: string; shortLabel: string; value: QuickPatternPreset }> = [
  { label: "🚀 Viral Boost", shortLabel: "🚀 Viral", value: "viral-boost" },
  { label: "⚡ Fast Start", shortLabel: "⚡ Fast", value: "fast-start" },
  { label: "🔥 Trending Push", shortLabel: "🔥 Trend", value: "trending-push" },
  { label: "🌊 Slow Burn", shortLabel: "🌊 Slow", value: "slow-burn" },
];

function lineTypeForPattern(patternType: PatternPlan["patternType"]) {
  if (patternType === "sawtooth") return "stepAfter";
  if (patternType === "viral-spike" || patternType === "micro-burst") return "linear";
  if (patternType === "heartbeat") return "natural";
  return "monotoneX";
}

function buildGraphData(plan: PatternPlan) {
  const safeRuns = plan?.runs || [];
  const rows: Array<{
    label: string;
    views: number;
    likes: number;
    shares: number;
    saves: number;
    comments: number;
  }> = [];

  rows.push({ label: "0m", views: 0, likes: 0, shares: 0, saves: 0, comments: 0 });

  for (let index = 0; index < safeRuns.length; index += 1) {
    const current = safeRuns[index];
    const previous = index === 0
      ? { minutesFromStart: 0, cumulativeViews: 0, cumulativeLikes: 0, cumulativeShares: 0, cumulativeSaves: 0, cumulativeComments: 0 }
      : safeRuns[index - 1];

    const dt = Math.max(1, current.minutesFromStart - previous.minutesFromStart);
    const phase = index / Math.max(1, safeRuns.length - 1);
    const segmentNoise = clamp(0.01 + (current.views / Math.max(1, safeRuns[0]?.views ?? 1)) * 0.004, 0.01, 0.03);

    const pointValue = (start: number, end: number, progress: number, wobbleScale: number, preserveMonotone: boolean) => {
      const eased = Math.pow(progress, phase < 0.2 ? 1.8 : phase > 0.8 ? 0.88 : 1.05);
      const delta = end - start;
      const wobble = delta * segmentNoise * wobbleScale;
      const value = start + delta * eased + wobble;
      if (!preserveMonotone) return Math.max(0, value);
      return clamp(value, Math.min(start, end), Math.max(start, end));
    };

    const wave = Math.sin((index + 1) * 1.13 + phase * Math.PI * 1.7);
    const minuteA = previous.minutesFromStart + dt * 0.38;
    const minuteB = previous.minutesFromStart + dt * 0.76;

    rows.push({
      label: `${Math.round(minuteA)}m`,
      views: pointValue(previous.cumulativeViews, current.cumulativeViews, 0.38, wave * 0.7, true),
      likes: pointValue(previous.cumulativeLikes, current.cumulativeLikes, 0.38, wave * 0.8, false),
      shares: pointValue(previous.cumulativeShares, current.cumulativeShares, 0.38, wave * 0.75, false),
      saves: pointValue(previous.cumulativeSaves, current.cumulativeSaves, 0.38, wave * 0.85, false),
      comments: pointValue(previous.cumulativeComments, current.cumulativeComments, 0.38, wave * 0.9, false),
    });

    rows.push({
      label: `${Math.round(minuteB)}m`,
      views: pointValue(previous.cumulativeViews, current.cumulativeViews, 0.76, wave * -0.55, true),
      likes: pointValue(previous.cumulativeLikes, current.cumulativeLikes, 0.76, wave * -0.62, false),
      shares: pointValue(previous.cumulativeShares, current.cumulativeShares, 0.76, wave * -0.58, false),
      saves: pointValue(previous.cumulativeSaves, current.cumulativeSaves, 0.76, wave * -0.64, false),
      comments: pointValue(previous.cumulativeComments, current.cumulativeComments, 0.76, wave * -0.7, false),
    });

    rows.push({
      label: current.at.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
      views: current.cumulativeViews,
      likes: current.cumulativeLikes,
      shares: current.cumulativeShares,
      saves: current.cumulativeSaves,
      comments: current.cumulativeComments,
    });
  }

  return rows;
}

export function GrowthGraph({ plan, selectedPreset, onApplyPreset, onGenerate }: GrowthGraphProps) {
  const safePlan = useMemo(() => ({ ...plan, runs: plan?.runs || [] }), [plan]);
  const chartData = useMemo(() => buildGraphData(safePlan), [safePlan]);
  const curveType = lineTypeForPattern(safePlan.patternType);

  return (
    <section className="rounded-2xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-3 sm:p-5">

      {/* Header */}
      <div className="mb-3 sm:mb-4 flex flex-col gap-3">
        <div className="flex items-center justify-between gap-2">
          <h2 className="text-sm sm:text-lg font-semibold text-yellow-400">
            📈 Growth Projection
          </h2>

          {/* New Pattern Button */}
          {onGenerate && (
            <button
              type="button"
              onClick={onGenerate}
              className="flex-shrink-0 rounded-lg border border-yellow-500/50 bg-yellow-500/10 px-2.5 py-1.5 text-[10px] sm:px-3 sm:text-xs font-medium text-yellow-300 transition hover:bg-yellow-500/20"
            >
              🔄 New Pattern
            </button>
          )}
        </div>

        {/* Preset Buttons - scrollable on mobile */}
        {onApplyPreset && (
          <div className="flex gap-1.5 overflow-x-auto no-scrollbar pb-1">
            {presetButtons.map((preset) => {
              const active = selectedPreset === preset.value;
              return (
                <button
                  key={preset.value}
                  type="button"
                  onClick={() => onApplyPreset(preset.value)}
                  className={`flex-shrink-0 rounded-lg border px-2 py-1.5 text-[10px] sm:px-2.5 sm:text-xs transition ${
                    active
                      ? "border-yellow-500/70 bg-yellow-500/20 text-yellow-300"
                      : "border-gray-700 text-gray-500 hover:border-yellow-500/30 hover:text-yellow-400"
                  }`}
                >
                  {/* Short label on mobile, full label on desktop */}
                  <span className="sm:hidden">{preset.shortLabel}</span>
                  <span className="hidden sm:inline">{preset.label}</span>
                </button>
              );
            })}
          </div>
        )}
      </div>

      {/* Chart */}
      <motion.div
        key={`${safePlan.patternId}-${safePlan.totalRuns}`}
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.35, ease: "easeOut" }}
        className="h-48 sm:h-80"
      >
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={chartData}
            margin={{ top: 8, right: 8, left: -20, bottom: 4 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis
              dataKey="label"
              tick={{ fill: "#9ca3af", fontSize: 9 }}
              minTickGap={30}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={{ fill: "#9ca3af", fontSize: 9 }}
              width={40}
            />
            <Tooltip
              contentStyle={{
                background: "#000000",
                border: "1px solid #eab308",
                borderRadius: "0.75rem",
                color: "#d1d5db",
                fontSize: "11px",
              }}
            />
            <Legend
              wrapperStyle={{ fontSize: "10px", color: "#d1d5db" }}
            />
            <Line type={curveType} dataKey="views" name="Views" stroke="#eab308" strokeWidth={2} dot={false} isAnimationActive animationDuration={900} />
            <Line type={curveType} dataKey="likes" name="Likes" stroke="#a78bfa" strokeWidth={1.5} dot={false} isAnimationActive animationDuration={900} />
            <Line type={curveType} dataKey="shares" name="Shares" stroke="#f59e0b" strokeWidth={1.5} dot={false} isAnimationActive animationDuration={900} />
            <Line type={curveType} dataKey="saves" name="Saves" stroke="#34d399" strokeWidth={1.5} dot={false} isAnimationActive animationDuration={900} />
            <Line type={curveType} dataKey="comments" name="Comments" stroke="#f472b6" strokeWidth={1.5} dot={false} isAnimationActive animationDuration={900} />
          </LineChart>
        </ResponsiveContainer>
      </motion.div>
    </section>
  );
}

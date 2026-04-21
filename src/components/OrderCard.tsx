import { useEffect, useMemo, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import type { CreatedOrder, OrderStatus } from "../types/order";
import { RunTable } from "./RunTable";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
} from "recharts";

interface OrderCardProps {
  order: CreatedOrder;
  onControl: (order: CreatedOrder, action: "pause" | "resume" | "cancel") => void;
  onClone: (order: CreatedOrder) => void;
  controlBusy: boolean;
}

const statusColor: Record<OrderStatus, string> = {
  running: "text-yellow-300",
  paused: "text-amber-300",
  cancelled: "text-red-300",
  completed: "text-emerald-300",
  processing: "text-yellow-300",
  failed: "text-red-300",
};

export function OrderCard({ order, onControl, onClone, controlBusy }: OrderCardProps) {
  const [expanded, setExpanded] = useState(false);
  const [nowMs, setNowMs] = useState(() => Date.now());
  const safeRuns = order?.runs || [];
  const safeRunStatuses = order?.runStatuses || [];
  const safeRunErrors = order?.runErrors || [];
  const finishTime = safeRuns[safeRuns.length - 1]?.at;

  useEffect(() => {
    const timer = window.setInterval(() => setNowMs(Date.now()), 4000);
    return () => window.clearInterval(timer);
  }, []);

  const { totalRuns, completedRuns, progressPercent } = useMemo(() => {
    const nextTotalRuns = Math.max(1, safeRuns.length);
    const completedFromStatuses = safeRunStatuses.filter((status) => status === "completed").length;
    const completedFromTime = safeRuns.reduce((count, run) => {
      const runTime = run?.at instanceof Date ? run.at.getTime() : new Date(run?.at ?? Date.now()).getTime();
      return runTime <= nowMs ? count + 1 : count;
    }, 0);
    const isTimeTrackedStatus = order.status === "running" || order.status === "processing" || order.status === "completed";
    const nextCompletedRuns = Math.min(
      nextTotalRuns,
      Math.max(
        0,
        order.status === "completed" ? nextTotalRuns : 0,
        Number.isFinite(order.completedRuns) ? order.completedRuns : 0,
        completedFromStatuses,
        isTimeTrackedStatus ? completedFromTime : 0
      )
    );
    const nextProgressPercent = Math.round((nextCompletedRuns / nextTotalRuns) * 100);
    return { totalRuns: nextTotalRuns, completedRuns: nextCompletedRuns, progressPercent: nextProgressPercent };
  }, [safeRuns, safeRunStatuses, order.status, order.completedRuns, nowMs]);

  const effectiveStatus = useMemo(() => {
    const runs = order.runs || [];
    const now = Date.now();
    if (runs.length > 0) {
      const allCompleted = runs.every((run) => {
        const runTime = new Date(run.at).getTime();
        return runTime <= now;
      });
      if (allCompleted) return "completed";
    }
    if (order.status === "processing") return "running";
    return order.status;
  }, [order, nowMs]);

  const graphData = useMemo(() => {
    const runs = Array.isArray(order?.runs) ? order.runs : [];
    let v = 0, l = 0, sh = 0, sa = 0, c = 0;
    return runs.map((run) => {
      if (!run) return null;
      const runTime = run?.at ? new Date(run.at).getTime() : 0;
      if (runTime <= nowMs) {
        v += Number(run?.views || 0);
        l += Number(run?.likes || 0);
        sh += Number(run?.shares || 0);
        sa += Number(run?.saves || 0);
        c += Number(run?.comments || 0);
      }
      return { time: run.at, views: v, likes: l, shares: sh, saves: sa, comments: c };
    }).filter(Boolean);
  }, [order?.runs, nowMs]);

  const plannedData = useMemo(() => {
    const runs = order.runs || [];
    return runs.map((run) => ({
      time: run.at,
      views: run.cumulativeViews || 0,
      likes: (run.cumulativeLikes || 0) * 10,
      shares: (run.cumulativeShares || 0) * 10,
      saves: (run.cumulativeSaves || 0) * 10,
      comments: (run.cumulativeComments || 0) * 10,
    }));
  }, [order.runs]);

  const shortLink =
    order.link.length > 56
      ? `${order.link.slice(0, 36)}...${order.link.slice(-14)}`
      : order.link;

  const handleControl = async (action: "pause" | "resume" | "cancel") => {
    try {
      if (action === "cancel") {
        const confirmCancel = window.confirm("Are you sure you want to cancel this mission?");
        if (!confirmCancel) return;
      }
      onControl(order, action);
    } catch (err) {
      console.error("Control action failed", err);
      alert("Action failed. Please try again.");
    }
  };

  return (
    <article className="rounded-2xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-3 sm:p-5">

      {/* Top Section */}
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between sm:gap-4">

        {/* Left - Mission Info */}
        <div className="space-y-1 sm:space-y-2 min-w-0">
          <p className="text-[10px] sm:text-xs uppercase tracking-wide text-gray-600">
            Mission ID
          </p>
          <h3 className="text-base sm:text-lg font-semibold text-yellow-400">
            {order.id}
          </h3>
          <p className="text-xs sm:text-sm text-yellow-300">
            {order.name || `Mission #${order.id}`}
          </p>
          <p
            className="max-w-full truncate text-xs sm:text-sm text-gray-500"
            title={order.link || "No link provided"}
          >
            {shortLink || "No link provided"}
          </p>
          {order.schedulerOrderId && (
            <p className="text-[10px] sm:text-xs text-gray-600 font-mono">
              Scheduler: {order.schedulerOrderId}
            </p>
          )}
        </div>

        {/* Right - Order Details */}
        <div className="flex flex-row flex-wrap gap-x-4 gap-y-1 sm:flex-col sm:space-y-2 sm:text-right">
          <p className="text-xs sm:text-sm text-gray-500">
            Panel ID:{" "}
            <span className="font-semibold text-yellow-300">{order.smmOrderId}</span>
          </p>
          <p className="text-xs sm:text-sm text-gray-500">
            Service:{" "}
            <span className="font-semibold text-gray-300">{order.serviceId}</span>
          </p>
          <p className="text-xs sm:text-sm text-gray-500">
            Qty:{" "}
            <span className="font-semibold text-gray-300">{order.totalViews}</span>
          </p>
          <p className="text-xs sm:text-sm text-gray-500">
            Status:{" "}
            <span className={`font-semibold ${statusColor[effectiveStatus]}`}>
              {effectiveStatus}
            </span>
          </p>
          {order.errorMessage && (
            <p className="text-[10px] sm:text-xs text-red-400 break-words">
              Error: {order.errorMessage}
            </p>
          )}
          {finishTime && (
            <p className="text-[10px] sm:text-xs text-gray-600">
              ETA: {finishTime.toLocaleString()}
            </p>
          )}
          <p className="text-[10px] sm:text-xs text-gray-600">
            Updated: {new Date(order.lastUpdatedAt || order.createdAt).toLocaleString()}
          </p>
        </div>
      </div>

      {/* Progress Section */}
      <div className="mt-3 sm:mt-4 space-y-2">
        <div className="flex items-center justify-between text-xs text-gray-500">
          <span>Progress</span>
          <span>{progressPercent}%</span>
        </div>
        <div className="h-2 w-full overflow-hidden rounded-full bg-gray-800">
          <div
            className="h-full rounded-full bg-yellow-500 transition-all"
            style={{ width: `${progressPercent}%` }}
          />
        </div>
        <p className="text-xs text-gray-500">
          {completedRuns} / {totalRuns} runs completed
        </p>

        {/* Chart - smaller on mobile */}
        <div className="mt-3 sm:mt-4 h-36 sm:h-48 w-full">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart
              data={plannedData}
              margin={{ top: 4, right: 4, left: -25, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#111" opacity={0.3} />
              <XAxis
                dataKey="time"
                stroke="#666"
                tick={{ fontSize: 8 }}
                tickFormatter={(time) => {
                  const d = new Date(time);
                  return d.getHours() + ":" + String(d.getMinutes()).padStart(2, "0");
                }}
              />
              <YAxis stroke="#666" tick={{ fontSize: 8 }} width={30} />
              <Tooltip
                formatter={(value, name) => {
                  if (name?.startsWith("planned")) return null;
                  return [value, name];
                }}
                contentStyle={{
                  background: "#000",
                  border: "1px solid #eab308",
                  borderRadius: "8px",
                  fontSize: "10px",
                }}
              />
              <Line type="monotone" dataKey="views" stroke="#3b82f6" opacity={0.1} dot={false} strokeDasharray="5 5" name="planned-views" />
              <Line type="monotone" dataKey="likes" stroke="#ec4899" opacity={0.1} dot={false} strokeDasharray="5 5" name="planned-likes" />
              <Line type="monotone" dataKey="shares" stroke="#22c55e" opacity={0.1} dot={false} strokeDasharray="5 5" name="planned-shares" />
              <Line type="monotone" dataKey="saves" stroke="#eab308" opacity={0.1} dot={false} strokeDasharray="5 5" name="planned-saves" />
              <Line type="monotone" dataKey="comments" stroke="#a855f7" opacity={0.1} dot={false} strokeDasharray="5 5" name="planned-comments" />
              <Line type="monotone" dataKey="views" stroke="#3b82f6" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="likes" stroke="#ec4899" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="shares" stroke="#22c55e" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="comments" stroke="#a855f7" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Action Buttons */}
      <div className="mt-3 sm:mt-4 flex flex-wrap gap-2">
        <button
          type="button"
          disabled={controlBusy || effectiveStatus !== "running"}
          onClick={() => handleControl("pause")}
          className="rounded-lg border border-amber-500/50 bg-amber-500/10 px-2.5 py-1.5 text-xs text-amber-300 transition hover:bg-amber-500/20 disabled:cursor-not-allowed disabled:opacity-50"
        >
          ⏸️ Pause
        </button>
        <button
          type="button"
          disabled={controlBusy || effectiveStatus !== "paused"}
          onClick={() => handleControl("resume")}
          className="rounded-lg border border-emerald-500/50 bg-emerald-500/10 px-2.5 py-1.5 text-xs text-emerald-300 transition hover:bg-emerald-500/20 disabled:cursor-not-allowed disabled:opacity-50"
        >
          ▶️ Resume
        </button>
        <button
          type="button"
          disabled={controlBusy || effectiveStatus === "cancelled" || effectiveStatus === "completed"}
          onClick={() => handleControl("cancel")}
          className="rounded-lg border border-red-500/50 bg-red-500/10 px-2.5 py-1.5 text-xs text-red-300 transition hover:bg-red-500/20 disabled:cursor-not-allowed disabled:opacity-50"
        >
          ❌ Cancel
        </button>
        <button
          type="button"
          onClick={() => onClone(order)}
          className="rounded-lg border border-yellow-500/50 bg-yellow-500/10 px-2.5 py-1.5 text-xs text-yellow-300 transition hover:bg-yellow-500/20"
        >
          📋 Clone
        </button>
        <button
          type="button"
          onClick={() => setExpanded((prev) => !prev)}
          className={`ml-auto rounded-lg border px-2.5 py-1.5 text-xs transition ${
            expanded
              ? "border-yellow-500/50 bg-yellow-500/20 text-yellow-300"
              : "border-yellow-500/30 bg-yellow-500/10 text-yellow-400 hover:bg-yellow-500/20"
          }`}
        >
          {expanded ? "🔼 Hide" : "📋 Runs"}
        </button>
      </div>

      {/* Run Table */}
      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
          >
            <RunTable
              runs={safeRuns}
              runStatuses={safeRunStatuses}
              runErrors={safeRunErrors}
              runRetries={order.runRetries || []}
              runOriginalTimes={order.runOriginalTimes || []}
              runCurrentTimes={order.runCurrentTimes || []}
              runReasons={order.runReasons || []}
              mode="logs"
            />
          </motion.div>
        )}
      </AnimatePresence>
    </article>
  );
}

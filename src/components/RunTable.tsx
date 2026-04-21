import { useMemo } from "react";
import type { RunStep } from "../types/order";

type ExtendedRunStatus = "pending" | "completed" | "cancelled" | "retrying" | "executing" | "timeout";

interface RunTableProps {
  runs: RunStep[];
  runStatuses?: Array<"pending" | "completed" | "cancelled" | "retrying">;
  runErrors?: string[];
  runRetries?: number[];
  runOriginalTimes?: string[];
  runCurrentTimes?: string[];
  runReasons?: string[];
  mode?: "schedule" | "logs";
}

const STATUS_CONFIG: Record<ExtendedRunStatus, { label: string; color: string; bg: string; icon: string }> = {
  completed: { label: "Done", color: "text-emerald-400", bg: "bg-emerald-500/20", icon: "✅" },
  pending: { label: "Pending", color: "text-blue-400", bg: "bg-blue-500/20", icon: "⏳" },
  retrying: { label: "Retry", color: "text-yellow-400", bg: "bg-yellow-500/20", icon: "🔄" },
  executing: { label: "Running", color: "text-purple-400", bg: "bg-purple-500/20", icon: "⚡" },
  cancelled: { label: "Cancel", color: "text-red-400", bg: "bg-red-500/20", icon: "❌" },
  timeout: { label: "Timeout", color: "text-orange-400", bg: "bg-orange-500/20", icon: "⏰" },
};

export function RunTable({
  runs,
  runStatuses = [],
  runErrors = [],
  runRetries = [],
  runOriginalTimes = [],
  runCurrentTimes = [],
  runReasons = [],
  mode = "logs",
}: RunTableProps) {
  const safeRuns = runs || [];
  const safeRunStatuses = runStatuses || [];
  const safeRunErrors = runErrors || [];
  const safeRunRetries = runRetries || [];
  const safeRunOriginalTimes = runOriginalTimes || [];
  const safeRunCurrentTimes = runCurrentTimes || [];
  const safeRunReasons = runReasons || [];

  const getTimeDisplay = (index: number, originalRunTime: Date) => {
    const originalTime = safeRunOriginalTimes[index];
    const currentTime = safeRunCurrentTimes[index];
    if (originalTime && currentTime) {
      const origDate = new Date(originalTime);
      const currDate = new Date(currentTime);
      const isRescheduled = origDate.getTime() !== currDate.getTime();
      return { original: origDate, current: currDate, isRescheduled };
    }
    return { original: originalRunTime, current: originalRunTime, isRescheduled: false };
  };

  const getStatus = (index: number): ExtendedRunStatus => {
    const status = safeRunStatuses[index];
    const retryCount = safeRunRetries[index] || 0;
    const reason = safeRunReasons[index];
    if (status === "cancelled") return "cancelled";
    if (status === "completed") return "completed";
    if (reason?.toLowerCase().includes("timeout")) return "timeout";
    if (status === "retrying" || retryCount > 0) return "retrying";
    return "pending";
  };

  const formatTime = (date: Date) => {
    return date.toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  };

  const formatRelativeTime = (date: Date) => {
    const now = new Date();
    const diff = date.getTime() - now.getTime();
    if (diff < 0) {
      const minutes = Math.abs(Math.floor(diff / (1000 * 60)));
      if (minutes < 60) return `${minutes}m ago`;
      const hours = Math.floor(minutes / 60);
      return `${hours}h ago`;
    }
    const minutes = Math.floor(diff / (1000 * 60));
    if (minutes < 60) return `in ${minutes}m`;
    const hours = Math.floor(minutes / 60);
    return `in ${hours}h`;
  };

  const stats = useMemo(() => ({
    total: safeRuns.length,
    completed: safeRunStatuses.filter(s => s === "completed").length,
    retrying: safeRunStatuses.filter(s => s === "retrying").length,
    pending: safeRunStatuses.filter(s => s === "pending").length,
    cancelled: safeRunStatuses.filter(s => s === "cancelled").length,
    totalRetries: safeRunRetries.reduce((sum, r) => sum + (r || 0), 0),
  }), [safeRuns, safeRunStatuses, safeRunRetries]);

  // Schedule mode - simple view
  if (mode === "schedule") {
    return (
      <div className="mt-3 max-h-64 sm:max-h-72 overflow-auto rounded-xl border border-slate-800">
        <table className="w-full text-left text-[10px] sm:text-xs text-slate-300">
          <thead className="sticky top-0 bg-[#0f1627] text-slate-400">
            <tr>
              <th className="px-2 py-2 sm:px-3">#</th>
              <th className="px-2 py-2 sm:px-3">Time</th>
              <th className="px-2 py-2 sm:px-3">Views</th>
              <th className="hidden sm:table-cell px-3 py-2">Likes</th>
              <th className="hidden sm:table-cell px-3 py-2">Shares</th>
              <th className="hidden sm:table-cell px-3 py-2">Saves</th>
              <th className="hidden sm:table-cell px-3 py-2">Cmts</th>
            </tr>
          </thead>
          <tbody>
            {safeRuns.map((run) => (
              <tr key={run.run} className="border-t border-slate-800/80 align-top">
                <td className="px-2 py-1.5 sm:px-3">#{run.run}</td>
                <td className="px-2 py-1.5 sm:px-3 text-slate-400 text-[9px] sm:text-[10px]">
                  {run.at.toLocaleString(undefined, {
                    month: "short",
                    day: "numeric",
                    hour: "2-digit",
                    minute: "2-digit",
                  })}
                </td>
                <td className="px-2 py-1.5 sm:px-3">{run.views}</td>
                <td className="hidden sm:table-cell px-3 py-1.5">{run.likes}</td>
                <td className="hidden sm:table-cell px-3 py-1.5">{run.shares}</td>
                <td className="hidden sm:table-cell px-3 py-1.5">{run.saves}</td>
                <td className="hidden sm:table-cell px-3 py-1.5">{run.comments || 0}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  // Logs mode - detailed view
  return (
    <div className="mt-3 space-y-3">

      {/* Stats Summary */}
      {stats.total > 0 && (
        <div className="flex flex-wrap gap-1.5 sm:gap-2 text-[9px] sm:text-[10px]">
          <span className="rounded-full bg-emerald-500/20 px-2 py-0.5 text-emerald-400">
            ✅ {stats.completed}
          </span>
          {stats.retrying > 0 && (
            <span className="rounded-full bg-yellow-500/20 px-2 py-0.5 text-yellow-400">
              🔄 {stats.retrying}
            </span>
          )}
          {stats.pending > 0 && (
            <span className="rounded-full bg-blue-500/20 px-2 py-0.5 text-blue-400">
              ⏳ {stats.pending}
            </span>
          )}
          {stats.cancelled > 0 && (
            <span className="rounded-full bg-red-500/20 px-2 py-0.5 text-red-400">
              ❌ {stats.cancelled}
            </span>
          )}
          {stats.totalRetries > 0 && (
            <span className="rounded-full bg-orange-500/20 px-2 py-0.5 text-orange-400">
              ↻ {stats.totalRetries}
            </span>
          )}
        </div>
      )}

      {/* Runs Table */}
      <div className="max-h-80 sm:max-h-96 overflow-auto rounded-xl border border-slate-800">
        <table className="w-full text-left text-[9px] sm:text-xs text-slate-300">
          <thead className="sticky top-0 bg-[#0f1627] text-slate-400 z-10">
            <tr>
              <th className="px-2 py-2 sm:px-3 w-10">#</th>
              <th className="px-2 py-2 sm:px-3">Time</th>
              <th className="px-2 py-2 sm:px-3">Views</th>
              <th className="hidden sm:table-cell px-3 py-2">Likes</th>
              <th className="hidden sm:table-cell px-3 py-2">Shares</th>
              <th className="hidden sm:table-cell px-3 py-2">Saves</th>
              <th className="hidden sm:table-cell px-3 py-2">Cmts</th>
              <th className="px-2 py-2 sm:px-3">Status</th>
              <th className="hidden sm:table-cell px-3 py-2">Retry</th>
              <th className="hidden sm:table-cell px-3 py-2">Info</th>
            </tr>
          </thead>
          <tbody>
            {safeRuns.map((run, index) => {
              const status = getStatus(index);
              const config = STATUS_CONFIG[status];
              const retryCount = safeRunRetries[index] || 0;
              const error = safeRunErrors[index];
              const reason = safeRunReasons[index];
              const timeData = getTimeDisplay(index, run.at);

              return (
                <tr
                  key={run.run}
                  className={`border-t border-slate-800/80 align-top transition-colors ${
                    status === "retrying" ? "bg-yellow-500/5" :
                    status === "cancelled" ? "bg-red-500/5" :
                    status === "completed" ? "bg-emerald-500/5" : ""
                  }`}
                >
                  {/* Run # */}
                  <td className="px-2 py-1.5 sm:px-3 font-medium">
                    #{run.run}
                  </td>

                  {/* Time */}
                  <td className="px-2 py-1.5 sm:px-3">
                    <div className={`font-medium text-[9px] sm:text-[10px] ${
                      timeData.isRescheduled ? "text-yellow-400" : "text-slate-300"
                    }`}>
                      {formatTime(timeData.current)}
                    </div>
                    <div className="text-[8px] sm:text-[9px] text-slate-600">
                      {formatRelativeTime(timeData.current)}
                    </div>
                    {timeData.isRescheduled && (
                      <div className="text-[8px] text-slate-600 line-through hidden sm:block">
                        {formatTime(timeData.original)}
                      </div>
                    )}
                  </td>

                  {/* Views */}
                  <td className="px-2 py-1.5 sm:px-3 text-slate-400">
                    {run.views}
                  </td>

                  {/* Hidden on mobile */}
                  <td className="hidden sm:table-cell px-3 py-1.5 text-slate-400">{run.likes}</td>
                  <td className="hidden sm:table-cell px-3 py-1.5 text-slate-400">{run.shares}</td>
                  <td className="hidden sm:table-cell px-3 py-1.5 text-slate-400">{run.saves}</td>
                  <td className="hidden sm:table-cell px-3 py-1.5 text-slate-400">{run.comments}</td>

                  {/* Status Badge */}
                  <td className="px-2 py-1.5 sm:px-3">
                    <span className={`inline-flex items-center gap-0.5 sm:gap-1 rounded-full px-1.5 py-0.5 text-[8px] sm:text-[10px] font-medium ${config.bg} ${config.color}`}>
                      <span>{config.icon}</span>
                      <span className="hidden sm:inline">{config.label}</span>
                    </span>
                  </td>

                  {/* Retry - hidden on mobile */}
                  <td className="hidden sm:table-cell px-3 py-1.5">
                    {retryCount > 0 ? (
                      <span className="inline-flex items-center gap-0.5 rounded-full bg-yellow-500/20 px-2 py-0.5 text-[10px] font-medium text-yellow-400">
                        ↻ {retryCount}
                      </span>
                    ) : (
                      <span className="text-slate-700">-</span>
                    )}
                  </td>

                  {/* Info - hidden on mobile */}
                  <td className="hidden sm:table-cell px-3 py-1.5 max-w-[200px]">
                    {(reason || error) ? (
                      <div className="space-y-0.5">
                        {reason && (
                          <p className={`text-[10px] truncate ${
                            reason.toLowerCase().includes("waiting") ? "text-yellow-400" :
                            reason.toLowerCase().includes("timeout") ? "text-orange-400" :
                            reason.toLowerCase().includes("success") ? "text-emerald-400" :
                            "text-slate-500"
                          }`} title={reason}>
                            {reason.length > 40 ? `${reason.slice(0, 40)}...` : reason}
                          </p>
                        )}
                        {error && !reason?.includes(error) && (
                          <p className="text-[10px] text-rose-400 truncate" title={error}>
                            ⚠️ {error.length > 35 ? `${error.slice(0, 35)}...` : error}
                          </p>
                        )}
                      </div>
                    ) : (
                      <span className="text-slate-700">-</span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Legend */}
      <div className="flex flex-wrap gap-2 sm:gap-3 text-[8px] sm:text-[9px] text-slate-600 pt-1">
        <span>Legend:</span>
        <span className="flex items-center gap-1">
          <span className="h-1.5 w-1.5 rounded-full bg-emerald-500" /> Done
        </span>
        <span className="flex items-center gap-1">
          <span className="h-1.5 w-1.5 rounded-full bg-blue-500" /> Pending
        </span>
        <span className="flex items-center gap-1">
          <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-yellow-500" /> Retrying
        </span>
        <span className="flex items-center gap-1">
          <span className="h-1.5 w-1.5 rounded-full bg-red-500" /> Cancelled
        </span>
      </div>
    </div>
  );
}

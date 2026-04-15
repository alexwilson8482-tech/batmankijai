import { useMemo } from "react";
import type { RunStep } from "../types/order";

// 🔥 Extended run status type
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

// Status configuration
const STATUS_CONFIG: Record<ExtendedRunStatus, { label: string; color: string; bg: string; icon: string }> = {
  completed: { label: "Success", color: "text-emerald-400", bg: "bg-emerald-500/20", icon: "✅" },
  pending: { label: "Pending", color: "text-blue-400", bg: "bg-blue-500/20", icon: "⏳" },
  retrying: { label: "Retrying", color: "text-yellow-400", bg: "bg-yellow-500/20", icon: "🔄" },
  executing: { label: "Executing", color: "text-purple-400", bg: "bg-purple-500/20", icon: "⚡" },
  cancelled: { label: "Cancelled", color: "text-red-400", bg: "bg-red-500/20", icon: "❌" },
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
  mode = "logs" 
}: RunTableProps) {
  const safeRuns = runs || [];
  const safeRunStatuses = runStatuses || [];
  const safeRunErrors = runErrors || [];
  const safeRunRetries = runRetries || [];
  const safeRunOriginalTimes = runOriginalTimes || [];
  const safeRunCurrentTimes = runCurrentTimes || [];
  const safeRunReasons = runReasons || [];

  // 🔥 Calculate if time was rescheduled
  const getTimeDisplay = (index: number, originalRunTime: Date) => {
    const originalTime = safeRunOriginalTimes[index];
    const currentTime = safeRunCurrentTimes[index];
    
    // If we have backend times, use them
    if (originalTime && currentTime) {
      const origDate = new Date(originalTime);
      const currDate = new Date(currentTime);
      const isRescheduled = origDate.getTime() !== currDate.getTime();
      
      return {
        original: origDate,
        current: currDate,
        isRescheduled,
      };
    }
    
    // Fallback to run.at
    return {
      original: originalRunTime,
      current: originalRunTime,
      isRescheduled: false,
    };
  };

  // 🔥 Get status with retry consideration
  const getStatus = (index: number): ExtendedRunStatus => {
    const status = safeRunStatuses[index];
    const retryCount = safeRunRetries[index] || 0;
    const error = safeRunErrors[index];
    const reason = safeRunReasons[index];
    
    if (status === "cancelled") return "cancelled";
    if (status === "completed") return "completed";
    
    // Check for timeout
    if (reason?.toLowerCase().includes("timeout")) return "timeout";
    
    // If retrying
    if (status === "retrying" || retryCount > 0) return "retrying";
    
    return "pending";
  };

  // Format time nicely
  const formatTime = (date: Date) => {
    return date.toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  };

  // Format relative time
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

  // 🔥 Stats summary
  const stats = useMemo(() => {
    return {
      total: safeRuns.length,
      completed: safeRunStatuses.filter(s => s === "completed").length,
      retrying: safeRunStatuses.filter(s => s === "retrying").length,
      pending: safeRunStatuses.filter(s => s === "pending").length,
      cancelled: safeRunStatuses.filter(s => s === "cancelled").length,
      totalRetries: safeRunRetries.reduce((sum, r) => sum + (r || 0), 0),
    };
  }, [safeRuns, safeRunStatuses, safeRunRetries]);

  // Schedule mode (simple view)
  if (mode === "schedule") {
    return (
      <div className="mt-3 max-h-72 overflow-auto rounded-xl border border-slate-800">
        <table className="w-full text-left text-xs text-slate-300">
          <thead className="sticky top-0 bg-[#0f1627] text-slate-400">
            <tr>
              <th className="px-3 py-2">Run</th>
              <th className="px-3 py-2">Time</th>
              <th className="px-3 py-2">Views</th>
              <th className="px-3 py-2">Likes</th>
              <th className="px-3 py-2">Shares</th>
              <th className="px-3 py-2">Saves</th>
              <th className="px-3 py-2">Comments</th>
            </tr>
          </thead>
          <tbody>
            {safeRuns.map((run) => (
              <tr key={run.run} className="border-t border-slate-800/80 align-top">
                <td className="px-3 py-2">#{run.run}</td>
                <td className="px-3 py-2 text-slate-400">{run.at.toLocaleString()}</td>
                <td className="px-3 py-2">{run.views}</td>
                <td className="px-3 py-2">{run.likes}</td>
                <td className="px-3 py-2">{run.shares}</td>
                <td className="px-3 py-2">{run.saves}</td>
                <td className="px-3 py-2">{run.comments || 0}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  // 🔥 Logs mode (detailed view with retry info)
  return (
    <div className="mt-3 space-y-3">
      {/* Stats Summary */}
      {stats.total > 0 && (
        <div className="flex flex-wrap gap-2 text-[10px]">
          <span className="rounded-full bg-emerald-500/20 px-2 py-0.5 text-emerald-400">
            ✅ {stats.completed} completed
          </span>
          {stats.retrying > 0 && (
            <span className="rounded-full bg-yellow-500/20 px-2 py-0.5 text-yellow-400">
              🔄 {stats.retrying} retrying
            </span>
          )}
          {stats.pending > 0 && (
            <span className="rounded-full bg-blue-500/20 px-2 py-0.5 text-blue-400">
              ⏳ {stats.pending} pending
            </span>
          )}
          {stats.cancelled > 0 && (
            <span className="rounded-full bg-red-500/20 px-2 py-0.5 text-red-400">
              ❌ {stats.cancelled} cancelled
            </span>
          )}
          {stats.totalRetries > 0 && (
            <span className="rounded-full bg-orange-500/20 px-2 py-0.5 text-orange-400">
              ↻ {stats.totalRetries} total retries
            </span>
          )}
        </div>
      )}

      {/* Runs Table */}
      <div className="max-h-96 overflow-auto rounded-xl border border-slate-800">
        <table className="w-full text-left text-xs text-slate-300">
          <thead className="sticky top-0 bg-[#0f1627] text-slate-400 z-10">
            <tr>
              <th className="px-3 py-2 w-12">Run</th>
              <th className="px-3 py-2">Time</th>
              <th className="px-3 py-2 w-16">Views</th>
              <th className="px-3 py-2 w-14">Likes</th>
              <th className="px-3 py-2 w-16">Shares</th>
              <th className="px-3 py-2 w-14">Saves</th>
              <th className="px-3 py-2 w-14">Comments</th>
              <th className="px-3 py-2 w-24">Status</th>
              <th className="px-3 py-2 w-14">Retry</th>
              <th className="px-3 py-2">Info</th>
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
                  {/* Run Number */}
                  <td className="px-3 py-2 font-medium">#{run.run}</td>
                  
                  {/* Time - Original + Current */}
                  <td className="px-3 py-2">
                    <div className="space-y-0.5">
                      {/* Current/Scheduled Time */}
                      <div className={`font-medium ${
                        timeData.isRescheduled ? "text-yellow-400" : "text-slate-300"
                      }`}>
                        {formatTime(timeData.current)}
                        <span className="ml-1 text-slate-600 text-[10px]">
                          ({formatRelativeTime(timeData.current)})
                        </span>
                      </div>
                      
                      {/* Original Time (if rescheduled) */}
                      {timeData.isRescheduled && (
                        <div className="text-[10px] text-slate-600 line-through">
                          Original: {formatTime(timeData.original)}
                        </div>
                      )}
                    </div>
                  </td>
                  
                  {/* Quantities */}
                  <td className="px-3 py-2 text-slate-400">{run.views}</td>
                  <td className="px-3 py-2 text-slate-400">{run.likes}</td>
                  <td className="px-3 py-2 text-slate-400">{run.shares}</td>
                  <td className="px-3 py-2 text-slate-400">{run.saves}</td>
                  <td className="px-3 py-2 text-slate-400">{run.comments}</td>
                  
                  {/* Status Badge */}
                  <td className="px-3 py-2">
                    <span className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-medium ${config.bg} ${config.color}`}>
                      <span>{config.icon}</span>
                      <span>{config.label}</span>
                    </span>
                  </td>
                  
                  {/* Retry Count */}
                  <td className="px-3 py-2">
                    {retryCount > 0 ? (
                      <span className="inline-flex items-center gap-0.5 rounded-full bg-yellow-500/20 px-2 py-0.5 text-[10px] font-medium text-yellow-400">
                        ↻ {retryCount}
                      </span>
                    ) : (
                      <span className="text-slate-700">-</span>
                    )}
                  </td>
                  
                  {/* Error/Reason */}
                  <td className="px-3 py-2 max-w-[200px]">
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
      <div className="flex flex-wrap gap-3 text-[9px] text-slate-600 pt-1">
        <span>Legend:</span>
        <span className="flex items-center gap-1">
          <span className="h-2 w-2 rounded-full bg-emerald-500"></span> Completed
        </span>
        <span className="flex items-center gap-1">
          <span className="h-2 w-2 rounded-full bg-blue-500"></span> Pending
        </span>
        <span className="flex items-center gap-1">
          <span className="h-2 w-2 rounded-full bg-yellow-500 animate-pulse"></span> Retrying
        </span>
        <span className="flex items-center gap-1">
          <span className="h-2 w-2 rounded-full bg-red-500"></span> Cancelled
        </span>
        <span className="flex items-center gap-1">
          <span className="text-yellow-400">Yellow time</span> = Rescheduled
        </span>
      </div>
    </div>
  );
}

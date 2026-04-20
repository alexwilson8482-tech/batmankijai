import { useEffect, useMemo, useState, useRef } from "react";
import { motion, AnimatePresence } from "framer-motion";
import type { BackendRunInfo, CreatedOrder } from "../types/order";
import { OrderCard } from "../components/OrderCard";

interface OrdersPageProps {
  orders: CreatedOrder[];
  notice: string;
  controllingOrderId: string | null;
  onControlOrder: (order: CreatedOrder, action: "pause" | "resume" | "cancel") => void;
  onCloneOrder: (order: CreatedOrder) => void;
  onDismissNotice: () => void;
}

type TabType = "running" | "completed" | "scheduled" | "cancelled";
type ViewMode = "rows" | "columns";

interface GroupedOrder {
  id: string;
  batchId: string | null;
  name: string;
  orders: CreatedOrder[];
  isBatch: boolean;
  totalViews: number;
  linksCount: number;
  createdAt: string;
}

const STATUS_COLORS: Record<string, { bg: string; text: string; dot: string }> = {
  running: { bg: "bg-yellow-500/15", text: "text-yellow-300", dot: "bg-yellow-400" },
  processing: { bg: "bg-yellow-500/15", text: "text-yellow-300", dot: "bg-yellow-400" },
  completed: { bg: "bg-emerald-500/15", text: "text-emerald-300", dot: "bg-emerald-400" },
  scheduled: { bg: "bg-amber-500/15", text: "text-amber-300", dot: "bg-amber-400" },
  paused: { bg: "bg-orange-500/15", text: "text-orange-300", dot: "bg-orange-400" },
  cancelled: { bg: "bg-red-500/15", text: "text-red-300", dot: "bg-red-400" },
  failed: { bg: "bg-red-500/15", text: "text-red-300", dot: "bg-red-400" },
  pending: { bg: "bg-gray-500/15", text: "text-gray-300", dot: "bg-gray-400" },
};

const TABS: { key: TabType; label: string; icon: string }[] = [
  { key: "running", label: "Active", icon: "⚡" },
  { key: "scheduled", label: "Scheduled", icon: "⏱" },
  { key: "completed", label: "Completed", icon: "✓" },
  { key: "cancelled", label: "Cancelled", icon: "❌" },
];

// 🔥 Shared getRealStatus - single source of truth
function getRealStatus(order: CreatedOrder): string {
  if (order.status === "cancelled") return "cancelled";
  if (order.status === "failed") return "failed";

  const runs = order.runs || [];
  const now = Date.now();

  if (runs.length > 0) {
    const allFuture = runs.every((run) => {
      const runTime =
        run?.at instanceof Date
          ? run.at.getTime()
          : new Date(run?.at ?? now).getTime();
      return runTime > now;
    });
    if (allFuture && order.status !== "paused") {
      return "scheduled";
    }
  }

  if (runs.length > 0) {
    const allPast = runs.every((run) => {
      const runTime =
        run?.at instanceof Date
          ? run.at.getTime()
          : new Date(run?.at ?? now).getTime();
      return runTime <= now;
    });
    if (allPast) return "completed";
  }

  if (order.status === "processing") return "running";
  if (order.status === "pending") return "running";

  return order.status;
}

// 🔥 BackendRunTable - Displays actual backend run data
function BackendRunTable({ runs }: { runs: BackendRunInfo[] }) {
  const labelColors: Record<string, string> = {
    VIEWS: "text-yellow-400",
    LIKES: "text-pink-400",
    SHARES: "text-blue-400",
    SAVES: "text-purple-400",
    COMMENTS: "text-green-400",
  };

  const statusColors: Record<string, string> = {
    completed: "text-emerald-400",
    failed: "text-red-400",
    cancelled: "text-red-400",
    processing: "text-yellow-400",
    queued: "text-amber-400",
    pending: "text-gray-400",
    paused: "text-orange-400",
  };

  const statusIcons: Record<string, string> = {
    completed: "✅",
    failed: "❌",
    cancelled: "🚫",
    processing: "⚡",
    queued: "⏳",
    pending: "🕐",
    paused: "⏸️",
  };

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-[10px]">
        <thead>
          <tr className="border-b border-gray-800 text-gray-600 uppercase tracking-wider">
            <th className="pb-2 pr-3">Type</th>
            <th className="pb-2 pr-3">Qty</th>
            <th className="pb-2 pr-3">Scheduled</th>
            <th className="pb-2 pr-3">Status</th>
            <th className="pb-2 pr-3">SMM ID</th>
            <th className="pb-2">Executed</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-900">
          {runs.map((run, index) => {
            const scheduledTime = new Date(run.time);
            const executedTime = run.executedAt ? new Date(run.executedAt) : null;
            const isCompleted = run.status === "completed";
            const isFailed = run.status === "failed" || run.status === "cancelled";

            return (
              <tr key={`${run.id}-${index}`} className="hover:bg-gray-900/50">
                <td className="py-1.5 pr-3">
                  <span
                    className={`font-semibold ${labelColors[run.label] || "text-gray-400"}`}
                  >
                    {run.label}
                  </span>
                </td>
                <td className="py-1.5 pr-3 text-gray-300 font-mono">
                  {run.quantity.toLocaleString()}
                </td>
                <td className="py-1.5 pr-3 text-gray-500">
                  <span title={scheduledTime.toLocaleString()}>
                    {scheduledTime.toLocaleDateString()}{" "}
                    {scheduledTime.toLocaleTimeString([], {
                      hour: "2-digit",
                      minute: "2-digit",
                    })}
                  </span>
                </td>
                <td className="py-1.5 pr-3">
                  <span
                    className={`flex items-center gap-1 ${statusColors[run.status] || "text-gray-400"}`}
                  >
                    <span>{statusIcons[run.status] || "❓"}</span>
                    <span className="capitalize">{run.status}</span>
                  </span>
                  {run.error && (
                    <span
                      className="block text-red-400/70 text-[9px] mt-0.5 max-w-[150px] truncate"
                      title={run.error}
                    >
                      {run.error}
                    </span>
                  )}
                </td>
                <td className="py-1.5 pr-3">
                  {isCompleted && run.smmOrderId ? (
                    <span className="font-mono text-emerald-400">
                      #{run.smmOrderId}
                    </span>
                  ) : (
                    <span className="text-gray-700">—</span>
                  )}
                </td>
                <td className="py-1.5">
                  {executedTime ? (
                    <span className="text-gray-500">
                      {executedTime.toLocaleTimeString([], {
                        hour: "2-digit",
                        minute: "2-digit",
                      })}
                    </span>
                  ) : (
                    <span className="text-gray-700">—</span>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function OrdersPage({
  orders,
  notice,
  controllingOrderId,
  onControlOrder,
  onCloneOrder,
  onDismissNotice,
}: OrdersPageProps) {
  const [query, setQuery] = useState("");
  const [viewMode, setViewMode] = useState<ViewMode>("rows");
  const [activeTab, setActiveTab] = useState<TabType>("running");
  const [openedGroupId, setOpenedGroupId] = useState<string | null>(null);
  const openedGroupIdRef = useRef<string | null>(null);

  useEffect(() => {
    openedGroupIdRef.current = openedGroupId;
  }, [openedGroupId]);

  function getProgress(order: CreatedOrder) {
    const safeRuns = order.runs || [];
    const totalRuns = safeRuns.length;
    if (totalRuns === 0) return { percent: 0, completed: 0, total: 0 };

    const now = Date.now();
    const statusCompleted = (order.runStatuses || []).filter(
      (s) => s === "completed"
    ).length;
    const completed = Math.min(
      totalRuns,
      Math.max(order.completedRuns || 0, statusCompleted)
    );

    return {
      percent: Math.round((completed / totalRuns) * 100),
      completed,
      total: totalRuns,
    };
  }

  function getGroupProgress(group: GroupedOrder) {
    let completedCount = 0;
    let totalCount = 0;

    group.orders.forEach((order) => {
      const progress = getProgress(order);
      completedCount += progress.completed;
      totalCount += progress.total;
    });

    return {
      percent: totalCount > 0 ? Math.round((completedCount / totalCount) * 100) : 0,
      completed: completedCount,
      total: totalCount,
    };
  }

  function getGroupStatus(group: GroupedOrder): string {
    const statuses = group.orders.map((o) => getRealStatus(o));
    if (statuses.every((s) => s === "cancelled" || s === "failed")) return "cancelled";
    if (statuses.every((s) => s === "completed")) return "completed";
    if (statuses.every((s) => s === "scheduled")) return "scheduled";
    if (statuses.some((s) => s === "failed")) return "failed";
    if (statuses.some((s) => s === "paused")) return "paused";
    if (statuses.some((s) => s === "running")) return "running";
    return "running";
  }

  function getGroupCategory(group: GroupedOrder): TabType {
    const status = getGroupStatus(group);
    if (status === "cancelled" || status === "failed") return "cancelled";
    if (status === "completed") return "completed";
    if (status === "scheduled") return "scheduled";
    return "running";
  }

  function toShortLink(link: string) {
    if (!link) return "-";
    return link.length > 48 ? `${link.slice(0, 30)}...${link.slice(-12)}` : link;
  }

  function extractReelId(link: string) {
    const match = link.match(/\/reel\/([^/?]+)/);
    return match ? match[1] : link.slice(-15);
  }

  const groupedOrders = useMemo(() => {
    const groups: Map<string, GroupedOrder> = new Map();

    orders.forEach((order) => {
      const groupKey = order.batchId || order.id;

      if (groups.has(groupKey)) {
        const existing = groups.get(groupKey)!;
        existing.orders.push(order);
        existing.totalViews += order.totalViews;
        existing.linksCount += 1;
      } else {
        groups.set(groupKey, {
          id: groupKey,
          batchId: order.batchId || null,
          name: order.name,
          orders: [order],
          isBatch: !!order.batchId,
          totalViews: order.totalViews,
          linksCount: 1,
          createdAt: order.createdAt,
        });
      }
    });

    groups.forEach((group) => {
      group.orders.sort((a, b) => (a.batchIndex || 0) - (b.batchIndex || 0));
    });

    return Array.from(groups.values());
  }, [orders]);

  const categorizedGroups = useMemo(() => {
    const running: GroupedOrder[] = [];
    const completed: GroupedOrder[] = [];
    const scheduled: GroupedOrder[] = [];
    const cancelled: GroupedOrder[] = [];

    groupedOrders.forEach((group) => {
      const category = getGroupCategory(group);
      if (category === "running") running.push(group);
      else if (category === "completed") completed.push(group);
      else if (category === "scheduled") scheduled.push(group);
      else if (category === "cancelled") cancelled.push(group);
    });

    running.sort(
      (a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
    );
    completed.sort(
      (a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
    );
    scheduled.sort(
      (a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime()
    );
    cancelled.sort(
      (a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
    );

    return { running, completed, scheduled, cancelled };
  }, [groupedOrders]);

  const filteredGroups = useMemo(() => {
    const groupsForTab = categorizedGroups[activeTab];
    const value = query.trim().toLowerCase();
    if (!value) return groupsForTab;

    return groupsForTab.filter(
      (group) =>
        group.name.toLowerCase().includes(value) ||
        group.orders.some(
          (order) =>
            order.link.toLowerCase().includes(value) ||
            order.id.toLowerCase().includes(value)
        )
    );
  }, [categorizedGroups, activeTab, query]);

  const openedGroup = useMemo(
    () => groupedOrders.find((group) => group.id === openedGroupId) ?? null,
    [groupedOrders, openedGroupId]
  );

  useEffect(() => {
    if (!openedGroupId) return;
    const stillExists = groupedOrders.some((group) => group.id === openedGroupId);
    if (!stillExists) setOpenedGroupId(null);
  }, [groupedOrders, openedGroupId]);

  function StatusBadge({ status }: { status: string }) {
    const colors = STATUS_COLORS[status] || STATUS_COLORS.pending;
    return (
      <span
        className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium ${colors.bg} ${colors.text}`}
      >
        <span
          className={`h-1.5 w-1.5 rounded-full ${colors.dot} ${status === "running" ? "animate-pulse" : ""}`}
        />
        {status.charAt(0).toUpperCase() + status.slice(1)}
      </span>
    );
  }

  function ProgressBar({
    percent,
    size = "normal",
  }: {
    percent: number;
    size?: "small" | "normal";
  }) {
    const height = size === "small" ? "h-1" : "h-1.5";
    const getColor = () => {
      if (percent === 100) return "bg-emerald-500";
      if (percent > 50) return "bg-yellow-500";
      return "bg-yellow-600";
    };
    return (
      <div className={`w-full overflow-hidden rounded-full bg-gray-800 ${height}`}>
        <div
          className={`${height} rounded-full transition-all duration-500 ${getColor()}`}
          style={{ width: `${percent}%` }}
        />
      </div>
    );
  }

  function EmptyState({ tab }: { tab: TabType }) {
    const messages = {
      running: { title: "No active missions", description: "Missions in progress will appear here" },
      completed: { title: "No completed missions", description: "Finished missions will appear here" },
      scheduled: { title: "No scheduled missions", description: "Future missions will appear here" },
      cancelled: { title: "No cancelled missions", description: "Cancelled & failed missions will appear here" },
    };
    const icons = { running: "⚡", completed: "✅", scheduled: "📅", cancelled: "🗑️" };

    return (
      <div className="flex flex-col items-center justify-center rounded-xl border border-dashed border-yellow-500/30 bg-black py-16">
        <span className="text-4xl">{icons[tab]}</span>
        <p className="mt-4 text-sm font-medium text-yellow-400">{messages[tab].title}</p>
        <p className="mt-1 text-xs text-gray-600">{messages[tab].description}</p>
      </div>
    );
  }

  function StatsSummary() {
    const stats = [
      { label: "Active", count: categorizedGroups.running.length, color: "text-yellow-400", icon: "⚡" },
      { label: "Scheduled", count: categorizedGroups.scheduled.length, color: "text-amber-400", icon: "⏱" },
      { label: "Completed", count: categorizedGroups.completed.length, color: "text-emerald-400", icon: "✅" },
      { label: "Cancelled", count: categorizedGroups.cancelled.length, color: "text-red-400", icon: "❌" },
    ];

    return (
      <div className="grid grid-cols-4 gap-3">
        {stats.map((stat) => (
          <div
            key={stat.label}
            className="rounded-lg border border-yellow-500/20 bg-black px-4 py-3 text-center"
          >
            <div className="flex items-center justify-center gap-1">
              <span className="text-sm">{stat.icon}</span>
              <p className={`text-2xl font-bold ${stat.color}`}>{stat.count}</p>
            </div>
            <p className="mt-1 text-xs text-gray-600">{stat.label}</p>
          </div>
        ))}
      </div>
    );
  }

  function GroupTableRow({ group }: { group: GroupedOrder }) {
    const progress = getGroupProgress(group);
    const status = getGroupStatus(group);

    return (
      <tr
        onClick={() => setOpenedGroupId(group.id)}
        className="cursor-pointer border-t border-gray-800 transition hover:bg-yellow-500/5"
      >
        <td className="px-4 py-3">
          <div className="flex items-center gap-2">
            <p className="font-medium text-white">
              {group.name || `Mission #${group.id.slice(0, 8)}`}
            </p>
            {group.isBatch && (
              <span className="rounded-full bg-blue-500/20 border border-blue-500/30 px-2 py-0.5 text-[10px] text-blue-300">
                📦 {group.linksCount} links
              </span>
            )}
          </div>
          <p className="mt-0.5 text-[11px] text-gray-600 font-mono">
            {group.isBatch
              ? group.batchId?.slice(0, 15)
              : group.orders[0]?.id}
          </p>
        </td>
        <td className="max-w-[220px] px-4 py-3">
          {group.isBatch ? (
            <p className="text-gray-500 text-xs">{group.linksCount} Instagram links</p>
          ) : (
            <p className="truncate text-gray-500" title={group.orders[0]?.link}>
              {toShortLink(group.orders[0]?.link || "")}
            </p>
          )}
        </td>
        <td className="px-4 py-3">
          <StatusBadge status={status} />
        </td>
        <td className="px-4 py-3">
          <div className="w-32">
            <div className="flex items-center justify-between mb-1">
              <span className="text-[11px] text-gray-600">
                {progress.completed}/{progress.total} runs
              </span>
              <span className="text-[11px] font-medium text-gray-500">
                {progress.percent}%
              </span>
            </div>
            <ProgressBar percent={progress.percent} />
          </div>
        </td>
        <td className="px-4 py-3 text-gray-600 text-xs">
          {new Date(group.createdAt).toLocaleDateString()}
          <span className="block text-gray-700">
            {new Date(group.createdAt).toLocaleTimeString()}
          </span>
        </td>
      </tr>
    );
  }

  function GroupCardItem({ group }: { group: GroupedOrder }) {
    const progress = getGroupProgress(group);
    const status = getGroupStatus(group);
    const isCancelled = status === "cancelled" || status === "failed";

    return (
      <button
        type="button"
        onClick={() => setOpenedGroupId(group.id)}
        className={`group rounded-xl border bg-gradient-to-br from-gray-900 to-black p-4 text-left transition-all hover:shadow-lg w-full ${
          isCancelled
            ? "border-red-500/20 hover:border-red-500/40 hover:shadow-red-500/5"
            : "border-yellow-500/20 hover:border-yellow-500/40 hover:shadow-yellow-500/5"
        }`}
      >
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <p
                className={`truncate text-sm font-semibold ${isCancelled ? "text-red-200" : "text-white"} group-hover:text-yellow-100`}
              >
                {group.name || `Mission #${group.id.slice(0, 8)}`}
              </p>
              {group.isBatch && (
                <span className="rounded-full bg-blue-500/20 border border-blue-500/30 px-1.5 py-0.5 text-[9px] text-blue-300">
                  📦 {group.linksCount}
                </span>
              )}
            </div>
            <p className="mt-1 truncate text-xs text-gray-600 font-mono">
              {group.isBatch
                ? `Batch: ${group.linksCount} links`
                : group.orders[0]?.id}
            </p>
          </div>
          <StatusBadge status={status} />
        </div>

        {!group.isBatch && (
          <p
            className="mt-3 truncate text-xs text-gray-500"
            title={group.orders[0]?.link}
          >
            {toShortLink(group.orders[0]?.link || "")}
          </p>
        )}

        {group.isBatch && (
          <div className="mt-3 flex flex-wrap gap-1">
            {group.orders.slice(0, 3).map((order) => (
              <span
                key={order.id}
                className={`rounded px-1.5 py-0.5 text-[9px] ${
                  getRealStatus(order) === "cancelled" ||
                  getRealStatus(order) === "failed"
                    ? "bg-red-900/50 text-red-400"
                    : "bg-gray-800 text-gray-400"
                }`}
              >
                {extractReelId(order.link)}
              </span>
            ))}
            {group.orders.length > 3 && (
              <span className="rounded bg-gray-800 px-1.5 py-0.5 text-[9px] text-gray-500">
                +{group.orders.length - 3} more
              </span>
            )}
          </div>
        )}

        <div className="mt-4">
          <div className="flex items-center justify-between text-xs mb-1.5">
            <span className="text-gray-600">Progress</span>
            <span className="text-gray-500">
              {progress.completed}/{progress.total} ({progress.percent}%)
            </span>
          </div>
          <ProgressBar percent={progress.percent} />
        </div>

        <div className="mt-3 flex items-center justify-between text-[11px] text-gray-600">
          <span>{isCancelled ? "Cancelled" : "Deployed"}</span>
          <span>{new Date(group.createdAt).toLocaleDateString()}</span>
        </div>
      </button>
    );
  }

  // 🔥 FIXED: IndividualLinkCard - uses backendRuns for actual run data
  function IndividualLinkCard({
    order,
    index,
  }: {
    order: CreatedOrder;
    index: number;
  }) {
    const [showRuns, setShowRuns] = useState(false);
    const progress = getProgress(order);
    const status = getRealStatus(order);
    const isControlling = controllingOrderId === order.id;
    const isCancelled = status === "cancelled" || status === "failed";

    // 🔥 FIXED: Prefer backendRuns (real data) over frontend runs for display
    const hasBackendRuns =
      order.backendRuns && order.backendRuns.length > 0;
    const displayRunCount = hasBackendRuns
      ? order.backendRuns!.length
      : order.runs?.length || 0;

    return (
      <motion.div
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: index * 0.05 }}
        className={`rounded-xl border bg-gradient-to-br from-gray-900 to-black p-4 ${
          isCancelled ? "border-red-500/30" : "border-gray-800"
        }`}
      >
        {/* Header */}
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <span
                className={`flex items-center justify-center h-6 w-6 rounded-full text-xs font-bold ${
                  isCancelled
                    ? "bg-red-500/20 text-red-400"
                    : "bg-yellow-500/20 text-yellow-400"
                }`}
              >
                {index + 1}
              </span>
              <a
                href={order.link}
                target="_blank"
                rel="noopener noreferrer"
                className={`truncate text-sm hover:underline ${
                  isCancelled
                    ? "text-red-400 hover:text-red-300"
                    : "text-blue-400 hover:text-blue-300"
                }`}
                onClick={(e) => e.stopPropagation()}
              >
                {toShortLink(order.link)}
              </a>
            </div>
            <p className="mt-1 ml-8 text-[10px] text-gray-600 font-mono">
              {order.id}
            </p>
            {order.schedulerOrderId && (
              <p className="ml-8 text-[9px] text-gray-700 font-mono">
                Scheduler: {order.schedulerOrderId}
              </p>
            )}
          </div>
          <StatusBadge status={status} />
        </div>

        {/* Error Message */}
        {order.errorMessage && (
          <div className="mt-2 ml-8 rounded-md bg-red-500/10 border border-red-500/20 px-2 py-1">
            <p className="text-[10px] text-red-400">❌ {order.errorMessage}</p>
          </div>
        )}

        {/* Progress */}
        <div className="mt-3 ml-8">
          <div className="flex items-center justify-between text-xs mb-1">
            <span className="text-gray-600">
              {progress.completed}/{progress.total} runs
            </span>
            <span className="text-gray-500">{progress.percent}%</span>
          </div>
          <ProgressBar percent={progress.percent} size="small" />
        </div>

        {/* Stats */}
        <div className="mt-3 ml-8 grid grid-cols-5 gap-2">
          <div className="rounded-md bg-black/50 px-2 py-1 text-center">
            <p className="text-xs font-medium text-yellow-400">
              {(order.totalViews / 1000).toFixed(0)}k
            </p>
            <p className="text-[9px] text-gray-600">Views</p>
          </div>
          <div className="rounded-md bg-black/50 px-2 py-1 text-center">
            <p className="text-xs font-medium text-pink-400">
              {order.engagement.likes}
            </p>
            <p className="text-[9px] text-gray-600">Likes</p>
          </div>
          <div className="rounded-md bg-black/50 px-2 py-1 text-center">
            <p className="text-xs font-medium text-blue-400">
              {order.engagement.shares}
            </p>
            <p className="text-[9px] text-gray-600">Shares</p>
          </div>
          <div className="rounded-md bg-black/50 px-2 py-1 text-center">
            <p className="text-xs font-medium text-purple-400">
              {order.engagement.saves}
            </p>
            <p className="text-[9px] text-gray-600">Saves</p>
          </div>
          {/* 🔥 FIXED: Added comments column */}
          <div className="rounded-md bg-black/50 px-2 py-1 text-center">
            <p className="text-xs font-medium text-green-400">
              {order.engagement.comments || 0}
            </p>
            <p className="text-[9px] text-gray-600">Comments</p>
          </div>
        </div>

        {/* Controls */}
        <div className="mt-3 ml-8 flex items-center gap-2 flex-wrap">
          {!isCancelled && status === "running" && (
            <button
              onClick={(e) => {
                e.stopPropagation();
                onControlOrder(order, "pause");
              }}
              disabled={isControlling}
              className="flex items-center gap-1 rounded-md border border-orange-500/30 bg-orange-500/10 px-2 py-1 text-[10px] font-medium text-orange-300 hover:bg-orange-500/20 transition disabled:opacity-50"
            >
              {isControlling ? "⏳" : "⏸️"} Pause
            </button>
          )}
          {!isCancelled && status === "paused" && (
            <button
              onClick={(e) => {
                e.stopPropagation();
                onControlOrder(order, "resume");
              }}
              disabled={isControlling}
              className="flex items-center gap-1 rounded-md border border-emerald-500/30 bg-emerald-500/10 px-2 py-1 text-[10px] font-medium text-emerald-300 hover:bg-emerald-500/20 transition disabled:opacity-50"
            >
              {isControlling ? "⏳" : "▶️"} Resume
            </button>
          )}
          {!isCancelled && status !== "completed" && (
            <button
              onClick={(e) => {
                e.stopPropagation();
                if (
                  window.confirm(
                    `Cancel this order?\n\nLink: ${order.link.slice(0, 50)}...`
                  )
                ) {
                  onControlOrder(order, "cancel");
                }
              }}
              disabled={isControlling}
              className="flex items-center gap-1 rounded-md border border-red-500/30 bg-red-500/10 px-2 py-1 text-[10px] font-medium text-red-300 hover:bg-red-500/20 transition disabled:opacity-50"
            >
              {isControlling ? "⏳" : "❌"} Cancel
            </button>
          )}
          <button
            onClick={(e) => {
              e.stopPropagation();
              onCloneOrder(order);
            }}
            className="flex items-center gap-1 rounded-md border border-gray-600 bg-black px-2 py-1 text-[10px] font-medium text-gray-400 hover:text-white hover:border-gray-500 transition"
          >
            📋 Clone
          </button>
          <a
            href={order.link}
            target="_blank"
            rel="noopener noreferrer"
            onClick={(e) => e.stopPropagation()}
            className="flex items-center gap-1 rounded-md border border-gray-600 bg-black px-2 py-1 text-[10px] font-medium text-gray-400 hover:text-white hover:border-gray-500 transition"
          >
            🔗 Open
          </a>

          {/* View Runs Toggle */}
          <button
            onClick={(e) => {
              e.stopPropagation();
              setShowRuns(!showRuns);
            }}
            className={`flex items-center gap-1 rounded-md border px-2 py-1 text-[10px] font-medium transition ml-auto ${
              showRuns
                ? "border-yellow-500/50 bg-yellow-500/20 text-yellow-300"
                : "border-yellow-500/30 bg-yellow-500/10 text-yellow-400 hover:bg-yellow-500/20"
            }`}
          >
            {showRuns ? "🔼 Hide Runs" : `📋 View Runs (${displayRunCount})`}
          </button>
        </div>

        {/* 🔥 FIXED: Run List - shows backend runs if available, otherwise frontend runs */}
        <AnimatePresence>
          {showRuns && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: "auto" }}
              exit={{ opacity: 0, height: 0 }}
              className="mt-4 ml-8 overflow-hidden"
            >
              <div className="rounded-lg border border-yellow-500/20 bg-black/50 p-3">
                <div className="flex items-center justify-between mb-3">
                  <h4 className="text-xs font-semibold text-yellow-400">
                    📋 Run Schedule
                    {hasBackendRuns && (
                      <span className="ml-2 text-[9px] text-emerald-400">
                        ✅ Live from backend
                      </span>
                    )}
                  </h4>
                  <span className="text-[10px] text-gray-600">
                    {progress.completed} completed
                  </span>
                </div>

                {/* 🔥 FIXED: Show BackendRunTable if we have backend data */}
                {hasBackendRuns ? (
                  <BackendRunTable runs={order.backendRuns!} />
                ) : order.runs && order.runs.length > 0 ? (
                  // Fallback: show frontend scheduled runs
                  <div className="overflow-x-auto">
                    <table className="w-full text-left text-[10px]">
                      <thead>
                        <tr className="border-b border-gray-800 text-gray-600 uppercase tracking-wider">
                          <th className="pb-2 pr-3">#</th>
                          <th className="pb-2 pr-3">Scheduled Time</th>
                          <th className="pb-2 pr-3">Views</th>
                          <th className="pb-2 pr-3">Likes</th>
                          <th className="pb-2 pr-3">Shares</th>
                          <th className="pb-2 pr-3">Saves</th>
                          <th className="pb-2">Status</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-900">
                        {order.runs.map((run, i) => {
                          const runTime =
                            run.at instanceof Date
                              ? run.at
                              : new Date(run.at);
                          const isPast = runTime.getTime() <= Date.now();
                          const runStatus =
                            order.runStatuses?.[i] || "pending";

                          return (
                            <tr key={i} className="hover:bg-gray-900/50">
                              <td className="py-1.5 pr-3 text-gray-500">
                                {i + 1}
                              </td>
                              <td className="py-1.5 pr-3 text-gray-400">
                                {runTime.toLocaleDateString()}{" "}
                                {runTime.toLocaleTimeString([], {
                                  hour: "2-digit",
                                  minute: "2-digit",
                                })}
                              </td>
                              <td className="py-1.5 pr-3 text-yellow-400">
                                {(run.views || 0).toLocaleString()}
                              </td>
                              <td className="py-1.5 pr-3 text-pink-400">
                                {run.likes || 0}
                              </td>
                              <td className="py-1.5 pr-3 text-blue-400">
                                {run.shares || 0}
                              </td>
                              <td className="py-1.5 pr-3 text-purple-400">
                                {run.saves || 0}
                              </td>
                              <td className="py-1.5">
                                {runStatus === "completed" ? (
                                  <span className="text-emerald-400">✅</span>
                                ) : runStatus === "cancelled" ? (
                                  <span className="text-red-400">🚫</span>
                                ) : isPast ? (
                                  <span className="text-yellow-400">⚡</span>
                                ) : (
                                  <span className="text-gray-500">🕐</span>
                                )}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="rounded-lg border border-dashed border-gray-700 bg-black/30 p-4 text-center">
                    <p className="text-xs text-gray-500">
                      No runs data available. Sync will populate this.
                    </p>
                  </div>
                )}
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </motion.div>
    );
  }

  // Batch Detail Popup
  function BatchDetailPopup({ group }: { group: GroupedOrder }) {
    const overallProgress = getGroupProgress(group);
    const overallStatus = getGroupStatus(group);
    const isCancelled = overallStatus === "cancelled" || overallStatus === "failed";

    const statusCounts = useMemo(() => {
      const counts: Record<string, number> = {};
      group.orders.forEach((order) => {
        const status = getRealStatus(order);
        counts[status] = (counts[status] || 0) + 1;
      });
      return counts;
    }, [group.orders]);

    const totalRunsInBatch = useMemo(() => {
      return group.orders.reduce(
        (sum, order) => sum + (order.backendRuns?.length || order.runs?.length || 0),
        0
      );
    }, [group.orders]);

    return (
      <div
        className="fixed inset-0 z-50 flex items-center justify-center bg-black/90 backdrop-blur-sm px-4 py-6"
        onClick={() => setOpenedGroupId(null)}
      >
        <motion.div
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          className={`max-h-[92vh] w-full max-w-4xl overflow-hidden rounded-2xl border bg-black shadow-2xl flex flex-col ${
            isCancelled ? "border-red-500/30" : "border-yellow-500/30"
          }`}
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <div className="border-b border-gray-800 px-5 py-4 flex-shrink-0">
            <div className="flex items-center justify-between">
              <div>
                <div className="flex items-center gap-2">
                  <h3
                    className={`text-lg font-semibold ${isCancelled ? "text-red-400" : "text-yellow-400"}`}
                  >
                    {group.name}
                  </h3>
                  {group.isBatch && (
                    <span className="rounded-full bg-blue-500/20 border border-blue-500/30 px-2 py-0.5 text-xs text-blue-300">
                      📦 Bulk Order
                    </span>
                  )}
                  {isCancelled && (
                    <span className="rounded-full bg-red-500/20 border border-red-500/30 px-2 py-0.5 text-xs text-red-300">
                      ❌ Cancelled
                    </span>
                  )}
                </div>
                <p className="mt-0.5 text-xs text-gray-600 font-mono">
                  {group.batchId || group.id}
                </p>
              </div>
              <button
                type="button"
                onClick={() => setOpenedGroupId(null)}
                className="rounded-lg border border-yellow-500/30 bg-yellow-500/10 px-4 py-2 text-sm text-yellow-300 transition hover:bg-yellow-500/20"
              >
                ✕ Close
              </button>
            </div>

            {/* Overall Stats */}
            <div className="mt-4 grid grid-cols-2 sm:grid-cols-5 gap-3">
              <div className="rounded-lg bg-gray-900 px-3 py-2 text-center">
                <p className="text-xl font-bold text-yellow-400">
                  {group.linksCount}
                </p>
                <p className="text-[10px] text-gray-500">Total Links</p>
              </div>
              <div className="rounded-lg bg-gray-900 px-3 py-2 text-center">
                <p className="text-xl font-bold text-yellow-400">
                  {(group.totalViews / 1000).toFixed(0)}k
                </p>
                <p className="text-[10px] text-gray-500">Total Views</p>
              </div>
              <div className="rounded-lg bg-gray-900 px-3 py-2 text-center">
                <p className="text-xl font-bold text-blue-400">
                  {totalRunsInBatch}
                </p>
                <p className="text-[10px] text-gray-500">Total Runs</p>
              </div>
              <div className="rounded-lg bg-gray-900 px-3 py-2 text-center">
                <p
                  className={`text-xl font-bold ${isCancelled ? "text-red-400" : "text-emerald-400"}`}
                >
                  {overallProgress.percent}%
                </p>
                <p className="text-[10px] text-gray-500">Progress</p>
              </div>
              <div className="rounded-lg bg-gray-900 px-3 py-2 text-center">
                <StatusBadge status={overallStatus} />
              </div>
            </div>

            {/* Status Summary */}
            <div className="mt-3 flex flex-wrap gap-2">
              {Object.entries(statusCounts).map(([status, count]) => (
                <div key={status} className="flex items-center gap-1 text-xs">
                  <span
                    className={`h-2 w-2 rounded-full ${STATUS_COLORS[status]?.dot || "bg-gray-500"}`}
                  />
                  <span className="text-gray-400">
                    {count} {status}
                  </span>
                </div>
              ))}
            </div>

            {/* Bulk Actions */}
            {!isCancelled && (
              <div className="mt-4 flex flex-wrap gap-2">
                <button
                  onClick={() => {
                    const runningCount = group.orders.filter(
                      (o) => getRealStatus(o) === "running"
                    ).length;
                    if (
                      runningCount > 0 &&
                      window.confirm(`Pause ALL ${runningCount} running orders?`)
                    ) {
                      group.orders.forEach((order) => {
                        if (getRealStatus(order) === "running") {
                          onControlOrder(order, "pause");
                        }
                      });
                    }
                  }}
                  className="flex items-center gap-1 rounded-lg border border-orange-500/30 bg-orange-500/10 px-3 py-1.5 text-xs font-medium text-orange-300 hover:bg-orange-500/20 transition"
                >
                  ⏸️ Pause All Running
                </button>
                <button
                  onClick={() => {
                    const pausedCount = group.orders.filter(
                      (o) => getRealStatus(o) === "paused"
                    ).length;
                    if (
                      pausedCount > 0 &&
                      window.confirm(`Resume ALL ${pausedCount} paused orders?`)
                    ) {
                      group.orders.forEach((order) => {
                        if (getRealStatus(order) === "paused") {
                          onControlOrder(order, "resume");
                        }
                      });
                    }
                  }}
                  className="flex items-center gap-1 rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-3 py-1.5 text-xs font-medium text-emerald-300 hover:bg-emerald-500/20 transition"
                >
                  ▶️ Resume All Paused
                </button>
                <button
                  onClick={() => {
                    const activeCount = group.orders.filter(
                      (o) =>
                        !["completed", "cancelled", "failed"].includes(
                          getRealStatus(o)
                        )
                    ).length;
                    if (
                      activeCount > 0 &&
                      window.confirm(
                        `⚠️ Cancel ALL ${activeCount} active orders?\n\nThis cannot be undone!`
                      )
                    ) {
                      group.orders.forEach((order) => {
                        const status = getRealStatus(order);
                        if (
                          status !== "completed" &&
                          status !== "cancelled" &&
                          status !== "failed"
                        ) {
                          onControlOrder(order, "cancel");
                        }
                      });
                    }
                  }}
                  className="flex items-center gap-1 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-1.5 text-xs font-medium text-red-300 hover:bg-red-500/20 transition"
                >
                  ❌ Cancel All Active
                </button>
              </div>
            )}
          </div>

          {/* Individual Links List */}
          <div className="flex-1 overflow-y-auto p-5">
            <h4 className="text-sm font-semibold text-gray-400 mb-3">
              📋 Individual Links ({group.orders.length})
            </h4>
            <div className="space-y-3">
              {group.orders.map((order, index) => (
                <IndividualLinkCard
                  key={order.id}
                  order={order}
                  index={index}
                />
              ))}
            </div>
          </div>
        </motion.div>
      </div>
    );
  }

  // Single Order Popup
  function SingleOrderPopup({ order }: { order: CreatedOrder }) {
    const [showRuns, setShowRuns] = useState(false);
    const status = getRealStatus(order);
    const progress = getProgress(order);
    const isControlling = controllingOrderId === order.id;
    const isCancelled = status === "cancelled" || status === "failed";
    const hasBackendRuns = order.backendRuns && order.backendRuns.length > 0;

    return (
      <div
        className="fixed inset-0 z-50 flex items-center justify-center bg-black/90 backdrop-blur-sm px-4 py-6"
        onClick={() => setOpenedGroupId(null)}
      >
        <div
          className="max-h-[92vh] w-full max-w-4xl overflow-auto rounded-2xl border border-yellow-500/30 bg-black shadow-2xl flex flex-col"
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <div className="border-b border-gray-800 px-5 py-4 flex-shrink-0">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-lg font-semibold text-yellow-400">
                  {order.name || "Mission Details"}
                </h3>
                <p className="mt-0.5 text-xs text-gray-600 font-mono">{order.id}</p>
                {order.schedulerOrderId && (
                  <p className="text-[10px] text-gray-700 font-mono">
                    Backend: {order.schedulerOrderId}
                  </p>
                )}
              </div>
              <button
                type="button"
                onClick={() => setOpenedGroupId(null)}
                className="rounded-lg border border-yellow-500/30 bg-yellow-500/10 px-4 py-2 text-sm text-yellow-300 transition hover:bg-yellow-500/20"
              >
                ✕ Close
              </button>
            </div>

            {/* Stats Row */}
            <div className="mt-4 grid grid-cols-2 sm:grid-cols-4 gap-3">
              <div className="rounded-lg bg-gray-900 px-3 py-2 text-center">
                <p className="text-lg font-bold text-yellow-400">
                  {(order.totalViews / 1000).toFixed(0)}k
                </p>
                <p className="text-[10px] text-gray-500">Views</p>
              </div>
              <div className="rounded-lg bg-gray-900 px-3 py-2 text-center">
                <p className="text-lg font-bold text-white">
                  {progress.completed}/{progress.total}
                </p>
                <p className="text-[10px] text-gray-500">Runs</p>
              </div>
              <div className="rounded-lg bg-gray-900 px-3 py-2 text-center">
                <p className="text-lg font-bold text-emerald-400">
                  {progress.percent}%
                </p>
                <p className="text-[10px] text-gray-500">Progress</p>
              </div>
              <div className="rounded-lg bg-gray-900 px-3 py-2 text-center">
                <StatusBadge status={status} />
              </div>
            </div>

            {/* Engagement Row */}
            <div className="mt-3 grid grid-cols-4 gap-2">
              <div className="rounded-md bg-black/50 px-2 py-1 text-center">
                <p className="text-xs font-medium text-pink-400">
                  {order.engagement.likes}
                </p>
                <p className="text-[9px] text-gray-600">Likes</p>
              </div>
              <div className="rounded-md bg-black/50 px-2 py-1 text-center">
                <p className="text-xs font-medium text-blue-400">
                  {order.engagement.shares}
                </p>
                <p className="text-[9px] text-gray-600">Shares</p>
              </div>
              <div className="rounded-md bg-black/50 px-2 py-1 text-center">
                <p className="text-xs font-medium text-purple-400">
                  {order.engagement.saves}
                </p>
                <p className="text-[9px] text-gray-600">Saves</p>
              </div>
              <div className="rounded-md bg-black/50 px-2 py-1 text-center">
                <p className="text-xs font-medium text-green-400">
                  {order.engagement.comments || 0}
                </p>
                <p className="text-[9px] text-gray-600">Comments</p>
              </div>
            </div>

            {/* Progress Bar */}
            <div className="mt-3">
              <ProgressBar percent={progress.percent} />
            </div>

            {/* Controls */}
            <div className="mt-4 flex flex-wrap gap-2">
              {!isCancelled && status === "running" && (
                <button
                  onClick={() => onControlOrder(order, "pause")}
                  disabled={isControlling}
                  className="flex items-center gap-1 rounded-lg border border-orange-500/30 bg-orange-500/10 px-3 py-1.5 text-xs font-medium text-orange-300 hover:bg-orange-500/20 transition disabled:opacity-50"
                >
                  {isControlling ? "⏳" : "⏸️"} Pause
                </button>
              )}
              {!isCancelled && status === "paused" && (
                <button
                  onClick={() => onControlOrder(order, "resume")}
                  disabled={isControlling}
                  className="flex items-center gap-1 rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-3 py-1.5 text-xs font-medium text-emerald-300 hover:bg-emerald-500/20 transition disabled:opacity-50"
                >
                  {isControlling ? "⏳" : "▶️"} Resume
                </button>
              )}
              {!isCancelled && status !== "completed" && (
                <button
                  onClick={() => {
                    if (window.confirm("Cancel this mission?")) {
                      onControlOrder(order, "cancel");
                    }
                  }}
                  disabled={isControlling}
                  className="flex items-center gap-1 rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-1.5 text-xs font-medium text-red-300 hover:bg-red-500/20 transition disabled:opacity-50"
                >
                  {isControlling ? "⏳" : "❌"} Cancel
                </button>
              )}
              <button
                onClick={() => onCloneOrder(order)}
                className="flex items-center gap-1 rounded-lg border border-gray-600 bg-black px-3 py-1.5 text-xs font-medium text-gray-400 hover:text-white transition"
              >
                📋 Clone Mission
              </button>
              <a
                href={order.link}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-1 rounded-lg border border-gray-600 bg-black px-3 py-1.5 text-xs font-medium text-gray-400 hover:text-white transition"
              >
                🔗 Open Link
              </a>
              <button
                onClick={() => setShowRuns(!showRuns)}
                className={`flex items-center gap-1 rounded-lg border px-3 py-1.5 text-xs font-medium transition ml-auto ${
                  showRuns
                    ? "border-yellow-500/50 bg-yellow-500/20 text-yellow-300"
                    : "border-yellow-500/30 bg-yellow-500/10 text-yellow-400 hover:bg-yellow-500/20"
                }`}
              >
                {showRuns ? "🔼 Hide Runs" : `📋 View Runs`}
              </button>
            </div>
          </div>

          {/* Run Table */}
          <div className="flex-1 overflow-y-auto p-5">
            <AnimatePresence>
              {showRuns && (
                <motion.div
                  initial={{ opacity: 0, height: 0 }}
                  animate={{ opacity: 1, height: "auto" }}
                  exit={{ opacity: 0, height: 0 }}
                  className="overflow-hidden"
                >
                  <div className="rounded-lg border border-yellow-500/20 bg-black/50 p-3">
                    <div className="flex items-center justify-between mb-3">
                      <h4 className="text-xs font-semibold text-yellow-400">
                        📋 Run Schedule
                        {hasBackendRuns && (
                          <span className="ml-2 text-[9px] text-emerald-400">
                            ✅ Live from backend
                          </span>
                        )}
                      </h4>
                    </div>
                    {hasBackendRuns ? (
                      <BackendRunTable runs={order.backendRuns!} />
                    ) : order.runs && order.runs.length > 0 ? (
                      <div className="overflow-x-auto">
                        <table className="w-full text-left text-[10px]">
                          <thead>
                            <tr className="border-b border-gray-800 text-gray-600 uppercase tracking-wider">
                              <th className="pb-2 pr-3">#</th>
                              <th className="pb-2 pr-3">Scheduled Time</th>
                              <th className="pb-2 pr-3">Views</th>
                              <th className="pb-2 pr-3">Likes</th>
                              <th className="pb-2 pr-3">Shares</th>
                              <th className="pb-2 pr-3">Saves</th>
                              <th className="pb-2">Status</th>
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-gray-900">
                            {order.runs.map((run, i) => {
                              const runTime =
                                run.at instanceof Date
                                  ? run.at
                                  : new Date(run.at);
                              const isPast = runTime.getTime() <= Date.now();
                              const runStatus =
                                order.runStatuses?.[i] || "pending";
                              return (
                                <tr key={i} className="hover:bg-gray-900/50">
                                  <td className="py-1.5 pr-3 text-gray-500">
                                    {i + 1}
                                  </td>
                                  <td className="py-1.5 pr-3 text-gray-400">
                                    {runTime.toLocaleDateString()}{" "}
                                    {runTime.toLocaleTimeString([], {
                                      hour: "2-digit",
                                      minute: "2-digit",
                                    })}
                                  </td>
                                  <td className="py-1.5 pr-3 text-yellow-400">
                                    {(run.views || 0).toLocaleString()}
                                  </td>
                                  <td className="py-1.5 pr-3 text-pink-400">
                                    {run.likes || 0}
                                  </td>
                                  <td className="py-1.5 pr-3 text-blue-400">
                                    {run.shares || 0}
                                  </td>
                                  <td className="py-1.5 pr-3 text-purple-400">
                                    {run.saves || 0}
                                  </td>
                                  <td className="py-1.5">
                                    {runStatus === "completed" ? (
                                      <span className="text-emerald-400">✅</span>
                                    ) : runStatus === "cancelled" ? (
                                      <span className="text-red-400">🚫</span>
                                    ) : isPast ? (
                                      <span className="text-yellow-400">⚡</span>
                                    ) : (
                                      <span className="text-gray-500">🕐</span>
                                    )}
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <div className="rounded-lg border border-dashed border-gray-700 p-4 text-center">
                        <p className="text-xs text-gray-500">
                          No run data available. Sync will populate this.
                        </p>
                      </div>
                    )}
                  </div>
                </motion.div>
              )}
            </AnimatePresence>

            {/* Order metadata */}
            <div className="mt-4 grid grid-cols-2 gap-3 text-xs text-gray-500">
              <div>
                <span className="text-gray-600">API: </span>
                <span>{order.selectedAPI}</span>
              </div>
              <div>
                <span className="text-gray-600">Bundle: </span>
                <span>{order.selectedBundle}</span>
              </div>
              <div>
                <span className="text-gray-600">Pattern: </span>
                <span>{order.patternName}</span>
              </div>
              <div>
                <span className="text-gray-600">Created: </span>
                <span>
                  {new Date(order.createdAt).toLocaleDateString()}{" "}
                  {new Date(order.createdAt).toLocaleTimeString([], {
                    hour: "2-digit",
                    minute: "2-digit",
                  })}
                </span>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-7xl space-y-6 px-6 py-8">
      {/* Header */}
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div className="flex items-center gap-3">
            <span className="text-2xl">📦</span>
            <h2 className="text-2xl font-bold tracking-tight text-yellow-400">
              Mission Control
            </h2>
          </div>
          <p className="mt-1 text-sm text-gray-600">
            Track and manage all your operations
          </p>
        </div>
        <div className="flex items-center gap-2 text-xs text-gray-600">
          <span className="inline-flex h-2 w-2 rounded-full bg-yellow-400 animate-pulse" />
          <span>Live monitoring</span>
        </div>
      </div>

      {/* Stats Summary */}
      <StatsSummary />

      {/* Notice */}
      {notice && (
        <div className="flex items-center justify-between rounded-xl border border-emerald-500/40 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-300">
          <div className="flex items-center gap-2">
            <span>✓</span>
            <p>{notice}</p>
          </div>
          <button
            type="button"
            onClick={onDismissNotice}
            className="rounded-lg px-2 py-1 text-emerald-200 hover:bg-emerald-500/20 transition"
          >
            Dismiss
          </button>
        </div>
      )}

      {/* Tabs & Controls */}
      <div className="rounded-xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-4">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex flex-wrap gap-2">
            {TABS.map((tab) => {
              const count = categorizedGroups[tab.key].length;
              const isActive = activeTab === tab.key;
              const isCancelledTab = tab.key === "cancelled";
              return (
                <button
                  key={tab.key}
                  type="button"
                  onClick={() => setActiveTab(tab.key)}
                  className={`inline-flex items-center gap-2 rounded-lg px-4 py-2.5 text-sm font-medium transition-all ${
                    isActive
                      ? isCancelledTab
                        ? "bg-red-500/20 text-red-300 shadow-lg shadow-red-500/10"
                        : "bg-yellow-500/20 text-yellow-300 shadow-lg shadow-yellow-500/10"
                      : "text-gray-500 hover:bg-yellow-500/10 hover:text-yellow-400"
                  }`}
                >
                  <span>{tab.icon}</span>
                  <span>{tab.label}</span>
                  <span
                    className={`ml-1 rounded-full px-2 py-0.5 text-xs ${
                      isActive
                        ? isCancelledTab
                          ? "bg-red-500/30 text-red-100"
                          : "bg-yellow-500/30 text-yellow-100"
                        : "bg-gray-800 text-gray-500"
                    }`}
                  >
                    {count}
                  </span>
                </button>
              );
            })}
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <div className="relative flex-1 min-w-[240px]">
              <span className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-600">
                🔍
              </span>
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search missions..."
                className="w-full rounded-lg border border-yellow-500/30 bg-black py-2.5 pl-10 pr-4 text-sm text-gray-100 outline-none ring-yellow-500/40 transition placeholder:text-gray-700 focus:border-yellow-500/50 focus:ring-2"
              />
              {query && (
                <button
                  type="button"
                  onClick={() => setQuery("")}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-600 hover:text-gray-400"
                >
                  ✕
                </button>
              )}
            </div>

            <div className="inline-flex rounded-lg border border-yellow-500/30 bg-black p-1">
              <button
                type="button"
                onClick={() => setViewMode("rows")}
                className={`rounded-md px-3 py-2 text-xs transition ${
                  viewMode === "rows"
                    ? "bg-yellow-500/20 text-yellow-300"
                    : "text-gray-500 hover:text-yellow-400"
                }`}
                title="Table View"
              >
                ☰ Rows
              </button>
              <button
                type="button"
                onClick={() => setViewMode("columns")}
                className={`rounded-md px-3 py-2 text-xs transition ${
                  viewMode === "columns"
                    ? "bg-yellow-500/20 text-yellow-300"
                    : "text-gray-500 hover:text-yellow-400"
                }`}
                title="Grid View"
              >
                ⊞ Grid
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Results Info */}
      {query && (
        <p className="text-sm text-gray-600">
          Found{" "}
          <span className="text-gray-400 font-medium">{filteredGroups.length}</span>{" "}
          missions matching "
          <span className="text-yellow-400">{query}</span>" in {activeTab}
        </p>
      )}

      {/* Orders Display */}
      {filteredGroups.length === 0 ? (
        <EmptyState tab={activeTab} />
      ) : viewMode === "rows" ? (
        <div className="overflow-hidden rounded-xl border border-yellow-500/20 bg-black">
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs text-gray-400">
              <thead className="bg-gray-900 text-gray-500 uppercase tracking-wider">
                <tr>
                  <th className="px-4 py-3 font-medium">Mission</th>
                  <th className="px-4 py-3 font-medium">Link(s)</th>
                  <th className="px-4 py-3 font-medium">Status</th>
                  <th className="px-4 py-3 font-medium">Progress</th>
                  <th className="px-4 py-3 font-medium">
                    {activeTab === "cancelled" ? "Cancelled" : "Deployed"}
                  </th>
                </tr>
              </thead>
              <tbody>
                {filteredGroups.map((group) => (
                  <GroupTableRow key={group.id} group={group} />
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {filteredGroups.map((group) => (
            <GroupCardItem key={group.id} group={group} />
          ))}
        </div>
      )}

      {/* Detail Popup */}
      <AnimatePresence>
        {openedGroup &&
          (openedGroup.isBatch ? (
            <BatchDetailPopup group={openedGroup} />
          ) : (
            <SingleOrderPopup order={openedGroup.orders[0]} />
          ))}
      </AnimatePresence>
    </div>
  );
}

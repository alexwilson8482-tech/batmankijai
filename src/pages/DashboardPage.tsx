import { useState, useMemo } from "react";
import type { CreatedOrder } from "../types/order";

interface DashboardPageProps {
  orders: CreatedOrder[];
}

type TimePeriod = "today" | "week" | "month" | "all";

export function DashboardPage({ orders }: DashboardPageProps) {
  const [period, setPeriod] = useState<TimePeriod>("all");
  const [showClearConfirm, setShowClearConfirm] = useState(false);

  function getRealStatus(order: CreatedOrder): string {
    const runs = order.runs || [];
    const now = Date.now();

    if (runs.length > 0) {
      const allFuture = runs.every((run) => {
        const runTime = run?.at instanceof Date ? run.at.getTime() : new Date(run?.at ?? now).getTime();
        return runTime > now;
      });
      if (allFuture && order.status !== "cancelled" && order.status !== "paused") {
        return "scheduled";
      }
    }

    if (runs.length > 0) {
      const allCompleted = runs.every((run) => {
        const runTime = run?.at instanceof Date ? run.at.getTime() : new Date(run?.at ?? now).getTime();
        return runTime <= now;
      });
      if (allCompleted) return "completed";
    }

    if (order.status === "processing") return "running";
    if (order.status === "pending") return "running";

    return order.status;
  }

  const filteredOrders = useMemo(() => {
    const now = new Date();
    const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const weekStart = new Date(todayStart);
    weekStart.setDate(weekStart.getDate() - 7);
    const monthStart = new Date(todayStart);
    monthStart.setDate(monthStart.getDate() - 30);

    return orders.filter((order) => {
      const orderDate = new Date(order.createdAt);
      if (period === "today") return orderDate >= todayStart;
      if (period === "week") return orderDate >= weekStart;
      if (period === "month") return orderDate >= monthStart;
      return true;
    });
  }, [orders, period]);

  const stats = useMemo(() => {
    const total = filteredOrders.length;

    const running = filteredOrders.filter((o) => {
      const realStatus = getRealStatus(o);
      return realStatus === "running" || realStatus === "processing" || realStatus === "paused";
    }).length;

    const completed = filteredOrders.filter((o) => {
      const realStatus = getRealStatus(o);
      return realStatus === "completed";
    }).length;

    const failed = filteredOrders.filter((o) => {
      const realStatus = getRealStatus(o);
      return realStatus === "failed" || realStatus === "cancelled";
    }).length;

    const scheduled = filteredOrders.filter((o) => {
      const realStatus = getRealStatus(o);
      return realStatus === "scheduled";
    }).length;

    const successRate = total > 0 ? Math.round((completed / total) * 100) : 0;

    return { total, running, completed, failed, scheduled, successRate };
  }, [filteredOrders]);

  const servicesBreakdown = useMemo(() => {
    let views = 0;
    let likes = 0;
    let shares = 0;
    let saves = 0;

    filteredOrders.forEach((order) => {
      (order.runs || []).forEach((run) => {
        views += run.views || 0;
        likes += run.likes || 0;
        shares += run.shares || 0;
        saves += run.saves || 0;
      });
    });

    const total = views + likes + shares + saves;
    return {
      views: { count: views, percent: total > 0 ? Math.round((views / total) * 100) : 0 },
      likes: { count: likes, percent: total > 0 ? Math.round((likes / total) * 100) : 0 },
      shares: { count: shares, percent: total > 0 ? Math.round((shares / total) * 100) : 0 },
      saves: { count: saves, percent: total > 0 ? Math.round((saves / total) * 100) : 0 },
      total,
    };
  }, [filteredOrders]);

  const chartData = useMemo(() => {
    const days: { label: string; count: number; date: Date }[] = [];
    const now = new Date();

    for (let i = 6; i >= 0; i--) {
      const date = new Date(now);
      date.setDate(date.getDate() - i);
      const dayStart = new Date(date.getFullYear(), date.getMonth(), date.getDate());
      const dayEnd = new Date(dayStart);
      dayEnd.setDate(dayEnd.getDate() + 1);

      const count = orders.filter((order) => {
        const orderDate = new Date(order.createdAt);
        return orderDate >= dayStart && orderDate < dayEnd;
      }).length;

      days.push({
        label: date.toLocaleDateString("en", { weekday: "short" }),
        count,
        date: dayStart,
      });
    }

    const maxCount = Math.max(...days.map((d) => d.count), 1);
    return { days, maxCount };
  }, [orders]);

  const recentOrders = useMemo(() => {
    return [...orders]
      .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())
      .slice(0, 5);
  }, [orders]);

  const getStatusColor = (order: CreatedOrder) => {
    const status = getRealStatus(order);
    switch (status) {
      case "running":
      case "processing": return "text-yellow-400";
      case "completed": return "text-emerald-400";
      case "paused": return "text-orange-400";
      case "scheduled": return "text-blue-400";
      case "failed":
      case "cancelled": return "text-red-400";
      default: return "text-gray-400";
    }
  };

  const getStatusBg = (order: CreatedOrder) => {
    const status = getRealStatus(order);
    switch (status) {
      case "running":
      case "processing": return "bg-yellow-500/20";
      case "completed": return "bg-emerald-500/20";
      case "paused": return "bg-orange-500/20";
      case "scheduled": return "bg-blue-500/20";
      case "failed":
      case "cancelled": return "bg-red-500/20";
      default: return "bg-gray-500/20";
    }
  };

  const getStatusIcon = (order: CreatedOrder) => {
    const status = getRealStatus(order);
    switch (status) {
      case "running":
      case "processing": return "⚡";
      case "completed": return "✅";
      case "paused": return "⏸️";
      case "scheduled": return "📅";
      case "failed":
      case "cancelled": return "❌";
      default: return "📦";
    }
  };

  const handleClearOrders = () => {
    localStorage.removeItem("dev-smm-orders");
    window.location.reload();
  };

  // ✅ Export handler
  const handleExport = () => {
    const data = {
      orders: localStorage.getItem("dev-smm-orders"),
      apis: localStorage.getItem("dev-smm-apis"),
      bundles: localStorage.getItem("dev-smm-bundles"),
    };
    const blob = new Blob([JSON.stringify(data)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `gotham-backup-${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // ✅ Import handler
  const handleImport = () => {
    const input = document.createElement("input");
    input.type = "file";
    input.accept = ".json";
    input.onchange = (e) => {
      const file = (e.target as HTMLInputElement).files?.[0];
      if (!file) return;
      const reader = new FileReader();
      reader.onload = (ev) => {
        try {
          const data = JSON.parse(ev.target?.result as string);
          if (data.orders) localStorage.setItem("dev-smm-orders", data.orders);
          if (data.apis) localStorage.setItem("dev-smm-apis", data.apis);
          if (data.bundles) localStorage.setItem("dev-smm-bundles", data.bundles);
          alert("✅ Data imported! Refreshing...");
          window.location.reload();
        } catch {
          alert("❌ Invalid backup file");
        }
      };
      reader.readAsText(file);
    };
    input.click();
  };

  return (
    <div className="mx-auto max-w-7xl space-y-4 px-3 py-4 sm:px-6 sm:py-7">

      {/* Header */}
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div className="flex items-center gap-2 sm:gap-3">
            <span className="text-2xl sm:text-3xl">🦇</span>
            <h2 className="text-xl sm:text-2xl font-bold tracking-tight text-yellow-400">
              Gotham Command
            </h2>
          </div>
          <p className="mt-1 text-xs sm:text-sm text-gray-500">
            Monitoring all operations across the city
          </p>
        </div>

        {/* Time Period Filter */}
        <div className="inline-flex rounded-lg border border-yellow-500/30 bg-black p-1 self-start sm:self-auto">
          {[
            { key: "today", label: "Today" },
            { key: "week", label: "7D" },
            { key: "month", label: "30D" },
            { key: "all", label: "All" },
          ].map((item) => (
            <button
              key={item.key}
              type="button"
              onClick={() => setPeriod(item.key as TimePeriod)}
              className={`rounded-md px-2 py-1.5 text-xs font-medium transition sm:px-3 ${
                period === item.key
                  ? "bg-yellow-500/20 text-yellow-400"
                  : "text-gray-500 hover:text-yellow-300"
              }`}
            >
              {item.label}
            </button>
          ))}
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-2 gap-3 sm:gap-4 lg:grid-cols-5">
        <div className="rounded-xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-3 sm:p-5">
          <div className="flex items-center justify-between">
            <p className="text-[10px] sm:text-xs font-medium uppercase tracking-wide text-gray-500">Total</p>
            <span className="text-lg sm:text-xl">📦</span>
          </div>
          <p className="mt-2 sm:mt-3 text-2xl sm:text-3xl font-bold text-white">{stats.total}</p>
          <p className="mt-1 text-[10px] sm:text-xs text-gray-600">
            {period === "today" && "Today"}
            {period === "week" && "Last 7 days"}
            {period === "month" && "Last 30 days"}
            {period === "all" && "All time"}
          </p>
        </div>

        <div className="rounded-xl border border-yellow-500/30 bg-gradient-to-br from-yellow-500/10 to-black p-3 sm:p-5">
          <div className="flex items-center justify-between">
            <p className="text-[10px] sm:text-xs font-medium uppercase tracking-wide text-yellow-500">Active</p>
            <span className="text-lg sm:text-xl">⚡</span>
          </div>
          <p className="mt-2 sm:mt-3 text-2xl sm:text-3xl font-bold text-yellow-400">{stats.running}</p>
          <div className="mt-1 sm:mt-2 flex items-center gap-1">
            {stats.running > 0 ? (
              <>
                <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-yellow-400" />
                <p className="text-[10px] sm:text-xs text-yellow-500/70">In progress</p>
              </>
            ) : (
              <p className="text-[10px] sm:text-xs text-yellow-500/70">None active</p>
            )}
          </div>
        </div>

        <div className="rounded-xl border border-blue-500/30 bg-gradient-to-br from-blue-500/10 to-black p-3 sm:p-5">
          <div className="flex items-center justify-between">
            <p className="text-[10px] sm:text-xs font-medium uppercase tracking-wide text-blue-500">Scheduled</p>
            <span className="text-lg sm:text-xl">📅</span>
          </div>
          <p className="mt-2 sm:mt-3 text-2xl sm:text-3xl font-bold text-blue-400">{stats.scheduled}</p>
          <p className="mt-1 text-[10px] sm:text-xs text-blue-500/70">Awaiting deploy</p>
        </div>

        <div className="rounded-xl border border-emerald-500/30 bg-gradient-to-br from-emerald-500/10 to-black p-3 sm:p-5">
          <div className="flex items-center justify-between">
            <p className="text-[10px] sm:text-xs font-medium uppercase tracking-wide text-emerald-500">Done</p>
            <span className="text-lg sm:text-xl">✅</span>
          </div>
          <p className="mt-2 sm:mt-3 text-2xl sm:text-3xl font-bold text-emerald-400">{stats.completed}</p>
          <p className="mt-1 text-[10px] sm:text-xs text-emerald-500/70">Accomplished</p>
        </div>

        <div className="col-span-2 rounded-xl border border-red-500/30 bg-gradient-to-br from-red-500/10 to-black p-3 sm:p-5 lg:col-span-1">
          <div className="flex items-center justify-between">
            <p className="text-[10px] sm:text-xs font-medium uppercase tracking-wide text-red-500">Failed</p>
            <span className="text-lg sm:text-xl">❌</span>
          </div>
          <p className="mt-2 sm:mt-3 text-2xl sm:text-3xl font-bold text-red-400">{stats.failed}</p>
          <p className="mt-1 text-[10px] sm:text-xs text-red-500/70">Needs attention</p>
        </div>
      </div>

      {/* Success Rate Bar */}
      <div className="rounded-xl border border-yellow-500/20 bg-gradient-to-r from-gray-900 to-black p-4 sm:p-5">
        <div className="flex items-center justify-between">
          <h3 className="text-xs sm:text-sm font-medium text-yellow-400">🎯 Mission Success Rate</h3>
          <span className={`text-xl sm:text-2xl font-bold ${
            stats.successRate >= 70 ? "text-emerald-400" :
            stats.successRate >= 40 ? "text-yellow-400" : "text-red-400"
          }`}>
            {stats.successRate}%
          </span>
        </div>
        <div className="mt-3 h-3 w-full overflow-hidden rounded-full bg-gray-800">
          <div
            className={`h-full rounded-full transition-all duration-500 ${
              stats.successRate >= 70 ? "bg-emerald-500" :
              stats.successRate >= 40 ? "bg-yellow-500" : "bg-red-500"
            }`}
            style={{ width: `${stats.successRate}%` }}
          />
        </div>
        <div className="mt-2 flex justify-between text-xs text-gray-500">
          <span>{stats.completed} successful</span>
          <span>{stats.failed} failed</span>
        </div>
      </div>

      {/* Two Column Layout */}
      <div className="grid gap-4 sm:gap-6 lg:grid-cols-2">
        <div className="rounded-xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-4 sm:p-5">
          <h3 className="text-xs sm:text-sm font-medium text-yellow-400">📈 Night Patrol Activity</h3>
          <div className="mt-4 sm:mt-5 flex h-32 sm:h-40 items-end justify-between gap-1 sm:gap-2">
            {chartData.days.map((day, index) => {
              const height = chartData.maxCount > 0 ? (day.count / chartData.maxCount) * 100 : 0;
              const isToday = index === chartData.days.length - 1;
              return (
                <div key={day.label} className="flex flex-1 flex-col items-center gap-1 sm:gap-2">
                  <span className="text-[10px] text-gray-500">{day.count}</span>
                  <div className="relative w-full flex-1">
                    <div
                      className={`absolute bottom-0 w-full rounded-t-md transition-all duration-500 ${
                        isToday ? "bg-yellow-500" : "bg-gray-700"
                      }`}
                      style={{ height: `${Math.max(height, 4)}%` }}
                    />
                  </div>
                  <span className={`text-[10px] ${isToday ? "text-yellow-400 font-medium" : "text-gray-600"}`}>
                    {day.label}
                  </span>
                </div>
              );
            })}
          </div>
        </div>

        <div className="rounded-xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-4 sm:p-5">
          <h3 className="text-xs sm:text-sm font-medium text-yellow-400">🦇 Arsenal Breakdown</h3>
          <div className="mt-4 sm:mt-5 space-y-3 sm:space-y-4">
            {[
              { label: "👁️ Views", data: servicesBreakdown.views, color: "bg-yellow-500" },
              { label: "❤️ Likes", data: servicesBreakdown.likes, color: "bg-yellow-600" },
              { label: "🔄 Shares", data: servicesBreakdown.shares, color: "bg-yellow-700" },
              { label: "🔖 Saves", data: servicesBreakdown.saves, color: "bg-amber-600" },
            ].map((item) => (
              <div key={item.label}>
                <div className="flex items-center justify-between text-xs">
                  <span className="text-gray-400">{item.label}</span>
                  <span className="text-gray-500">
                    {item.data.count.toLocaleString()} ({item.data.percent}%)
                  </span>
                </div>
                <div className="mt-1.5 h-2 w-full overflow-hidden rounded-full bg-gray-800">
                  <div
                    className={`h-full rounded-full transition-all duration-500 ${item.color}`}
                    style={{ width: `${item.data.percent}%` }}
                  />
                </div>
              </div>
            ))}
            <div className="mt-3 sm:mt-4 rounded-lg border border-yellow-500/20 bg-yellow-500/5 p-3 text-center">
              <p className="text-xs text-gray-500">Total Engagements</p>
              <p className="text-lg sm:text-xl font-bold text-yellow-400">
                {servicesBreakdown.total.toLocaleString()}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Recent Orders */}
      <div className="rounded-xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-4 sm:p-5">
        <div className="flex items-center justify-between">
          <h3 className="text-xs sm:text-sm font-medium text-yellow-400">⏰ Recent Missions</h3>
          <span className="text-[10px] sm:text-xs text-gray-600">Last 5 operations</span>
        </div>

        {recentOrders.length === 0 ? (
          <div className="mt-5 rounded-lg border border-dashed border-yellow-500/30 py-8 text-center">
            <span className="text-4xl">🦇</span>
            <p className="mt-2 text-sm text-gray-500">No missions deployed yet</p>
            <p className="mt-1 text-xs text-gray-600">The night is quiet... for now</p>
          </div>
        ) : (
          <div className="mt-4 space-y-2">
            {recentOrders.map((order) => {
              const realStatus = getRealStatus(order);
              return (
                <div
                  key={order.id}
                  className="flex items-center justify-between gap-2 rounded-lg border border-gray-800 bg-black/50 p-2.5 sm:p-3 transition hover:border-yellow-500/30"
                >
                  <div className="flex items-center gap-2 sm:gap-3 min-w-0">
                    <span className={`inline-flex h-7 w-7 sm:h-8 sm:w-8 flex-shrink-0 items-center justify-center rounded-full text-sm ${getStatusBg(order)}`}>
                      {getStatusIcon(order)}
                    </span>
                    <div className="min-w-0">
                      <p className="truncate text-xs sm:text-sm font-medium text-white">
                        {order.name || `Mission #${order.id.slice(0, 8)}`}
                      </p>
                      <p className="text-[10px] sm:text-xs text-gray-600">
                        {new Date(order.createdAt).toLocaleDateString()}
                      </p>
                    </div>
                  </div>
                  <div className="flex-shrink-0 text-right">
                    <span className={`inline-block rounded-full px-2 py-0.5 text-[10px] sm:text-xs font-medium capitalize ${getStatusBg(order)} ${getStatusColor(order)}`}>
                      {realStatus}
                    </span>
                    <p className="mt-1 text-[10px] text-gray-600">
                      {order.runs?.length || 0} runs
                    </p>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Quick Stats Footer */}
      <div className="grid grid-cols-3 gap-3 sm:gap-4">
        <div className="rounded-xl border border-yellow-500/20 bg-black p-3 sm:p-4 text-center">
          <p className="text-[10px] sm:text-xs text-gray-500">Avg Runs/Mission</p>
          <p className="mt-1 text-lg sm:text-xl font-bold text-yellow-400">
            {filteredOrders.length > 0
              ? Math.round(filteredOrders.reduce((sum, o) => sum + (o.runs?.length || 0), 0) / filteredOrders.length)
              : 0}
          </p>
        </div>
        <div className="rounded-xl border border-yellow-500/20 bg-black p-3 sm:p-4 text-center">
          <p className="text-[10px] sm:text-xs text-gray-500">Total Runs</p>
          <p className="mt-1 text-lg sm:text-xl font-bold text-white">
            {filteredOrders.reduce((sum, o) => sum + (o.runs?.length || 0), 0)}
          </p>
        </div>
        <div className="rounded-xl border border-yellow-500/20 bg-black p-3 sm:p-4 text-center">
          <p className="text-[10px] sm:text-xs text-gray-500">Completed Runs</p>
          <p className="mt-1 text-lg sm:text-xl font-bold text-emerald-400">
            {filteredOrders.reduce((sum, o) => sum + (o.completedRuns || 0), 0)}
          </p>
        </div>
      </div>

      {/* ✅ Export / Import Data */}
      <div className="rounded-xl border border-blue-500/30 bg-blue-500/5 p-4 sm:p-5">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h3 className="text-xs sm:text-sm font-medium text-blue-300">💾 Backup & Restore</h3>
            <p className="mt-1 text-[10px] sm:text-xs text-blue-400/70">
              Export your data to share across devices or restore from backup.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            {/* Export Button */}
            <button
              type="button"
              onClick={handleExport}
              className="rounded-lg border border-blue-500/50 bg-blue-500/10 px-3 py-2 text-xs sm:text-sm font-medium text-blue-200 transition hover:bg-blue-500/20"
            >
              📤 Export Data
            </button>
            {/* Import Button */}
            <button
              type="button"
              onClick={handleImport}
              className="rounded-lg border border-emerald-500/50 bg-emerald-500/10 px-3 py-2 text-xs sm:text-sm font-medium text-emerald-200 transition hover:bg-emerald-500/20"
            >
              📥 Import Data
            </button>
          </div>
        </div>
      </div>

      {/* Clear Orders */}
      <div className="rounded-xl border border-orange-500/30 bg-orange-500/5 p-4 sm:p-5">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h3 className="text-xs sm:text-sm font-medium text-orange-300">🧹 Clear Orders</h3>
            <p className="mt-1 text-[10px] sm:text-xs text-orange-400/70">
              Delete all orders for a fresh start.
              <br />
              <span className="text-emerald-400">✓ APIs and Bundles will be kept safe!</span>
            </p>
          </div>

          {!showClearConfirm ? (
            <button
              type="button"
              onClick={() => setShowClearConfirm(true)}
              className="self-start rounded-lg border border-orange-500/50 bg-orange-500/10 px-4 py-2 text-xs sm:text-sm font-medium text-orange-200 transition hover:bg-orange-500/20 sm:self-auto"
            >
              🗑️ Clear Orders
            </button>
          ) : (
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-xs text-orange-300">Are you sure?</span>
              <button
                type="button"
                onClick={handleClearOrders}
                className="rounded-lg border border-red-500 bg-red-500/30 px-3 py-1.5 text-xs font-medium text-red-100 transition hover:bg-red-500/50"
              >
                ✓ Yes, Delete
              </button>
              <button
                type="button"
                onClick={() => setShowClearConfirm(false)}
                className="rounded-lg border border-gray-600 bg-gray-800 px-3 py-1.5 text-xs font-medium text-gray-300 transition hover:bg-gray-700"
              >
                ✕ Cancel
              </button>
            </div>
          )}
        </div>
      </div>

    </div>
  );
}

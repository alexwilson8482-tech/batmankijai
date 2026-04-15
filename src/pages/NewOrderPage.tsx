import { useMemo, useState, useEffect } from "react";
import { motion } from "framer-motion";
import { GrowthGraph } from "../components/GrowthGraph";
import { PatternGenerator } from "../components/PatternGenerator";
import type {
  ApiPanel,
  Bundle,
  CreatedOrder,
  DeliveryOption,
  OrderConfig,
  PatternPlan,
  QuickPatternPreset,
} from "../types/order";
import { createSmmOrder } from "../utils/api";
import { createPatternPlan } from "../utils/patterns";

interface NewOrderPageProps {
  apis: ApiPanel[];
  bundles: Bundle[];
  orders: CreatedOrder[];
  prefillOrder?: CreatedOrder | null;
  onCreateOrder: (order: CreatedOrder) => void;
  onNavigateToOrders: (notice?: string) => void;
}

function createOrderId() {
  return `ORD-${Date.now().toString().slice(-6)}`;
}

export function NewOrderPage({ apis, bundles, orders, prefillOrder, onCreateOrder, onNavigateToOrders }: NewOrderPageProps) {
  const prefillApiId = prefillOrder ? apis.find((api) => api.name === prefillOrder.selectedAPI)?.id ?? "" : "";
  const prefillBundleId = prefillOrder
    ? bundles.find((bundle) => bundle.name === prefillOrder.selectedBundle && bundle.apiId === prefillApiId)?.id ?? ""
    : "";
  const prefillRuns = prefillOrder?.runs || [];
  const prefillPlan: PatternPlan | null = prefillOrder
    ? {
        patternId: Number(prefillOrder.id.replace(/\D/g, "")) || Date.now() % 1000,
        patternName: prefillOrder.patternName,
        patternType: prefillOrder.patternType,
        totalRuns: prefillRuns.length,
        approximateIntervalMin:
          prefillRuns.length > 1
            ? Math.max(
                1,
                Math.round(
                  prefillRuns
                    .slice(1)
                    .reduce((acc, run, index) => {
                      const prev = prefillRuns[index];
                      return acc + (run.at.getTime() - prev.at.getTime()) / 60000;
                    }, 0) / (prefillRuns.length - 1)
                )
              )
            : 0,
        finishTime: prefillRuns[prefillRuns.length - 1]?.at ?? new Date(),
        estimatedDurationHours:
          prefillRuns.length > 1
            ? Math.round(
                ((prefillRuns[prefillRuns.length - 1]?.at.getTime() ?? Date.now()) -
                  (prefillRuns[0]?.at.getTime() ?? Date.now())) /
                  3600000
              )
            : 0,
        risk: "Safe",
        runs: prefillRuns,
      }
    : null;

  const [orderName, setOrderName] = useState(prefillOrder?.name && !prefillOrder.name.startsWith("Order #") ? prefillOrder.name : "");
  const [postUrl, setPostUrl] = useState(prefillOrder?.link ?? "");
  const [bulkLinks, setBulkLinks] = useState("");
  const [totalViews, setTotalViews] = useState(prefillOrder?.totalViews ?? 50000);
  const [selectedApiId, setSelectedApiId] = useState(prefillApiId);
  const [selectedBundleId, setSelectedBundleId] = useState(prefillBundleId);
  const [startDelayHours, setStartDelayHours] = useState(prefillOrder?.startDelayHours ?? 0);
  const [includeLikes, setIncludeLikes] = useState((prefillOrder?.engagement.likes ?? 0) > 0);
  const [includeShares, setIncludeShares] = useState((prefillOrder?.engagement.shares ?? 0) > 0);
  const [includeSaves, setIncludeSaves] = useState((prefillOrder?.engagement.saves ?? 0) > 0);
  const [customComments, setCustomComments] = useState("");
  const [includeComments, setIncludeComments] = useState(false);
  const [variancePercent, setVariancePercent] = useState(40);
  const [peakHoursBoost, setPeakHoursBoost] = useState(false);
  const [quickPreset, setQuickPreset] = useState<QuickPatternPreset | null>(null);
  const [customHours, setCustomHours] = useState(30);
  const [delivery, setDelivery] = useState<DeliveryOption>({ mode: "auto", hours: 18, label: "Auto" });
  const [seed, setSeed] = useState(0);
  const [useClonedPlan, setUseClonedPlan] = useState(Boolean(prefillPlan));
  const [clonedPlan] = useState<PatternPlan | null>(prefillPlan);
  const [expandedRuns, setExpandedRuns] = useState(false);
  const [createError, setCreateError] = useState("");
  const [createSuccess, setCreateSuccess] = useState("");
  const [isCreatingOrder, setIsCreatingOrder] = useState(false);

  // 🔥 NEW: Minimum views per run state
  const [minViewsPerRun, setMinViewsPerRun] = useState(100);

  // 🔥 NEW: Fetch min views setting from backend on mount
  useEffect(() => {
    const fetchMinViews = async () => {
      try {
        const backendUrl = import.meta.env.VITE_BACKEND_URL || "https://backend-y30y.onrender.com";
        const response = await fetch("https://iamsuperman-backend.onrender.com/api/settings/min-views");
        if (response.ok) {
          const data = await response.json();
          if (data.minViewsPerRun) {
            setMinViewsPerRun(data.minViewsPerRun);
          }
        }
      } catch (error) {
        console.warn("Could not fetch min views setting, using default 100");
      }
    };
    fetchMinViews();
  }, []);

  // 🔥 UPDATED: Config now includes minViewsPerRun
  const selectedApi = apis.find(a => a.id === selectedApiId);
const selectedBundle = bundles.find(b => b.id === selectedBundleId);

const commentsService = selectedApi?.services.find(
  s => s.id === selectedBundle?.serviceIds.comments
);
  
  const config: OrderConfig = useMemo(
    () => ({
      postUrl,
      totalViews,
      startDelayHours,
      includeLikes,
      includeShares,
      includeSaves,
      includeComments,
      variancePercent,
      peakHoursBoost,
      quickPreset,
      delivery:
        delivery.mode === "custom"
          ? { ...delivery, hours: customHours, label: "Custom" }
          : delivery.mode === "auto"
            ? { ...delivery, hours: Math.max(6, Math.min(48, delivery.hours)) }
            : delivery,
      minViewsPerRun, // 🔥 NEW: Pass to pattern generator
    }),
    [
  postUrl,
  totalViews,
  startDelayHours,
  includeLikes,
  includeShares,
  includeSaves,
  includeComments, // ✅ ADD THIS
  variancePercent,
  peakHoursBoost,
  quickPreset,
  delivery,
  customHours,
  minViewsPerRun,
]
  );

  const generatedPlan = useMemo(() => {
    try {
      const nextPlan = createPatternPlan(config);
      return { ...nextPlan, runs: nextPlan?.runs || [] };
    } catch (error) {
      console.error("Pattern plan generation failed", error);
      const now = new Date();
      return {
        patternId: 0,
        patternName: "fallback",
        patternType: "smooth-s-curve" as const,
        totalRuns: 0,
        approximateIntervalMin: 0,
        finishTime: now,
        estimatedDurationHours: 0,
        risk: "Safe" as const,
        runs: [],
      };
    }
  }, [config, seed]);

  const plan = useMemo(() => {
    const basePlan = useClonedPlan && clonedPlan
      ? { ...clonedPlan, runs: clonedPlan.runs || [] }
      : generatedPlan;

    const runs = basePlan?.runs || [];

    if (runs.length <= 1) return basePlan;

    const baseIntervalMin = basePlan.approximateIntervalMin || 120;

    const newRuns = runs.map((run, i) => {
      if (i === 0) return run;

      const prevTime = new Date(runs[i - 1].at).getTime();
      const hour = new Date(prevTime).getHours();

      let multiplier = 1;

      if (hour >= 0 && hour < 6) multiplier = 1.4;
      else if (hour >= 6 && hour < 12) multiplier = 1.1;
      else if (hour >= 18 && hour <= 23) multiplier = 0.85;

      const baseIntervalMs = baseIntervalMin * 60 * 1000 * multiplier;
      const variation = baseIntervalMs * (Math.random() * 0.4 - 0.2);
      const newTime = prevTime + baseIntervalMs + variation;

      return {
        ...run,
        at: new Date(newTime),
      };
    });

    return {
      ...basePlan,
      runs: newRuns,
    };
  }, [useClonedPlan, clonedPlan, generatedPlan]);

  const safePlan = useMemo(() => ({ ...plan, runs: plan?.runs || [] }), [plan]);

  const bundleOptions = useMemo(() => {
    if (!selectedApiId) return bundles;
    return bundles.filter((bundle) => bundle.apiId === selectedApiId);
  }, [bundles, selectedApiId]);

  function isValidUrl(value: string) {
    try {
      const parsed = new URL(value);
      return parsed.protocol === "http:" || parsed.protocol === "https:";
    } catch {
      return false;
    }
  }

  const handleApplyPreset = (preset: QuickPatternPreset) => {
    setUseClonedPlan(false);
    setQuickPreset(preset);
    if (preset === "viral-boost") {
      setVariancePercent(48);
      setDelivery({ mode: "preset", label: "12h", hours: 12 });
    }
    if (preset === "fast-start") {
      setVariancePercent(32);
      setDelivery({ mode: "preset", label: "6h", hours: 6 });
    }
    if (preset === "trending-push") {
      setVariancePercent(40);
      setDelivery({ mode: "preset", label: "24h", hours: 24 });
    }
    if (preset === "slow-burn") {
      setVariancePercent(22);
      setDelivery({ mode: "preset", label: "48h", hours: 48 });
    }
    setSeed((current) => current + 1);
    setExpandedRuns(true);
  };

  const handleGenerate = () => {
    setUseClonedPlan(false);
    setSeed((current) => current + 1);
    setExpandedRuns(true);
  };

  // 🔥 NEW: Handle min views change - regenerate pattern when changed
  const handleMinViewsChange = (value: number) => {
    const newValue = Math.max(1, Math.floor(value));
    setMinViewsPerRun(newValue);
    setUseClonedPlan(false);
    setSeed((current) => current + 1); // 🔥 Force regenerate pattern

    // Also update backend
    const backendUrl = import.meta.env.VITE_BACKEND_URL || "https://backend-y30y.onrender.com";
    fetch("https://iamsuperman-backend.onrender.com/api/settings/min-views", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ minViewsPerRun: newValue }),
    }).catch(() => console.warn("Could not update min views setting on backend"));
  };

  const deliveryOptions: DeliveryOption[] = [
    { mode: "preset", label: "6h", hours: 6 },
    { mode: "preset", label: "12h", hours: 12 },
    { mode: "auto", label: "Auto", hours: 18 },
    { mode: "preset", label: "24h", hours: 24 },
    { mode: "preset", label: "48h", hours: 48 },
    { mode: "custom", label: "Custom", hours: customHours },
  ];

  // 🔥 Calculate estimated runs and views per run for display
  const estimatedRunCount = safePlan.runs.length;
  const averageViewsPerRun = estimatedRunCount > 0 ? Math.round(totalViews / estimatedRunCount) : 0;

  return (
    <div className="mx-auto max-w-7xl space-y-2 px-3 py-3">
      {/* Compact Header */}
      <motion.div initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.3 }}>
        <div className="flex items-center gap-2">
          <span className="text-xl">⚡</span>
          <h2 className="text-lg font-bold tracking-tight text-yellow-400">New Mission</h2>
          <span className="text-[10px] text-gray-500 ml-2">Configure delivery patterns</span>
        </div>
      </motion.div>

      {/* Main Grid - Two Columns */}
      <div className="grid gap-3 xl:grid-cols-2">
        
        {/* LEFT COLUMN - Basic Order Info */}
        <div className="space-y-2">
          <div className="rounded-xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-3">
            <h3 className="text-xs font-semibold text-yellow-400 mb-2">📋 Order Details</h3>
            
            {/* Order Name & URL */}
            <div className="grid grid-cols-2 gap-2 mb-2">
              <div>
                <label className="text-[10px] text-gray-500 mb-1 block">Order Name</label>
                <input
                  type="text"
                  value={orderName}
                  onChange={(e) => setOrderName(e.target.value)}
                  placeholder="Mission name..."
                  className="w-full rounded-lg border border-yellow-500/20 bg-black px-2 py-1.5 text-xs text-white placeholder-gray-600 focus:border-yellow-500/50 focus:outline-none"
                />
              </div>
              <div>
                <label className="text-[10px] text-gray-500 mb-1 block">Total Views</label>
                <input
                  type="number"
                  value={totalViews}
                  onChange={(e) => {
                    setUseClonedPlan(false);
                    const safeValue = Number.isFinite(Number(e.target.value)) ? Number(e.target.value) : 0;
                    setTotalViews(Math.max(0, Math.floor(safeValue)));
                  }}
                  className="w-full rounded-lg border border-yellow-500/20 bg-black px-2 py-1.5 text-xs text-white focus:border-yellow-500/50 focus:outline-none"
                />
              </div>
            </div>

            {/* Post URL */}
            <div className="mb-2">
              <label className="text-[10px] text-gray-500 mb-1 block">Post URL</label>
              <input
                type="text"
                value={postUrl}
                onChange={(e) => setPostUrl(e.target.value)}
                placeholder="https://instagram.com/reel/..."
                className="w-full rounded-lg border border-yellow-500/20 bg-black px-2 py-1.5 text-xs text-white placeholder-gray-600 focus:border-yellow-500/50 focus:outline-none"
              />
            </div>

            {/* Bulk Links */}
            <div className="mb-2">
              <label className="text-[10px] text-gray-500 mb-1 block">Bulk Links (one per line)</label>
              <textarea
                value={bulkLinks}
                onChange={(e) => setBulkLinks(e.target.value)}
                placeholder="Paste multiple URLs..."
                rows={2}
                className="w-full rounded-lg border border-yellow-500/20 bg-black px-2 py-1.5 text-xs text-white placeholder-gray-600 focus:border-yellow-500/50 focus:outline-none resize-none"
              />
            </div>

            {/* API & Bundle Selection */}
            <div className="grid grid-cols-2 gap-2">
              <div>
                <label className="text-[10px] text-gray-500 mb-1 block">API Panel</label>
                <select
                  value={selectedApiId}
                  onChange={(e) => {
                    setSelectedApiId(e.target.value);
                    setSelectedBundleId("");
                  }}
                  className="w-full rounded-lg border border-yellow-500/20 bg-black px-2 py-1.5 text-xs text-white focus:border-yellow-500/50 focus:outline-none"
                >
                  <option value="">Select API</option>
                  {apis.map((api) => (
                    <option key={api.id} value={api.id}>{api.name}</option>
                  ))}
                </select>
              </div>
              <div>
                <label className="text-[10px] text-gray-500 mb-1 block">Bundle</label>
                <select
                  value={selectedBundleId}
                  onChange={(e) => setSelectedBundleId(e.target.value)}
                  className="w-full rounded-lg border border-yellow-500/20 bg-black px-2 py-1.5 text-xs text-white focus:border-yellow-500/50 focus:outline-none"
                >
                  <option value="">Select Bundle</option>
                  {bundleOptions.map((bundle) => (
                    <option key={bundle.id} value={bundle.id}>{bundle.name}</option>
                  ))}
                </select>
              </div>
            </div>
          </div>

          {/* 🔥 NEW: Minimum Views Per Run Settings Block */}
          <div className="rounded-xl border border-blue-500/30 bg-gradient-to-br from-blue-900/20 to-black p-3">
            <h3 className="text-xs font-semibold text-blue-400 mb-2 flex items-center gap-2">
              <span>⚙️</span> Global Run Settings
            </h3>
            
            <div className="space-y-3">
              {/* Min Views Input */}
              <div className="flex items-center justify-between gap-3">
                <label className="text-[10px] text-gray-400">Minimum Views Per Run</label>
                <div className="flex items-center gap-2">
                  <input
                    type="number"
                    value={minViewsPerRun}
                    onChange={(e) => handleMinViewsChange(Number(e.target.value))}
                    min={1}
                    max={10000}
                    className="w-20 rounded-lg border border-blue-500/30 bg-black px-2 py-1 text-xs text-white text-center focus:border-blue-500/50 focus:outline-none"
                  />
                  <span className="text-[9px] text-gray-500">views/run</span>
                </div>
              </div>

              {/* Quick presets for min views */}
              <div className="flex gap-1 flex-wrap">
                {[100, 200, 300, 500, 1000].map((preset) => (
                  <button
                    key={preset}
                    type="button"
                    onClick={() => handleMinViewsChange(preset)}
                    className={`rounded-md px-2 py-0.5 text-[9px] font-medium transition ${
                      minViewsPerRun === preset
                        ? "border border-blue-500 bg-blue-500/20 text-blue-300"
                        : "border border-blue-500/20 bg-black text-gray-500 hover:text-blue-300"
                    }`}
                  >
                    {preset}
                  </button>
                ))}
              </div>

              {/* Live calculation display */}
              <div className="rounded-lg border border-blue-500/20 bg-blue-500/5 px-2 py-2">
                <div className="flex items-center justify-between text-[10px]">
                  <span className="text-gray-400">Estimated Runs:</span>
                  <span className="text-blue-300 font-semibold">{estimatedRunCount}</span>
                </div>
                <div className="flex items-center justify-between text-[10px] mt-1">
                  <span className="text-gray-400">Avg Views/Run:</span>
                  <span className="text-blue-300 font-semibold">{averageViewsPerRun.toLocaleString()}</span>
                </div>
                <div className="flex items-center justify-between text-[10px] mt-1">
                  <span className="text-gray-400">Max Possible Runs:</span>
                  <span className="text-gray-500">{Math.floor(totalViews / minViewsPerRun)}</span>
                </div>
              </div>

              <p className="text-[9px] text-blue-300/60 leading-relaxed">
                ℹ️ Higher minimum = fewer runs with more views each. Lower minimum = more runs with fewer views each.
              </p>
            </div>
          </div>

          {/* Growth Graph - Compact */}
          <GrowthGraph 
            plan={safePlan}
            selectedPreset={quickPreset}
            onApplyPreset={handleApplyPreset}
            onGenerate={handleGenerate}
          />
        </div>

        {/* RIGHT COLUMN - Schedule Preview + Advanced Controls */}
        <div className="space-y-2">
          
          {/* Detection Risk - Inline */}
          <div className="flex items-center justify-between rounded-lg border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black px-3 py-2">
            <div className="flex items-center gap-2">
              <span className="text-sm">🎯</span>
              <span className="text-xs font-medium text-yellow-400">Risk:</span>
            </div>
            <div className="flex items-center gap-3">
              <span className="text-[10px] text-gray-500">{safePlan.estimatedDurationHours}h duration</span>
              <span className="text-[10px] text-gray-500">{estimatedRunCount} runs</span>
              <span className={`rounded-md border px-2 py-0.5 text-xs font-semibold ${
                safePlan.risk === "Safe"
                  ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-400"
                  : safePlan.risk === "Medium"
                    ? "border-yellow-500/30 bg-yellow-500/10 text-yellow-400"
                    : "border-red-500/30 bg-red-500/10 text-red-400"
              }`}>
                {safePlan.risk}
              </span>
            </div>
          </div>

          {/* Schedule Preview */}
          <PatternGenerator
            plan={safePlan}
            expandedRuns={expandedRuns}
            onToggleRuns={() => setExpandedRuns((prev) => !prev)}
          />

          {/* Advanced Controls - Below Schedule Preview */}
          <div className="rounded-xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-3">
            <h3 className="text-xs font-semibold text-yellow-400 mb-2">⚙️ Advanced Controls</h3>
            
            {/* Row 1: Start Delay & Variance */}
            <div className="grid grid-cols-2 gap-2 mb-2">
              <div>
                <label className="text-[10px] text-gray-500 mb-1 block">Start Delay (hours)</label>
                <input
                  type="number"
                  value={startDelayHours}
                  onChange={(e) => {
                    setUseClonedPlan(false);
                    const safeValue = Number.isFinite(Number(e.target.value)) ? Number(e.target.value) : 0;
                    setStartDelayHours(Math.max(0, Math.min(168, Math.floor(safeValue))));
                  }}
                  min={0}
                  max={168}
                  className="w-full rounded-lg border border-yellow-500/20 bg-black px-2 py-1.5 text-xs text-white focus:border-yellow-500/50 focus:outline-none"
                />
              </div>
              <div>
                <label className="text-[10px] text-gray-500 mb-1 block">Variance: {variancePercent}%</label>
                <input
                  type="range"
                  value={variancePercent}
                  onChange={(e) => {
                    setUseClonedPlan(false);
                    setVariancePercent(Number(e.target.value));
                  }}
                  min={0}
                  max={50}
                  className="w-full h-1.5 bg-gray-700 rounded-lg appearance-none cursor-pointer accent-yellow-500"
                />
              </div>
            </div>

            {/* Row 2: Delivery Speed */}
            <div className="mb-2">
              <label className="text-[10px] text-gray-500 mb-1 block">Delivery Speed</label>
              <div className="flex gap-1 flex-wrap">
                {deliveryOptions.map((option) => (
                  <button
                    key={option.label}
                    type="button"
                    onClick={() => {
                      setUseClonedPlan(false);
                      setDelivery(option);
                    }}
                    className={`rounded-md px-2 py-1 text-[10px] font-medium transition ${
                      delivery.label === option.label
                        ? "border border-yellow-500 bg-yellow-500/20 text-yellow-300"
                        : "border border-yellow-500/20 bg-black text-gray-400 hover:text-yellow-300"
                    }`}
                  >
                    {option.label}
                  </button>
                ))}
              </div>
              {delivery.mode === "custom" && (
                <input
                  type="number"
                  value={customHours}
                  onChange={(e) => {
                    setUseClonedPlan(false);
                    const safeHours = Number.isFinite(Number(e.target.value)) ? Number(e.target.value) : 1;
                    const clampedHours = Math.max(1, Math.min(96, safeHours));
                    setCustomHours(clampedHours);
                    setDelivery({ mode: "custom", label: "Custom", hours: clampedHours });
                  }}
                  min={1}
                  max={96}
                  placeholder="Hours"
                  className="mt-1 w-20 rounded-lg border border-yellow-500/20 bg-black px-2 py-1 text-xs text-white focus:border-yellow-500/50 focus:outline-none"
                />
              )}
            </div>

            {/* Row 3: Engagement Toggles + Peak Hours */}
            <div className="flex flex-wrap items-center gap-2">
              <label className="text-[10px] text-gray-500">Engagement:</label>
              
              <button
                type="button"
                onClick={() => { setUseClonedPlan(false); setIncludeLikes(!includeLikes); }}
                className={`flex items-center gap-1 rounded-md px-2 py-1 text-[10px] font-medium transition ${
                  includeLikes
                    ? "border border-pink-500 bg-pink-500/20 text-pink-300"
                    : "border border-gray-600 bg-black text-gray-500"
                }`}
              >
                ❤️ Likes
              </button>
              
              <button
                type="button"
                onClick={() => { setUseClonedPlan(false); setIncludeShares(!includeShares); }}
                className={`flex items-center gap-1 rounded-md px-2 py-1 text-[10px] font-medium transition ${
                  includeShares
                    ? "border border-blue-500 bg-blue-500/20 text-blue-300"
                    : "border border-gray-600 bg-black text-gray-500"
                }`}
              >
                🔄 Shares
              </button>
              
              <button
                type="button"
                onClick={() => { setUseClonedPlan(false); setIncludeSaves(!includeSaves); }}
                className={`flex items-center gap-1 rounded-md px-2 py-1 text-[10px] font-medium transition ${
                  includeSaves
                    ? "border border-purple-500 bg-purple-500/20 text-purple-300"
                    : "border border-gray-600 bg-black text-gray-500"
                }`}
              >
                💾 Saves
              </button>

              <button
  type="button"
  onClick={() => { setUseClonedPlan(false); setIncludeComments(!includeComments); }}
  className={`flex items-center gap-1 rounded-md px-2 py-1 text-[10px] font-medium transition ${
    includeComments
      ? "border border-pink-500 bg-pink-500/20 text-pink-300"
      : "border border-gray-600 bg-black text-gray-500"
  }`}
>
  💬 Comments
</button>

              <div className="ml-auto">
                <button
                  type="button"
                  onClick={() => { setUseClonedPlan(false); setPeakHoursBoost(!peakHoursBoost); }}
                  className={`flex items-center gap-1 rounded-md px-2 py-1 text-[10px] font-medium transition ${
                    peakHoursBoost
                      ? "border border-orange-500 bg-orange-500/20 text-orange-300"
                      : "border border-gray-600 bg-black text-gray-500"
                  }`}
                >
                  🔥 Peak Hours
                </button>
              </div>
            </div>
          </div>
<div className="mt-2">
  <label className="text-[10px] text-gray-500 mb-1 block">
    💬 Custom Comments (one per line)
  </label>
  <textarea
    value={customComments}
    onChange={(e) => setCustomComments(e.target.value)}
    rows={3}
    placeholder={"Nice post!\n🔥🔥\nAmazing"}
    className="w-full rounded-lg border border-yellow-500/20 bg-black px-2 py-1.5 text-xs text-white"
  />
</div>
          {/* Price Calculator - Compact Horizontal */}
          {selectedBundleId && safePlan.runs.length > 0 && (
            <div className="rounded-lg border border-yellow-500/30 bg-gradient-to-br from-yellow-500/5 to-black p-2">
              <div className="flex items-center justify-between gap-2 flex-wrap">
                <span className="text-xs font-semibold text-yellow-400">💰</span>
                
                {/* Price Items */}
                <div className="flex items-center gap-1 flex-wrap flex-1">
                  {(() => {
                    const selectedBundle = bundles.find(b => b.id === selectedBundleId);
                    const selectedApi = apis.find(a => a.id === selectedApiId);
                    
                    if (!selectedBundle || !selectedApi) return null;
                    
                    const viewsService = selectedApi.services.find(s => s.id === selectedBundle.serviceIds.views);
                    const likesService = selectedApi.services.find(s => s.id === selectedBundle.serviceIds.likes);
                    const sharesService = selectedApi.services.find(s => s.id === selectedBundle.serviceIds.shares);
                    const savesService = selectedApi.services.find(s => s.id === selectedBundle.serviceIds.saves);
                    const commentsService = selectedApi.services.find(s => s.id === selectedBundle.serviceIds.comments);
                    const commentsMin = Number(commentsService?.min || 0);
                    
                    const totalViewsQty = safePlan.runs.reduce((sum, run) => sum + (run.views || 0), 0);
                    const totalLikesQty = safePlan.runs.reduce((sum, run) => sum + (run.likes || 0), 0);
                    const totalSharesQty = safePlan.runs.reduce((sum, run) => sum + (run.shares || 0), 0);
                    const totalSavesQty = safePlan.runs.reduce((sum, run) => sum + (run.saves || 0), 0);
                    const totalCommentsQty = safePlan.runs.reduce((sum, run) => sum + (run.comments || 0), 0);
                    
                    const viewsRate = parseFloat(viewsService?.rate || "0");
                    const likesRate = parseFloat(likesService?.rate || "0");
                    const sharesRate = parseFloat(sharesService?.rate || "0");
                    const savesRate = parseFloat(savesService?.rate || "0");
                    const commentsRate = parseFloat(commentsService?.rate || "0");
                    
                    const viewsPrice = (totalViewsQty / 1000) * viewsRate;
                    const likesPrice = includeLikes ? (totalLikesQty / 1000) * likesRate : 0;
                    const sharesPrice = includeShares ? (totalSharesQty / 1000) * sharesRate : 0;
                    const savesPrice = includeSaves ? (totalSavesQty / 1000) * savesRate : 0;
                    const commentsPrice = includeComments ? (totalCommentsQty / 1000) * commentsRate : 0;
                    
                    return (
                      <>
                        <span className="text-[10px] text-gray-400">👁️{(totalViewsQty/1000).toFixed(0)}k=₹{viewsPrice.toFixed(0)}</span>
                        {includeLikes && totalLikesQty > 0 && (
                          <span className="text-[10px] text-gray-400">❤️{(totalLikesQty/1000).toFixed(1)}k=₹{likesPrice.toFixed(0)}</span>
                        )}
                        {includeShares && totalSharesQty > 0 && (
                          <span className="text-[10px] text-gray-400">🔄{(totalSharesQty/1000).toFixed(1)}k=₹{sharesPrice.toFixed(0)}</span>
                        )}
                        {includeSaves && totalSavesQty > 0 && (
                          <span className="text-[10px] text-gray-400">💾{(totalSavesQty/1000).toFixed(1)}k=₹{savesPrice.toFixed(0)}</span>
                        )}
                        {includeComments && totalCommentsQty > 0 && (
                          <span className="text-[10px] text-gray-400">
                          💬{(totalCommentsQty/1000).toFixed(1)}k=₹{commentsPrice.toFixed(0)}
                          </span>
                        )}
                      </>
                    );
                  })()}
                </div>
                
                {/* Total */}
                <div className="rounded-md border border-yellow-500/40 bg-yellow-500/10 px-2 py-1">
                  <span className="text-sm font-bold text-yellow-400">
                    ₹{(() => {
                      const selectedBundle = bundles.find(b => b.id === selectedBundleId);
                      const selectedApi = apis.find(a => a.id === selectedApiId);
                      
                      if (!selectedBundle || !selectedApi) return "0";
                      
                      const viewsService = selectedApi.services.find(s => s.id === selectedBundle.serviceIds.views);
const likesService = selectedApi.services.find(s => s.id === selectedBundle.serviceIds.likes);
const sharesService = selectedApi.services.find(s => s.id === selectedBundle.serviceIds.shares);
const savesService = selectedApi.services.find(s => s.id === selectedBundle.serviceIds.saves);
const commentsService = selectedApi.services.find(s => s.id === selectedBundle.serviceIds.comments);

const totalViewsQty = safePlan.runs.reduce((sum, run) => sum + (run.views || 0), 0);
const totalLikesQty = safePlan.runs.reduce((sum, run) => sum + (run.likes || 0), 0);
const totalSharesQty = safePlan.runs.reduce((sum, run) => sum + (run.shares || 0), 0);
const totalSavesQty = safePlan.runs.reduce((sum, run) => sum + (run.saves || 0), 0);
const totalCommentsQty = safePlan.runs.reduce((sum, run) => sum + (run.comments || 0), 0);

const viewsRate = parseFloat(viewsService?.rate || "0");
const likesRate = parseFloat(likesService?.rate || "0");
const sharesRate = parseFloat(sharesService?.rate || "0");
const savesRate = parseFloat(savesService?.rate || "0");
const commentsRate = parseFloat(commentsService?.rate || "0");

const viewsPrice = (totalViewsQty / 1000) * viewsRate;
const likesPrice = includeLikes ? (totalLikesQty / 1000) * likesRate : 0;
const sharesPrice = includeShares ? (totalSharesQty / 1000) * sharesRate : 0;
const savesPrice = includeSaves ? (totalSavesQty / 1000) * savesRate : 0;
const commentsPrice = includeComments ? (totalCommentsQty / 1000) * commentsRate : 0;

return (viewsPrice + likesPrice + sharesPrice + savesPrice + commentsPrice).toFixed(0);
                    })()}
                  </span>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Deploy Button - Full Width Bottom */}
      <div className="flex items-center justify-between rounded-lg border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black px-3 py-2">
        <div className="flex items-center gap-2">
          {createError && <span className="text-[10px] text-red-400">❌ {createError}</span>}
          {createSuccess && <span className="text-[10px] text-emerald-400">✅ {createSuccess}</span>}
          {!createError && !createSuccess && (
            <span className="text-[10px] text-gray-500">
              Ready to deploy • {estimatedRunCount} runs • ~{averageViewsPerRun} views/run
            </span>
          )}
        </div>
        <button
          type="button"
          disabled={isCreatingOrder}
          onClick={async () => {
            console.log("CLICKED BUTTON");
            setCreateError("");
            setCreateSuccess("");
            if (!selectedBundleId) {
              setCreateError("Select a bundle before creating a mission.");
              return;
            }
            const bulkTargets = bulkLinks
              .split(/\r?\n/)
              .map((line) => line.trim())
              .filter(Boolean);
            const singleTarget = postUrl.trim();
            const targets = bulkTargets.length > 0 ? bulkTargets : singleTarget ? [singleTarget] : [];
            if (!targets.length) {
              setCreateError("Add a post URL or paste multiple links.");
              return;
            }
            const invalidTarget = targets.find((target) => !isValidUrl(target));
            if (invalidTarget) {
              setCreateError(`Invalid URL: ${invalidTarget.slice(0, 30)}...`);
              return;
            }

            const selectedApi = apis.find((api) => api.id === selectedApiId) ?? null;
            if (!selectedApi) {
              setCreateError("Select an API.");
              return;
            }
            if (!selectedApi.url.trim()) {
              setCreateError("API URL is required.");
              return;
            }
            if (!isValidUrl(selectedApi.url.trim())) {
              setCreateError("API URL must be valid.");
              return;
            }
            if (!selectedApi.key.trim()) {
              setCreateError("API key is required.");
              return;
            }

            const selectedBundle = bundles.find((bundle) => bundle.id === selectedBundleId);
            if (!selectedBundle) {
              setCreateError("Select a valid bundle.");
              return;
            }
            const viewsServiceId = selectedBundle.serviceIds.views.trim();
            if (!viewsServiceId) {
              setCreateError("Bundle has no Views service.");
              return;
            }
            const likesServiceId = selectedBundle.serviceIds.likes.trim();
            const sharesServiceId = selectedBundle.serviceIds.shares.trim();
            const savesServiceId = selectedBundle.serviceIds.saves.trim();
            if (includeLikes && !likesServiceId) {
              setCreateError("Bundle has no Likes service.");
              return;
            }
            if (includeShares && !sharesServiceId) {
              setCreateError("Bundle has no Shares service.");
              return;
            }
            if (includeSaves && !savesServiceId) {
              setCreateError("Bundle has no Saves service.");
              return;
            }
            const commentsServiceId = selectedBundle.serviceIds.comments?.trim();
            if (includeComments && !commentsServiceId) {
  setCreateError("Bundle has no Comments service.");
  return;
}

            const quantity = (safePlan?.runs || []).reduce((acc, run) => acc + run.views, 0);
            if (!Number.isFinite(quantity) || quantity <= 0) {
              setCreateError("Quantity must be > 0.");
              return;
            }
            if (quantity < minViewsPerRun) {
              setCreateError(`Views must be at least ${minViewsPerRun}.`);
              return;
            }

            const totalLikes = (safePlan?.runs || []).reduce((acc, run) => acc + run.likes, 0);
            const totalShares = (safePlan?.runs || []).reduce((acc, run) => acc + run.shares, 0);
            const totalSaves = (safePlan?.runs || []).reduce((acc, run) => acc + run.saves, 0);
            const totalCommentsQty = (safePlan?.runs || []).reduce(
  (acc, run) => acc + (run.comments || 0),
  0
);

            if (includeLikes && totalLikes < 10) {
              setCreateError("Likes must be at least 10.");
              return;
            }
            if (includeShares && totalShares < 20) {
              setCreateError("Shares must be at least 20.");
              return;
            }
            if (includeSaves && totalSaves < 10) {
              setCreateError("Saves must be at least 10.");
              return;
            }
            if (includeComments && totalCommentsQty <= 0) {
  setCreateError("Comments must be greater than 0.");
  return;
}

            if (quantity > 100000) {
              const proceed = window.confirm("Large mission. Continue?");
              if (!proceed) return;
            }

            const viewRuns = (safePlan?.runs || []).map((run) => ({
              time: run.at.toISOString(),
              quantity: Math.max(Math.floor(run.views), minViewsPerRun),
            }));
            if (!viewRuns.length || viewRuns.some((run) => !run.time || !Number.isFinite(run.quantity) || run.quantity <= 0)) {
              setCreateError("Invalid run schedule. Regenerate.");
              return;
            }

            const likesRuns = (safePlan?.runs || []).map((run) => ({
              time: run.at.toISOString(),
              quantity: Math.max(0, Math.floor(run.likes)),
            }));
            const sharesRuns = (safePlan?.runs || []).map((run) => ({
              time: run.at.toISOString(),
              quantity: Math.max(0, Math.floor(run.shares)),
            }));
            const savesRuns = (safePlan?.runs || []).map((run) => ({
              time: run.at.toISOString(),
              quantity: Math.max(0, Math.floor(run.saves)),
            }));

const commentList = customComments
  .split("\n")
  .map(c => c.trim())
  .filter(Boolean);

const commentsRuns = (safePlan?.runs || []).map((run) => {
  const required = Math.floor(run.comments || 0);

  if (required <= 0) {
    return { time: run.at.toISOString(), comments: "" };
  }

  let finalComments = [];

  if (commentList.length === 0) {
    finalComments = ["Nice post"];
  } else if (commentList.length >= required) {
    finalComments = commentList.slice(0, required);
  } else {
    // duplicate
    while (finalComments.length < required) {
      finalComments.push(
        commentList[finalComments.length % commentList.length]
      );
    }
  }

  return {
    time: run.at.toISOString(),
    comments: finalComments.join("\n"),
  };
});
            const filteredCommentsRuns = commentsRuns.filter(run => run.comments && run.comments.length > 0);

            const servicesPayload: {
              views: { serviceId: string; runs: Array<{ time: string; quantity: number }> };
              likes?: { serviceId: string; runs: Array<{ time: string; quantity: number }> };
              shares?: { serviceId: string; runs: Array<{ time: string; quantity: number }> };
              saves?: { serviceId: string; runs: Array<{ time: string; quantity: number }> };
              comments?: { serviceId: string; runs: Array<{ time: string; comments: string }> };
            } = {
              views: { serviceId: viewsServiceId, runs: viewRuns },
            };

            if (includeLikes) servicesPayload.likes = { serviceId: likesServiceId, runs: likesRuns };
            if (includeShares) servicesPayload.shares = { serviceId: sharesServiceId, runs: sharesRuns };
            if (includeSaves) servicesPayload.saves = { serviceId: savesServiceId, runs: savesRuns };
            if (includeComments && filteredCommentsRuns.length > 0) {
  servicesPayload.comments = {
    serviceId: commentsServiceId,
    runs: filteredCommentsRuns,
  };
}
            setIsCreatingOrder(true);
            setCreateSuccess(`Processing ${targets.length} missions...`);
            
            const batchId = targets.length > 1 ? `batch-${Date.now()}` : undefined;
            
            try {
              const activeLinks = new Set(
                orders
                  .filter((order) => {
                    const now = Date.now();
                    const runs = order.runs || [];
                    if (!runs.length) return false;
                    const allRunsCompleted = runs.every((run) => new Date(run.at).getTime() <= now);
                    return !allRunsCompleted && order.status !== "cancelled";
                  })
                  .map((order) => order.link.replace(/\/+$/, "").toLowerCase())
              );
              const createdLinks = new Set<string>();
              let successCount = 0;
              let failedCount = 0;
              let lastError = "";

              for (let index = 0; index < targets.length; index += 1) {
                const trimmedUrl = targets[index];
                const normalizedTarget = trimmedUrl.replace(/\/+$/, "").toLowerCase();
                if (activeLinks.has(normalizedTarget) || createdLinks.has(normalizedTarget)) {
                  failedCount += 1;
                  lastError = "Duplicate link.";
                  continue;
                }

                try {
                  const result = await createSmmOrder({
                    name: orderName.trim() || undefined,
                    apiUrl: selectedApi.url,
                    apiKey: selectedApi.key,
                    link: trimmedUrl,
                    services: servicesPayload,
                  });

                  const order: CreatedOrder = {
                    id: createOrderId(),
                    name: orderName.trim() || `Mission #${createOrderId()}`,
                    batchId,
                    batchIndex: index + 1,
                    batchTotal: targets.length,
                    schedulerOrderId: result.schedulerOrderId,
                    smmOrderId: result.orderId ?? "Scheduled",
                    link: trimmedUrl,
                    totalViews: quantity,
                    startDelayHours,
                    patternType: safePlan.patternType,
                    patternName: safePlan.patternName,
                    runs: safePlan?.runs || [],
                    engagement: { likes: totalLikes, shares: totalShares, saves: totalSaves, comments: totalCommentsQty },
                    serviceId: viewsServiceId,
                    selectedAPI: selectedApi.name,
                    selectedBundle: selectedBundle.name,
                    status: result.status === "completed" ? "completed" : "running",
                    completedRuns: typeof result.completedRuns === "number" ? result.completedRuns : 0,
                    runStatuses: (safePlan?.runs || []).map(() => "pending"),
                    createdAt: new Date().toISOString(),
                    lastUpdatedAt: new Date().toISOString(),
                  };

                  onCreateOrder(order);
                  createdLinks.add(normalizedTarget);
                  successCount += 1;
                } catch (error) {
                  const message = error instanceof Error ? error.message : "Failed";
                  const failedOrder: CreatedOrder = {
                    id: createOrderId(),
                    name: orderName.trim() || `Mission #${createOrderId()}`,
                    batchId,
                    batchIndex: index + 1,
                    batchTotal: targets.length,
                    smmOrderId: "N/A",
                    link: trimmedUrl,
                    totalViews: quantity,
                    startDelayHours,
                    patternType: safePlan.patternType,
                    patternName: safePlan.patternName,
                    runs: safePlan?.runs || [],
                    engagement: { likes: totalLikes, shares: totalShares, saves: totalSaves, comments: totalCommentsQty },
                    serviceId: viewsServiceId,
                    selectedAPI: selectedApi.name,
                    selectedBundle: selectedBundle.name,
                    status: "failed",
                    completedRuns: 0,
                    runStatuses: (safePlan?.runs || []).map((_, i) => (i === 0 ? "cancelled" : "pending")),
                    runErrors: (safePlan?.runs || []).map((_, i) => (i === 0 ? message : "")),
                    errorMessage: message,
                    createdAt: new Date().toISOString(),
                    lastUpdatedAt: new Date().toISOString(),
                  };
                  onCreateOrder(failedOrder);
                  failedCount += 1;
                  lastError = message;
                }
              }

              if (failedCount > 0 && successCount === 0) {
                setCreateError(lastError || "Failed.");
                setCreateSuccess("");
                return;
              }

              const successLabel = targets.length > 1
                ? `Done: ${successCount}/${targets.length}`
                : "Mission Deployed ✅";
              setCreateSuccess(successLabel);
              if (failedCount > 0) setCreateError(`${failedCount} failed`);
              onNavigateToOrders(successLabel);
            } finally {
              setIsCreatingOrder(false);
            }
          }}
          className="whitespace-nowrap rounded-lg border border-yellow-500/50 bg-yellow-500/20 px-4 py-1.5 text-xs font-semibold text-yellow-300 transition hover:bg-yellow-500/30 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {isCreatingOrder ? "Deploying..." : "🦇 Deploy"}
        </button>
      </div>
    </div>
  );
}

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { motion } from "framer-motion";
import { APIsPage } from "./pages/APIsPage";
import { BundlesPage } from "./pages/BundlesPage";
import { DashboardPage } from "./pages/DashboardPage";
import { NewOrderPage } from "./pages/NewOrderPage";
import { OrdersPage } from "./pages/OrdersPage";
import type { ApiPanel, Bundle, CreatedOrder, RunStatus } from "./types/order";
import { fetchServices, updateOrderControl, fetchOrderRuns } from "./utils/api";
import { cn } from "./utils/cn";

type NavKey = "dashboard" | "new-order" | "orders" | "apis" | "bundles";

const NAV_ITEMS: { key: NavKey; label: string; icon: string }[] = [
  { key: "dashboard", label: "Dashboard", icon: "📊" },
  { key: "new-order", label: "New Order", icon: "⚡" },
  { key: "orders", label: "Orders", icon: "📦" },
  { key: "apis", label: "APIs", icon: "🔗" },
  { key: "bundles", label: "Bundles", icon: "📁" },
];

const BATMAN_QUOTES = [
  "It's not who I am underneath, but what I do that defines me.",
  "The night is darkest just before the dawn.",
  "I'm whatever Gotham needs me to be.",
  "A hero can be anyone.",
  "Why do we fall? So we can learn to pick ourselves up.",
  "It's not about what I want. It's about what's fair.",
  "Criminals are a superstitious, cowardly lot.",
  "I wear a mask. And that mask is not to hide who I am, but to create what I am.",
  "The training is nothing! The will is everything!",
  "Sometimes the truth isn't good enough. Sometimes people deserve more.",
  "I won't kill you, but I don't have to save you.",
  "You either die a hero or live long enough to see yourself become the villain.",
  "Endure. You can be the outcast. You can be the one they all turn against.",
  "People need dramatic examples to shake them out of apathy.",
  "Everything's impossible until somebody does it.",
  "I am vengeance. I am the night. I am Batman.",
  "The world only makes sense if you force it to.",
  "It's not about deserve. It's about what you believe.",
  "You don't get heaven or hell. Do you know the only reward you get for being Batman? You get to be Batman.",
  "I have one power. I never give up.",
  "If you make yourself more than just a man, you become something else entirely.",
  "Legends don't burn down villages.",
  "You're much stronger than you think you are. Trust me.",
  "A vigilante is just a man lost in the scramble for his own gratification.",
  "I'm not going to kill you. I want you to tell all your friends about me.",
  "All men have limits. They learn what they are and learn not to exceed them. I ignore mine.",
  "Sometimes it's only madness that makes us what we are.",
  "It's not who you are underneath, it's what you do that defines you.",
  "The world doesn't make sense until you force it to.",
  "Success is stumbling from failure to failure with no loss of enthusiasm.",
];

function getRandomQuote() {
  return BATMAN_QUOTES[Math.floor(Math.random() * BATMAN_QUOTES.length)];
}

function readStorage<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T) : fallback;
  } catch {
    return fallback;
  }
}

function hydrateOrderDates(orders: CreatedOrder[]): CreatedOrder[] {
  return (orders || []).map((order) => {
    const safeRuns = Array.isArray(order?.runs)
  ? order.runs.map((run, index) => ({
      run: Number.isFinite(run?.run) ? run.run : index + 1,
      at: run?.at ? new Date(run.at) : new Date(),
      minutesFromStart: Number.isFinite(run?.minutesFromStart) ? run.minutesFromStart : 0,

      views: Number.isFinite(run?.views) ? run.views : 0,
      likes: Number.isFinite(run?.likes) ? run.likes : 0,
      shares: Number.isFinite(run?.shares) ? run.shares : 0,
      saves: Number.isFinite(run?.saves) ? run.saves : 0,
      comments: Number.isFinite(run?.comments) ? run.comments : 0, // ✅ ADD

      cumulativeViews: Number.isFinite(run?.cumulativeViews) ? run.cumulativeViews : 0,
      cumulativeLikes: Number.isFinite(run?.cumulativeLikes) ? run.cumulativeLikes : 0,
      cumulativeShares: Number.isFinite(run?.cumulativeShares) ? run.cumulativeShares : 0,
      cumulativeSaves: Number.isFinite(run?.cumulativeSaves) ? run.cumulativeSaves : 0,
      cumulativeComments: Number.isFinite(run?.cumulativeComments) ? run.cumulativeComments : 0, // ✅ ADD
    }))
      : [];

    const safeRunStatuses: RunStatus[] = Array.isArray(order?.runStatuses)
      ? safeRuns.map((_, index) => {
          const next = order.runStatuses[index];
          return next === "completed" || next === "cancelled" || next === "retrying" ? next : "pending";
        })
      : safeRuns.map(() => "pending");
    const safeRunErrors = Array.isArray(order?.runErrors)
      ? safeRuns.map((_, index) => order.runErrors?.[index] ?? "")
      : safeRuns.map(() => "");

    return {
      ...order,
      name: order?.name || `Order #${order?.id ?? Date.now()}`,
      smmOrderId: order?.smmOrderId ?? "N/A",
      serviceId: order?.serviceId ?? "N/A",
      status:
        order?.status === "failed" ||
        order?.status === "paused" ||
        order?.status === "cancelled" ||
        order?.status === "completed" ||
        order?.status === "running" ||
        order?.status === "processing"
          ? order.status
          : "running",
      completedRuns: Number.isFinite(order?.completedRuns) ? order.completedRuns : 0,
      runStatuses: safeRunStatuses,
      runErrors: safeRunErrors,
      runRetries: order?.runRetries || [],
      runOriginalTimes: order?.runOriginalTimes || [],
      runCurrentTimes: order?.runCurrentTimes || [],
      runReasons: order?.runReasons || [],
      lastUpdatedAt: order?.lastUpdatedAt ?? order?.createdAt ?? new Date().toISOString(),
      runs: safeRuns,
    };
  });
}

function hydrateApis(apis: ApiPanel[]): ApiPanel[] {
  return apis.map((api) => ({
    ...api,
    services: Array.isArray(api.services) ? api.services : [],
    lastFetchError: api.lastFetchError,
    lastFetchAt: api.lastFetchAt,
  }));
}

function hydrateBundles(bundles: Bundle[]): Bundle[] {
  return bundles.map((bundle) => ({
    ...bundle,
    apiId: bundle.apiId ?? "",
  }));
}

export default function App() {
  const [activePage, setActivePage] = useState<NavKey>(() => {
    const saved = localStorage.getItem("dev-smm-active-page");
    if (saved === "dashboard" || saved === "new-order" || saved === "orders" || saved === "apis" || saved === "bundles") {
      return saved;
    }
    return "new-order";
  });

  const [ordersNotice, setOrdersNotice] = useState("");
  const [orders, setOrders] = useState<CreatedOrder[]>(() => hydrateOrderDates(readStorage<CreatedOrder[]>("dev-smm-orders", [])));
  const [apis, setApis] = useState<ApiPanel[]>(() => hydrateApis(readStorage<ApiPanel[]>("dev-smm-apis", [])));
  const [bundles, setBundles] = useState<Bundle[]>(() => hydrateBundles(readStorage<Bundle[]>("dev-smm-bundles", [])));
  const [cloneSourceOrder, setCloneSourceOrder] = useState<CreatedOrder | null>(null);
  const [fetchingApiId, setFetchingApiId] = useState<string | null>(null);
  const [controllingOrderId, setControllingOrderId] = useState<string | null>(null);
  
  const [batmanQuote] = useState(() => getRandomQuote());

  // 🔥 NEW: Track if sync is in progress to prevent render loops
  const isSyncingRef = useRef(false);
  const lastSyncTimeRef = useRef(0);

  const navigateToPage = useCallback((page: NavKey) => {
    setActivePage(page);
    localStorage.setItem("dev-smm-active-page", page);
  }, []);

  const persistOrders = useCallback((next: CreatedOrder[] | ((prev: CreatedOrder[]) => CreatedOrder[])) => {
    if (typeof next === 'function') {
      setOrders((prev) => {
        const updated = next(prev);
        localStorage.setItem("dev-smm-orders", JSON.stringify(updated));
        return updated;
      });
    } else {
      setOrders(next);
      localStorage.setItem("dev-smm-orders", JSON.stringify(next));
    }
  }, []);

  const persistApis = useCallback((next: ApiPanel[]) => {
    setApis(next);
    localStorage.setItem("dev-smm-apis", JSON.stringify(next));
  }, []);

  const persistBundles = useCallback((next: Bundle[]) => {
    setBundles(next);
    localStorage.setItem("dev-smm-bundles", JSON.stringify(next));
  }, []);

  // 🔥 IMPROVED: Smarter sync that prevents render loops
  const syncOrdersWithBackend = useCallback(async () => {
    // Prevent concurrent syncs
    if (isSyncingRef.current) {
      console.log('[Sync] Already syncing, skipping...');
      return;
    }

    // Prevent syncing too frequently (debounce)
    const now = Date.now();
    const timeSinceLastSync = now - lastSyncTimeRef.current;
    if (timeSinceLastSync < 10000) { // Min 10 seconds between syncs
      console.log('[Sync] Too soon since last sync, skipping...');
      return;
    }

    isSyncingRef.current = true;
    lastSyncTimeRef.current = now;

    try {
      // Get current orders from localStorage (fresh copy)
      const currentOrders = hydrateOrderDates(readStorage<CreatedOrder[]>("dev-smm-orders", []));

      const activeOrders = currentOrders.filter(
        order => order.schedulerOrderId && 
        (order.status === "running" || order.status === "processing" || order.status === "paused")
      );

      if (activeOrders.length === 0) {
        console.log('[Sync] No active orders to sync');
        return;
      }

      console.log(`[Sync] Syncing ${activeOrders.length} active orders...`);

      const updates: Array<{ orderId: string; data: Partial<CreatedOrder> }> = [];

      for (const order of activeOrders) {
        try {
          const result = await fetchOrderRuns(order.schedulerOrderId!);
          
          const runRetries: number[] = [];
          const runOriginalTimes: string[] = [];
          const runCurrentTimes: string[] = [];
          const runReasons: string[] = [];
          const runStatuses: RunStatus[] = [];
          const runErrors: string[] = [];

          result.runs.forEach((backendRun) => {
            runRetries.push(backendRun.retryCount || 0);
            runOriginalTimes.push(backendRun.originalTime);
            runCurrentTimes.push(backendRun.currentTime);
            runReasons.push(backendRun.retryReason || "");
            runErrors.push(backendRun.lastError || "");

            if (backendRun.cancelled) {
              runStatuses.push("cancelled");
            } else if (backendRun.done) {
              runStatuses.push("completed");
            } else if (backendRun.retryCount > 0) {
              runStatuses.push("retrying");
            } else {
              runStatuses.push("pending");
            }
          });

          const completedRuns = runStatuses.filter(s => s === "completed").length;

          updates.push({
            orderId: order.id,
            data: {
              runRetries,
              runOriginalTimes,
              runCurrentTimes,
              runReasons,
              runStatuses,
              runErrors,
              completedRuns,
              lastUpdatedAt: new Date().toISOString(),
            },
          });
        } catch (error) {
          console.error(`[Sync] Failed to sync order ${order.id}:`, error);
        }
      }

      if (updates.length > 0) {
        persistOrders((prev) =>
          prev.map((order) => {
            const update = updates.find((u) => u.orderId === order.id);
            return update ? { ...order, ...update.data } : order;
          })
        );
        console.log(`[Sync] ✅ Updated ${updates.length} orders`);
      }
    } catch (error) {
      console.error('[Sync] Error:', error);
    } finally {
      isSyncingRef.current = false;
    }
  }, [persistOrders]);

  // 🔥 FIXED: Auto-sync every 5 MINUTES (300 seconds) - Only when on Orders page
  useEffect(() => {
    // Only sync when on orders or dashboard page
    if (activePage !== 'orders' && activePage !== 'dashboard') {
      console.log('[Sync] Not on orders/dashboard page, skipping sync setup');
      return;
    }

    console.log('[Sync] Setting up 5-minute auto-sync...');

    // Initial sync after 10 seconds
    const initialSync = setTimeout(() => {
      syncOrdersWithBackend();
    }, 10000);

    // Then sync every 5 minutes
    const interval = setInterval(() => {
      syncOrdersWithBackend();
    }, 300000); // 🔥 5 MINUTES (300,000 milliseconds)

    return () => {
      clearTimeout(initialSync);
      clearInterval(interval);
    };
  }, [activePage]); // 🔥 Only re-setup when page changes

  const content = useMemo(() => {
    if (activePage === "new-order") {
      return (
        <NewOrderPage
          apis={apis}
          bundles={bundles}
          orders={orders}
          prefillOrder={cloneSourceOrder}
          onCreateOrder={(order) => persistOrders((prev) => [order, ...prev])}
          onNavigateToOrders={(notice) => {
            if (notice) setOrdersNotice(notice);
            navigateToPage("orders");
          }}
        />
      );
    }
    if (activePage === "dashboard") {
      return <DashboardPage orders={orders} />;
    }
    if (activePage === "orders") {
      return (
        <OrdersPage
          orders={orders}
          notice={ordersNotice}
          controllingOrderId={controllingOrderId}
          onCloneOrder={(order) => {
            setCloneSourceOrder(order);
            navigateToPage("new-order");
          }}
          onControlOrder={async (order, action) => {
            const applyLocalUpdate = (nextStatus: CreatedOrder["status"]) => {
              persistOrders((prev) =>
                prev.map((item) => {
                  if (item.id !== order.id) return item;
                  if (nextStatus === "cancelled") {
                    const nextRunStatuses = item.runStatuses.map((status) => (status === "pending" || status === "retrying" ? "cancelled" : status));
                    const completedRuns = nextRunStatuses.filter((status) => status === "completed").length;
                    return {
                      ...item,
                      status: nextStatus,
                      runStatuses: nextRunStatuses,
                      completedRuns,
                      lastUpdatedAt: new Date().toISOString(),
                    };
                  }
                  return {
                    ...item,
                    status: nextStatus,
                    lastUpdatedAt: new Date().toISOString(),
                  };
                })
              );
            };

            setControllingOrderId(order.id);
            try {
              if (order.schedulerOrderId) {
                const result = await updateOrderControl({
                  schedulerOrderId: order.schedulerOrderId,
                  action,
                });
                const nextStatus =
                  result.status || (action === "pause" ? "paused" : action === "resume" ? "running" : "cancelled");
                persistOrders((prev) =>
                  prev.map((item) => {
                    if (item.id !== order.id) return item;
                    return {
                      ...item,
                      status: nextStatus,
                      completedRuns: typeof result.completedRuns === "number" ? result.completedRuns : item.completedRuns,
                      runStatuses: result.runStatuses ?? item.runStatuses,
                      lastUpdatedAt: new Date().toISOString(),
                    };
                  })
                );
                // 🔥 Sync immediately after control action
                setTimeout(() => syncOrdersWithBackend(), 2000);
              } else {
                applyLocalUpdate(action === "pause" ? "paused" : action === "resume" ? "running" : "cancelled");
              }
            } catch {
              applyLocalUpdate(action === "pause" ? "paused" : action === "resume" ? "running" : "cancelled");
            } finally {
              setControllingOrderId(null);
            }
          }}
          onDismissNotice={() => setOrdersNotice("")}
        />
      );
    }
    if (activePage === "apis") {
      return (
        <APIsPage
          apis={apis}
          onAddApi={(api) => {
            const next: ApiPanel[] = [
              ...apis,
              {
                id: `api-${Date.now()}`,
                name: api.name,
                url: api.url,
                key: api.key,
                status: "Active",
                services: [],
              },
            ];
            persistApis(next);
          }}
          onEditApi={(id, api) => {
            const next: ApiPanel[] = apis.map((item) =>
              item.id === id
                ? { ...item, name: api.name, url: api.url, key: api.key }
                : item
            );
            persistApis(next);
          }}
          onDeleteApi={(id) => {
            const next = apis.filter((api) => api.id !== id);
            persistApis(next);
          }}
          onToggleStatus={(id) => {
            const next: ApiPanel[] = apis.map((api) =>
              api.id === id ? { ...api, status: api.status === "Active" ? "Inactive" : "Active" } : api
            );
            persistApis(next);
          }}
          onFetchServices={async (id) => {
            const targetApi = apis.find((api) => api.id === id);
            if (!targetApi) return;

            setFetchingApiId(id);
            try {
              const services = await fetchServices(targetApi.url, targetApi.key);
              const next = apis.map((api) =>
                api.id === id
                  ? {
                      ...api,
                      services,
                      lastFetchAt: new Date().toISOString(),
                      lastFetchError: undefined,
                    }
                  : api
              );
              persistApis(next);
            } catch (error) {
              const message = error instanceof Error ? error.message : "Failed to fetch services";
              const next = apis.map((api) =>
                api.id === id
                  ? {
                      ...api,
                      lastFetchError: message,
                    }
                  : api
              );
              persistApis(next);
            } finally {
              setFetchingApiId(null);
            }
          }}
          fetchingApiId={fetchingApiId}
        />
      );
    }
    return (
      <BundlesPage
        apis={apis}
        bundles={bundles}
        onAddBundle={(bundle) => {
          const next: Bundle[] = [
            ...bundles,
            {
              id: `bundle-${Date.now()}`,
              apiId: bundle.apiId,
              name: bundle.name,
              serviceIds: {
  views: bundle.views,
  likes: bundle.likes,
  shares: bundle.shares,
  saves: bundle.saves,
  comments: bundle.comments, // ✅ ADD THIS
},
            },
          ];
          persistBundles(next);
        }}
        onUpdateBundle={(id, bundle) => {
          const next: Bundle[] = bundles.map((item) =>
            item.id === id
              ? {
                  ...item,
                  apiId: bundle.apiId,
                  name: bundle.name,
                  serviceIds: {
  views: bundle.views,
  likes: bundle.likes,
  shares: bundle.shares,
  saves: bundle.saves,
  comments: bundle.comments, // ✅ ADD THIS
},
                }
              : item
          );
          persistBundles(next);
        }}
        onDeleteBundle={(id) => {
          const next = bundles.filter((bundle) => bundle.id !== id);
          persistBundles(next);
        }}
      />
    );
  }, [activePage, apis, bundles, orders, fetchingApiId, controllingOrderId, ordersNotice, cloneSourceOrder, navigateToPage, persistOrders, persistApis, persistBundles, syncOrdersWithBackend]);

  return (
    <div className="min-h-screen bg-black text-gray-100">
      <div className="flex min-h-screen">
        <aside className="w-64 border-r border-yellow-500/20 bg-gradient-to-b from-gray-950 to-black p-6">
          <div className="mb-8 space-y-1">
            <div className="flex items-center gap-3">
              <div className="relative">
                <div className="absolute inset-0 animate-ping rounded-full bg-yellow-500/20" style={{ animationDuration: '3s' }} />
                <span className="relative text-3xl">🦇</span>
              </div>
              <div>
                <h1 className="text-xl font-bold tracking-tight text-yellow-400">GOTHAM</h1>
                <p className="text-xs text-yellow-600">SMM Command Center</p>
              </div>
            </div>
          </div>

          <nav className="space-y-2">
            {NAV_ITEMS.map((item) => {
              const isActive = activePage === item.key;
              return (
                <button
                  key={item.key}
                  type="button"
                  onClick={() => {
                    if (item.key === "new-order") {
                      setCloneSourceOrder(null);
                    }
                    navigateToPage(item.key);
                  }}
                  className={cn(
                    "relative flex w-full items-center gap-3 rounded-xl px-4 py-3 text-left text-sm font-medium transition-all",
                    isActive 
                      ? "bg-yellow-500/20 text-yellow-400 shadow-lg shadow-yellow-500/10" 
                      : "text-gray-400 hover:bg-yellow-500/10 hover:text-yellow-300"
                  )}
                >
                  {isActive && (
                    <motion.span
                      layoutId="active-nav"
                      className="absolute inset-0 rounded-xl border border-yellow-500/50"
                      transition={{ type: "spring", stiffness: 280, damping: 28 }}
                    />
                  )}
                  <span className="relative text-lg">{item.icon}</span>
                  <span className="relative">{item.label}</span>
                </button>
              );
            })}
          </nav>

          <motion.div 
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.2 }}
            className="mt-8 rounded-xl border border-yellow-500/20 bg-yellow-500/5 p-4"
          >
            <div className="mb-2 flex items-center gap-2">
              <span className="text-yellow-400">🦇</span>
              <span className="text-[10px] font-medium uppercase tracking-wider text-yellow-600">
                Quote of the Visit
              </span>
            </div>
            <p className="text-xs italic leading-relaxed text-yellow-500/70">
              "{batmanQuote}"
            </p>
            <p className="mt-2 text-right text-[10px] font-medium text-yellow-600">— Batman</p>
          </motion.div>

          <div className="mt-4 rounded-lg border border-gray-800 bg-black/50 px-3 py-2 text-center">
            <p className="text-[10px] text-gray-600">Auto-syncs every 5 min ⚡</p>
          </div>
        </aside>

        <main className="flex-1 overflow-y-auto bg-gradient-to-br from-gray-950 via-black to-gray-950">{content}</main>
      </div>
    </div>
  );
}

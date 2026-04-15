export type PatternType =
  | "smooth-s-curve"
  | "rocket-launch"
  | "sunset-fade"
  | "viral-spike"
  | "micro-burst"
  | "heartbeat"
  | "sawtooth"
  | "fibonacci-spiral"
  | "manual";

export type QuickPatternPreset = "viral-boost" | "fast-start" | "trending-push" | "slow-burn";

export interface DeliveryOption {
  mode: "auto" | "preset" | "custom";
  hours: number;
  label: string;
}

export interface OrderConfig {
  postUrl: string;
  totalViews: number;
  startDelayHours: number;
  includeLikes: boolean;
  includeShares: boolean;
  includeSaves: boolean;
  includeComments: boolean;
  variancePercent: number;
  peakHoursBoost: boolean;
  quickPreset: QuickPatternPreset | null;
  delivery: DeliveryOption;
  minViewsPerRun: number; // 🔥 NEW: Dynamic minimum views per run
}

export interface RunStep {
  run: number;
  at: Date;
  minutesFromStart: number;
  views: number;
  likes: number;
  shares: number;
  saves: number;
  comments: number;
  cumulativeViews: number;
  cumulativeLikes: number;
  cumulativeShares: number;
  cumulativeSaves: number;
  cumulativeComments: number;
}

export interface PatternPlan {
  patternId: number;
  patternName: string;
  patternType: PatternType;
  totalRuns: number;
  approximateIntervalMin: number;
  finishTime: Date;
  estimatedDurationHours: number;
  risk: "Safe" | "Medium" | "Risk";
  runs: RunStep[];
}

export type OrderStatus = "running" | "paused" | "cancelled" | "completed" | "processing" | "failed";

export type RunStatus = "pending" | "completed" | "cancelled" | "retrying";

export interface ApiService {
  id: string;
  name: string;
  type: string;
  rate: string;
  min: number;
  max: number;
}

export interface ApiPanel {
  id: string;
  name: string;
  url: string;
  key: string;
  status: "Active" | "Inactive";
  services: ApiService[];
  lastFetchAt?: string;
  lastFetchError?: string;
}

export interface Bundle {
  id: string;
  apiId: string;
  name: string;
  serviceIds: {
  views: string;
  likes: string;
  shares: string;
  saves: string;
  comments: string;
};
}

export interface BackendRunInfo {
  id: string;
  label: string;
  quantity: number;
  originalTime: string;
  currentTime: string;
  done: boolean;
  cancelled: boolean;
  paused: boolean;
  isExecuting: boolean;
  retryCount: number;
  retryReason: string | null;
  lastError: string | null;
  executedAt: string | null;
  smmOrderId: string | null;
}

export interface CreatedOrder {
  id: string;
  name: string;
  batchId?: string;
  batchIndex?: number;
  batchTotal?: number;
  schedulerOrderId?: string;
  smmOrderId: string;
  link: string;
  totalViews: number;
  startDelayHours: number;
  patternType: PatternType;
  patternName: string;
  runs: RunStep[];
  engagement: {
    likes: number;
    shares: number;
    saves: number;
  };
  serviceId: string;
  selectedAPI: string | null;
  selectedBundle: string;
  status: OrderStatus;
  completedRuns: number;
  runStatuses: RunStatus[];
  runErrors?: string[];
  runRetries?: number[];
  runOriginalTimes?: string[];
  runCurrentTimes?: string[];
  runReasons?: string[];
  errorMessage?: string;
  createdAt: string;
  lastUpdatedAt?: string;
}

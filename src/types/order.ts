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

export type QuickPatternPreset =
  | "viral-boost"
  | "fast-start"
  | "trending-push"
  | "slow-burn";

export interface DeliveryOption {
  mode: "auto" | "preset" | "custom";
  hours: number;
  label: string;
}

// 🔥 Single run in a pattern plan (frontend side)
export interface PatternRun {
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
  patternType:
    | "smooth-s-curve"
    | "viral-spike"
    | "steady-climb"
    | "wave-pattern"
    | "exponential"
    | "natural-decay";
  totalRuns: number;
  approximateIntervalMin: number;
  finishTime: Date;
  estimatedDurationHours: number;
  risk: "Safe" | "Medium" | "High";
  runs: PatternRun[];
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
  minViewsPerRun: number;
}

// 🔥 FIXED: BackendRunInfo now matches ACTUAL backend response fields exactly
export interface BackendRunInfo {
  id: number;
  label: string;         // "VIEWS" | "LIKES" | "SHARES" | "SAVES" | "COMMENTS"
  quantity: number;
  time: string;          // ISO string
  status: string;        // "pending" | "queued" | "processing" | "completed" | "failed" | "cancelled" | "paused"
  smmOrderId: number | null;
  executedAt: string | null;
  error: string | null;
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
  patternType: PatternPlan["patternType"];
  patternName: string;
  runs: PatternRun[];
  engagement: {
    likes: number;
    shares: number;
    saves: number;
    comments: number;
  };
  serviceId: string;
  selectedAPI: string;
  selectedBundle: string;
  status:
    | "running"
    | "paused"
    | "cancelled"
    | "completed"
    | "failed"
    | "processing"
    | "pending";
  completedRuns: number;
  runStatuses: RunStatus[];
  runErrors?: string[];
  runRetries?: number[];
  runOriginalTimes?: string[];
  runCurrentTimes?: string[];
  runReasons?: string[];
  errorMessage?: string;
  createdAt: string;
  lastUpdatedAt: string;

  // 🔥 NEW: Backend run details (populated during sync)
  backendRuns?: BackendRunInfo[];
}

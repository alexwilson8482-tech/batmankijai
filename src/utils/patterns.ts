import type { OrderConfig, PatternPlan, PatternType, QuickPatternPreset, RunStep } from "../types/order";

const PATTERN_TYPES: PatternType[] = [
  "smooth-s-curve",
  "rocket-launch",
  "sunset-fade",
  "viral-spike",
  "micro-burst",
  "heartbeat",
  "sawtooth",
  "fibonacci-spiral",
];

interface OrganicPatternProfile {
  key: string;
  name: string;
  baseType: PatternType;
  runMultiplier: number;
  durationMultiplier: number;
  earlyBand: [number, number];
  midBand: [number, number];
  lateBand: [number, number];
  midSpikeChance: number;
  spikeBand: [number, number];
  dipChance: number;
  dipBand: [number, number];
  waveAmplitude: number;
}

interface OrganicPatternVariant {
  earlyBand: [number, number];
  midBand: [number, number];
  lateBand: [number, number];
  midSpikeChance: number;
  spikeBand: [number, number];
  dipChance: number;
  dipBand: [number, number];
  waveAmplitude: number;
  waveFrequency: number;
  timingShift: number;
}

const BASE_ORGANIC_PATTERN_LIBRARY: OrganicPatternProfile[] = [
  {
    key: "slow-growth",
    name: "slow-growth",
    baseType: "smooth-s-curve",
    runMultiplier: 1.12,
    durationMultiplier: 1.16,
    earlyBand: [0.74, 0.92],
    midBand: [1.0, 1.14],
    lateBand: [0.84, 1.03],
    midSpikeChance: 0.08,
    spikeBand: [1.1, 1.22],
    dipChance: 0.06,
    dipBand: [0.84, 0.94],
    waveAmplitude: 0.02,
  },
  {
    key: "viral-spike",
    name: "viral-spike",
    baseType: "viral-spike",
    runMultiplier: 0.9,
    durationMultiplier: 0.92,
    earlyBand: [0.76, 0.96],
    midBand: [1.06, 1.34],
    lateBand: [0.82, 1.02],
    midSpikeChance: 0.26,
    spikeBand: [1.22, 1.56],
    dipChance: 0.08,
    dipBand: [0.78, 0.92],
    waveAmplitude: 0.035,
  },
  {
    key: "delayed-explosion",
    name: "delayed-explosion",
    baseType: "sunset-fade",
    runMultiplier: 0.96,
    durationMultiplier: 1.02,
    earlyBand: [0.7, 0.88],
    midBand: [0.94, 1.18],
    lateBand: [1.06, 1.3],
    midSpikeChance: 0.15,
    spikeBand: [1.14, 1.34],
    dipChance: 0.06,
    dipBand: [0.82, 0.94],
    waveAmplitude: 0.03,
  },
  {
    key: "wave-pattern",
    name: "wave-pattern",
    baseType: "heartbeat",
    runMultiplier: 1.02,
    durationMultiplier: 1,
    earlyBand: [0.82, 1.02],
    midBand: [0.96, 1.24],
    lateBand: [0.82, 1.06],
    midSpikeChance: 0.12,
    spikeBand: [1.1, 1.3],
    dipChance: 0.1,
    dipBand: [0.76, 0.92],
    waveAmplitude: 0.06,
  },
  {
    key: "plateau-growth",
    name: "plateau-growth",
    baseType: "sawtooth",
    runMultiplier: 1.08,
    durationMultiplier: 1.1,
    earlyBand: [0.86, 1.02],
    midBand: [0.94, 1.12],
    lateBand: [0.86, 1.04],
    midSpikeChance: 0.06,
    spikeBand: [1.08, 1.18],
    dipChance: 0.05,
    dipBand: [0.86, 0.95],
    waveAmplitude: 0.018,
  },
  {
    key: "drop-recovery",
    name: "sudden-drop-recovery",
    baseType: "heartbeat",
    runMultiplier: 1,
    durationMultiplier: 1,
    earlyBand: [0.88, 1.1],
    midBand: [0.82, 1.3],
    lateBand: [0.88, 1.18],
    midSpikeChance: 0.1,
    spikeBand: [1.12, 1.24],
    dipChance: 0.16,
    dipBand: [0.68, 0.88],
    waveAmplitude: 0.052,
  },
  {
    key: "exponential-growth",
    name: "exponential-growth",
    baseType: "fibonacci-spiral",
    runMultiplier: 0.88,
    durationMultiplier: 0.95,
    earlyBand: [0.72, 0.88],
    midBand: [1.0, 1.22],
    lateBand: [1.06, 1.34],
    midSpikeChance: 0.14,
    spikeBand: [1.14, 1.3],
    dipChance: 0.05,
    dipBand: [0.84, 0.94],
    waveAmplitude: 0.022,
  },
  {
    key: "organic-spread",
    name: "random-organic-spread",
    baseType: "micro-burst",
    runMultiplier: 1,
    durationMultiplier: 1,
    earlyBand: [0.78, 1.02],
    midBand: [0.96, 1.3],
    lateBand: [0.82, 1.06],
    midSpikeChance: 0.18,
    spikeBand: [1.12, 1.36],
    dipChance: 0.12,
    dipBand: [0.74, 0.92],
    waveAmplitude: 0.05,
  },
  {
    key: "multi-spike-viral",
    name: "multi-spike-viral",
    baseType: "viral-spike",
    runMultiplier: 0.92,
    durationMultiplier: 0.9,
    earlyBand: [0.76, 0.98],
    midBand: [1.08, 1.42],
    lateBand: [0.8, 1],
    midSpikeChance: 0.3,
    spikeBand: [1.24, 1.64],
    dipChance: 0.08,
    dipBand: [0.78, 0.9],
    waveAmplitude: 0.034,
  },
  {
    key: "gradual-decay",
    name: "gradual-decay",
    baseType: "rocket-launch",
    runMultiplier: 1.04,
    durationMultiplier: 1.08,
    earlyBand: [0.98, 1.22],
    midBand: [0.92, 1.12],
    lateBand: [0.74, 0.96],
    midSpikeChance: 0.08,
    spikeBand: [1.06, 1.22],
    dipChance: 0.12,
    dipBand: [0.72, 0.9],
    waveAmplitude: 0.02,
  },
  {
    key: "weekend-burst",
    name: "weekend-burst",
    baseType: "micro-burst",
    runMultiplier: 0.95,
    durationMultiplier: 0.84,
    earlyBand: [0.84, 1.02],
    midBand: [1.04, 1.32],
    lateBand: [0.86, 1.08],
    midSpikeChance: 0.2,
    spikeBand: [1.2, 1.45],
    dipChance: 0.08,
    dipBand: [0.8, 0.93],
    waveAmplitude: 0.042,
  },
  {
    key: "late-night-wave",
    name: "late-night-wave",
    baseType: "heartbeat",
    runMultiplier: 1.06,
    durationMultiplier: 1.18,
    earlyBand: [0.78, 0.95],
    midBand: [0.94, 1.18],
    lateBand: [0.96, 1.24],
    midSpikeChance: 0.1,
    spikeBand: [1.1, 1.24],
    dipChance: 0.1,
    dipBand: [0.76, 0.92],
    waveAmplitude: 0.058,
  },
  {
    key: "morning-ramp",
    name: "morning-ramp",
    baseType: "smooth-s-curve",
    runMultiplier: 1,
    durationMultiplier: 0.9,
    earlyBand: [0.74, 0.9],
    midBand: [1.02, 1.26],
    lateBand: [0.9, 1.08],
    midSpikeChance: 0.12,
    spikeBand: [1.14, 1.34],
    dipChance: 0.07,
    dipBand: [0.8, 0.94],
    waveAmplitude: 0.03,
  },
  {
    key: "lunch-hour-surge",
    name: "lunch-hour-surge",
    baseType: "viral-spike",
    runMultiplier: 0.96,
    durationMultiplier: 0.88,
    earlyBand: [0.8, 0.98],
    midBand: [1.08, 1.36],
    lateBand: [0.84, 1.04],
    midSpikeChance: 0.24,
    spikeBand: [1.2, 1.52],
    dipChance: 0.08,
    dipBand: [0.78, 0.9],
    waveAmplitude: 0.034,
  },
  {
    key: "double-plateau",
    name: "double-plateau",
    baseType: "sawtooth",
    runMultiplier: 1.1,
    durationMultiplier: 1.14,
    earlyBand: [0.86, 1.04],
    midBand: [0.92, 1.12],
    lateBand: [0.88, 1.04],
    midSpikeChance: 0.05,
    spikeBand: [1.06, 1.18],
    dipChance: 0.05,
    dipBand: [0.86, 0.94],
    waveAmplitude: 0.016,
  },
  {
    key: "staggered-spike",
    name: "staggered-spike",
    baseType: "micro-burst",
    runMultiplier: 0.94,
    durationMultiplier: 0.92,
    earlyBand: [0.8, 0.98],
    midBand: [1.04, 1.32],
    lateBand: [0.84, 1],
    midSpikeChance: 0.22,
    spikeBand: [1.18, 1.48],
    dipChance: 0.08,
    dipBand: [0.78, 0.9],
    waveAmplitude: 0.038,
  },
  {
    key: "quiet-then-boom",
    name: "quiet-then-boom",
    baseType: "sunset-fade",
    runMultiplier: 0.9,
    durationMultiplier: 1,
    earlyBand: [0.66, 0.84],
    midBand: [0.94, 1.16],
    lateBand: [1.12, 1.38],
    midSpikeChance: 0.2,
    spikeBand: [1.18, 1.44],
    dipChance: 0.06,
    dipBand: [0.82, 0.92],
    waveAmplitude: 0.028,
  },
  {
    key: "echo-wave",
    name: "echo-wave",
    baseType: "heartbeat",
    runMultiplier: 1.03,
    durationMultiplier: 1.06,
    earlyBand: [0.84, 1.04],
    midBand: [0.96, 1.2],
    lateBand: [0.86, 1.1],
    midSpikeChance: 0.12,
    spikeBand: [1.1, 1.26],
    dipChance: 0.1,
    dipBand: [0.74, 0.9],
    waveAmplitude: 0.062,
  },
  {
    key: "arc-rise",
    name: "arc-rise",
    baseType: "fibonacci-spiral",
    runMultiplier: 0.96,
    durationMultiplier: 0.96,
    earlyBand: [0.76, 0.92],
    midBand: [0.98, 1.18],
    lateBand: [1.02, 1.28],
    midSpikeChance: 0.12,
    spikeBand: [1.12, 1.3],
    dipChance: 0.06,
    dipBand: [0.82, 0.92],
    waveAmplitude: 0.024,
  },
  {
    key: "momentum-shift",
    name: "momentum-shift",
    baseType: "rocket-launch",
    runMultiplier: 0.98,
    durationMultiplier: 0.94,
    earlyBand: [0.9, 1.18],
    midBand: [0.92, 1.16],
    lateBand: [0.8, 1.02],
    midSpikeChance: 0.14,
    spikeBand: [1.12, 1.34],
    dipChance: 0.09,
    dipBand: [0.76, 0.9],
    waveAmplitude: 0.026,
  },
];

const EXTRA_PATTERN_COUNT = 100;
const EXTRA_PATTERN_PREFIXES = [
  "aurora",
  "ember",
  "pulse",
  "ripple",
  "glide",
  "nova",
  "drift",
  "cascade",
  "surge",
  "orbit",
];
const EXTRA_PATTERN_SUFFIXES = [
  "arc",
  "lift",
  "trail",
  "burst",
  "echo",
  "crest",
  "flow",
  "flare",
  "wave",
  "rise",
];

function createGeneratedPattern(template: OrganicPatternProfile, index: number): OrganicPatternProfile {
  const seed = index + 1;
  const seeded = (offset: number) => {
    const value = Math.sin(seed * 12.9898 + offset * 78.233) * 43758.5453;
    return value - Math.floor(value);
  };
  const clampValue = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));
  const between = (min: number, max: number, offset: number) => min + seeded(offset) * (max - min);
  const tweak = (index % 13) / 100;
  const jitter = ((index * 7) % 9) / 100;
  const key = `${EXTRA_PATTERN_PREFIXES[Math.floor(index / EXTRA_PATTERN_SUFFIXES.length)]}-${EXTRA_PATTERN_SUFFIXES[index % EXTRA_PATTERN_SUFFIXES.length]}-${index + 1}`;

  return {
    key,
    name: key,
    baseType: template.baseType,
    runMultiplier: clampValue(template.runMultiplier + tweak - 0.06, 0.78, 1.28),
    durationMultiplier: clampValue(template.durationMultiplier + jitter - 0.04, 0.78, 1.3),
    earlyBand: [
      clampValue(template.earlyBand[0] + between(-0.06, 0.06, 1), 0.58, 1.12),
      clampValue(template.earlyBand[1] + between(-0.06, 0.08, 2), 0.66, 1.24),
    ],
    midBand: [
      clampValue(template.midBand[0] + between(-0.06, 0.08, 3), 0.72, 1.28),
      clampValue(template.midBand[1] + between(-0.05, 0.12, 4), 0.9, 1.54),
    ],
    lateBand: [
      clampValue(template.lateBand[0] + between(-0.06, 0.08, 5), 0.68, 1.2),
      clampValue(template.lateBand[1] + between(-0.04, 0.1, 6), 0.84, 1.42),
    ],
    midSpikeChance: clampValue(template.midSpikeChance + between(-0.05, 0.09, 7), 0.03, 0.42),
    spikeBand: [
      clampValue(template.spikeBand[0] + between(-0.08, 0.1, 8), 1.02, 1.5),
      clampValue(template.spikeBand[1] + between(-0.06, 0.16, 9), 1.12, 1.78),
    ],
    dipChance: clampValue(template.dipChance + between(-0.04, 0.08, 10), 0.02, 0.26),
    dipBand: [
      clampValue(template.dipBand[0] + between(-0.07, 0.05, 11), 0.62, 0.96),
      clampValue(template.dipBand[1] + between(-0.06, 0.06, 12), 0.74, 0.98),
    ],
    waveAmplitude: clampValue(template.waveAmplitude + between(-0.018, 0.03, 13), 0.012, 0.11),
  };
}

const GENERATED_ORGANIC_PATTERNS: OrganicPatternProfile[] = Array.from({ length: EXTRA_PATTERN_COUNT }, (_, index) => {
  const template = BASE_ORGANIC_PATTERN_LIBRARY[index % BASE_ORGANIC_PATTERN_LIBRARY.length];
  return createGeneratedPattern(template, index);
});

const ORGANIC_PATTERN_LIBRARY: OrganicPatternProfile[] = [
  ...BASE_ORGANIC_PATTERN_LIBRARY,
  ...GENERATED_ORGANIC_PATTERNS,
];

let lastPatternKey: string | null = null;

// 🔥 REMOVED: const MIN_VIEWS_PER_RUN = 100; (now comes from config)

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));
const random = (min: number, max: number) => Math.random() * (max - min) + min;
const randomInt = (min: number, max: number) => Math.floor(random(min, max + 1));

function pickRandomPatternType(): PatternType {
  return PATTERN_TYPES[randomInt(0, PATTERN_TYPES.length - 1)];
}

interface PresetProfile {
  patternType?: PatternType;
  runMultiplier: number;
  durationMultiplier: number;
  varianceMultiplier: number;
  targetAverageViews: number;
}

function resolvePresetProfile(preset: QuickPatternPreset | null): PresetProfile {
  if (preset === "viral-boost") {
    return { patternType: "viral-spike", runMultiplier: 0.8, durationMultiplier: 0.7, varianceMultiplier: 1.3, targetAverageViews: 220 };
  }
  if (preset === "fast-start") {
    return { patternType: "rocket-launch", runMultiplier: 0.75, durationMultiplier: 0.65, varianceMultiplier: 1.05, targetAverageViews: 230 };
  }
  if (preset === "trending-push") {
    return { patternType: "viral-spike", runMultiplier: 0.9, durationMultiplier: 0.95, varianceMultiplier: 1.15, targetAverageViews: 195 };
  }
  if (preset === "slow-burn") {
    return { patternType: "smooth-s-curve", runMultiplier: 1.2, durationMultiplier: 1.35, varianceMultiplier: 0.65, targetAverageViews: 150 };
  }
  return { runMultiplier: 1, durationMultiplier: 1, varianceMultiplier: 1, targetAverageViews: 180 };
}

function withBandNoise(band: [number, number], amount = 0.08): [number, number] {
  const min = Math.max(0.45, band[0] + random(-amount, amount));
  const max = Math.max(min + 0.02, band[1] + random(-amount, amount));
  return [min, max];
}

function createPatternVariant(profile: OrganicPatternProfile): OrganicPatternVariant {
  return {
    earlyBand: withBandNoise(profile.earlyBand),
    midBand: withBandNoise(profile.midBand),
    lateBand: withBandNoise(profile.lateBand),
    midSpikeChance: clamp(profile.midSpikeChance + random(-0.04, 0.08), 0.02, 0.45),
    spikeBand: withBandNoise(profile.spikeBand, 0.12),
    dipChance: clamp(profile.dipChance + random(-0.04, 0.06), 0.01, 0.28),
    dipBand: withBandNoise(profile.dipBand, 0.1),
    waveAmplitude: clamp(profile.waveAmplitude + random(-0.02, 0.03), 0.01, 0.11),
    waveFrequency: random(1.4, 3.8),
    timingShift: random(-0.15, 0.15),
  };
}

function pickPatternProfile(presetType: PatternType | undefined): OrganicPatternProfile {
  const pool = presetType
    ? ORGANIC_PATTERN_LIBRARY.filter((profile) => profile.baseType === presetType)
    : ORGANIC_PATTERN_LIBRARY;

  const candidates = pool.length > 0 ? pool : ORGANIC_PATTERN_LIBRARY;
  let picked = candidates[randomInt(0, candidates.length - 1)];

  if (candidates.length > 1 && lastPatternKey === picked.key) {
    const alternatives = candidates.filter((profile) => profile.key !== lastPatternKey);
    picked = alternatives[randomInt(0, alternatives.length - 1)];
  }

  lastPatternKey = picked.key;
  return picked;
}

function resolveDurationHours(config: OrderConfig): number {
  if (config.delivery.mode === "custom" || config.delivery.mode === "preset") return config.delivery.hours;
  const automatic = 7 + Math.sqrt(Math.max(800, config.totalViews)) / 16;
  return clamp(automatic, 6, 48);
}

function pickWeightedIndex(weights: number[]): number {
  const sum = weights.reduce((acc, value) => acc + value, 0);
  if (sum <= 0) return randomInt(0, Math.max(0, weights.length - 1));
  const threshold = random(0, sum);
  let cursor = 0;
  for (let index = 0; index < weights.length; index += 1) {
    cursor += weights[index];
    if (threshold <= cursor) return index;
  }
  return Math.max(0, weights.length - 1);
}

// 🔥 UPDATED: Now accepts minViewsPerRun as parameter
function resolveRunCount(totalViews: number, desiredRuns: number, averageTarget: number, minViewsPerRun: number): number {
  // Maximum runs possible based on minimum views per run
  const maxRunsByMinimum = Math.max(1, Math.floor(totalViews / minViewsPerRun));
  let runCount = clamp(desiredRuns, 1, maxRunsByMinimum);

  // Ensure average views per run is reasonable
  while (runCount > 1 && totalViews / runCount < minViewsPerRun * 1.3) {
    runCount -= 1;
  }

  const averageBound = Math.max(1, Math.floor(totalViews / Math.max(minViewsPerRun, averageTarget)));
  runCount = Math.min(runCount, Math.max(1, averageBound));
  
  return Math.max(1, runCount);
}

interface CurveContext {
  spikes: Array<{ center: number; width: number; height: number }>;
  burstAnchors: number[];
  phase: number;
  stepCount: number;
  wobble: number;
}

function createCurveContext(type: PatternType): CurveContext {
  const spikeCount = type === "viral-spike" ? randomInt(2, 4) : 0;
  const spikes = Array.from({ length: spikeCount }, () => ({
    center: random(0.25, 0.85),
    width: random(0.03, 0.09),
    height: random(0.08, 0.2),
  }));

  return {
    spikes,
    burstAnchors: [random(0.15, 0.25), random(0.4, 0.55), random(0.7, 0.88)],
    phase: random(0, Math.PI * 2),
    stepCount: randomInt(8, 14),
    wobble: random(0.006, 0.018),
    macroType: randomInt(1, 4),
  };
}

function curveValue(type: PatternType, t: number, context: CurveContext): number {
  const macro = (context as any).macroType || 1;

  let value = 0;

  if (type === "smooth-s-curve") {
    value = 1 / (1 + Math.exp(-10 * (t - 0.5)));
  } 
  else if (type === "rocket-launch") {
    const k = 5.2;
    value = (1 - Math.exp(-k * t)) / (1 - Math.exp(-k));
  } 
  else if (type === "sunset-fade") {
    const k = 4.1;
    value = (Math.exp(k * t) - 1) / (Math.exp(k) - 1);
  } 
  else if (type === "viral-spike") {
    const base = 1 / (1 + Math.exp(-8 * (t - 0.48)));
    const spikeLift = context.spikes.reduce(
      (acc, spike) =>
        acc + Math.exp(-Math.pow((t - spike.center) / spike.width, 2)) * spike.height,
      0
    );
    value = base + spikeLift;
  } 
  else if (type === "heartbeat") {
    const base = Math.pow(t, 1.08);
    const pulse = Math.sin((t * 9.5 + 0.15) * Math.PI + context.phase) * 0.055 * (1 - t * 0.3);
    const microPulse = Math.sin((t * 19 + 0.2) * Math.PI + context.phase * 0.5) * 0.02;
    value = base + pulse + microPulse;
  } 
  else if (type === "sawtooth") {
    const step = Math.floor(t * context.stepCount) / context.stepCount;
    const remainder = (t * context.stepCount) % 1;
    value = step * 0.86 + remainder * 0.14;
  } 
  else if (type === "micro-burst") {
    const [a, b, c] = context.burstAnchors;
    const jump1 = t >= a ? 0.12 : 0;
    const jump2 = t >= b ? 0.16 : 0;
    const jump3 = t >= c ? 0.2 : 0;
    const drift = t * 0.58;
    const micro = Math.sin(t * 18 * Math.PI + context.phase) * 0.015;
    value = drift + jump1 + jump2 + jump3 + micro;
  } 
  else {
    const phi = 1.618;
    value = Math.pow(t, phi) + Math.pow(t, 2.6) * 0.18;
  }

  // 🔥 MACRO VARIATION (THIS IS THE REAL MAGIC)
  if (macro === 1) {
    // slow → spike → plateau
    value += Math.exp(-Math.pow((t - 0.6) / 0.15, 2)) * 0.25;
  }

  if (macro === 2) {
    // early burst → decay
    value += Math.exp(-t * 4) * 0.2;
  }

  if (macro === 3) {
    // flat → sudden jump
    if (t > 0.5) value += Math.pow((t - 0.5) * 2, 2) * 0.4;
  }

  if (macro === 4) {
    // wave pattern
    value += Math.sin(t * Math.PI * 3) * 0.08;
  }

  return value;
}

function normalizeMonotone(values: number[]): number[] {
  const series = [...values];
  for (let index = 1; index < series.length; index += 1) {
    series[index] = Math.max(series[index], series[index - 1] + 0.0001);
  }

  const first = series[0];
  const last = series[series.length - 1];
  const span = Math.max(0.0001, last - first);
  return series.map((value) => (value - first) / span);
}

function allocateRounded(values: number[], total: number): number[] {
  if (values.length === 0) return [];
  const floors = values.map((value) => Math.floor(value));
  let remainder = total - floors.reduce((acc, value) => acc + value, 0);
  const order = values
    .map((value, index) => ({ index, frac: value - Math.floor(value) }))
    .sort((a, b) => b.frac - a.frac);

  let cursor = 0;
  while (remainder > 0 && order.length > 0) {
    floors[order[cursor % order.length].index] += 1;
    remainder -= 1;
    cursor += 1;
  }
  return floors;
}

// 🔥 UPDATED: Now accepts minViewsPerRun as parameter
function redistributeForMinimum(runs: number[], minimum: number): number[] {
  const result = [...runs];

  for (let index = 0; index < result.length; index += 1) {
    if (result[index] >= minimum) continue;

    let deficit = minimum - result[index];
    while (deficit > 0) {
      let donor = -1;
      let donorExcess = 0;
      for (let candidate = 0; candidate < result.length; candidate += 1) {
        if (candidate === index) continue;
        const excess = result[candidate] - minimum;
        if (excess > donorExcess) {
          donorExcess = excess;
          donor = candidate;
        }
      }
      if (donor < 0 || donorExcess <= 0) break;

      const transfer = Math.min(deficit, donorExcess);
      result[index] += transfer;
      result[donor] -= transfer;
      deficit -= transfer;
    }
  }

  // Merge runs that are still below minimum
  for (let index = 0; index < result.length; index += 1) {
    if (result[index] >= minimum || result.length === 1) continue;

    if (index === result.length - 1) {
      result[index - 1] += result[index];
      result.splice(index, 1);
    } else {
      result[index + 1] += result[index];
      result.splice(index, 1);
      index -= 1;
    }
  }

  return result;
}

// 🔥 UPDATED: Now accepts minViewsPerRun as parameter
function distributeWithMinimum(weights: number[], total: number, minimum: number): number[] {
  if (total === 0) return [0];
  if (total < minimum) return [total];

  const count = clamp(weights.length, 1, Math.floor(total / minimum));
  const localWeights = weights.slice(0, count).map((weight) => Math.max(0.01, weight));
  const weightSum = localWeights.reduce((acc, value) => acc + value, 0);
  const baseline = count * minimum;
  const remainder = total - baseline;
  const rawExtras = localWeights.map((weight) => (weight / weightSum) * remainder);
  const extras = allocateRounded(rawExtras, remainder);
  return extras.map((extra) => extra + minimum);
}

// 🔥 UPDATED: Now accepts minViewsPerRun as parameter
function nudgeConsecutiveDuplicates(values: number[], minimum: number): number[] {
  if (values.length < 2) return values;
  const result = [...values];

  for (let index = 1; index < result.length; index += 1) {
    if (result[index] !== result[index - 1]) continue;

    const canRaiseCurrent = index < result.length - 1 || result[index - 1] > minimum;
    if (canRaiseCurrent) {
      result[index] += 1;
      let donated = false;
      if (index < result.length - 1 && result[index + 1] > minimum) {
        result[index + 1] -= 1;
        donated = true;
      } else {
        for (let donor = result.length - 1; donor >= 0; donor -= 1) {
          if (donor !== index && result[donor] > minimum) {
            result[donor] -= 1;
            donated = true;
            break;
          }
        }
      }
      if (!donated) result[index] -= 1;
    }
  }

  return result;
}

// 🔥 UPDATED: Now accepts minViewsPerRun as parameter
function generateViewRunsFromCurve(
  patternType: PatternType,
  totalViews: number,
  runCount: number,
  variancePercent: number,
  preset: QuickPatternPreset | null,
  variant: OrganicPatternVariant,
  minViewsPerRun: number
): number[] {
  if (totalViews <= 0) return [0];
  if (totalViews < minViewsPerRun) return [totalViews];

  const context = createCurveContext(patternType);
  const varianceFactor = clamp(variancePercent, 10, 50) / 100;
  const presetVarianceBoost = preset === "viral-boost" ? 1.2 : preset === "slow-burn" ? 0.8 : 1;
  const noiseAmplitude = clamp(0.01 + varianceFactor * 0.02 * presetVarianceBoost, 0.01, 0.03);

  const cumulativeRaw = Array.from({ length: runCount + 1 }, (_, index) => {
    const t = index / runCount;
    const base = curveValue(patternType, t, context);
    const wiggle = 1 + random(-noiseAmplitude, noiseAmplitude) + Math.sin((index + 1) * 0.8 + context.phase) * context.wobble;
    return base * wiggle;
  });

  const cumulative = normalizeMonotone(cumulativeRaw);
  const rampRuns = Math.max(3, Math.min(5, Math.floor(runCount * 0.2)));
  const incrementsRaw = Array.from({ length: runCount }, (_, index) => {
    const phase = index / Math.max(1, runCount - 1);
    const delta = Math.max(0.00001, cumulative[index + 1] - cumulative[index]);
    const shapeVariance = random(1 - varianceFactor * 0.55, 1 + varianceFactor * 0.7);
    const wave = 1 + Math.sin((phase + variant.timingShift) * Math.PI * variant.waveFrequency) * variant.waveAmplitude;
    let phaseFactor = 1;

    if (phase < 0.2) {
      phaseFactor = random(variant.earlyBand[0], variant.earlyBand[1]);
    } else if (phase <= 0.8) {
      phaseFactor = random(variant.midBand[0], variant.midBand[1]);
      const spikeChance = phase > 0.32 && phase < 0.72 ? variant.midSpikeChance + varianceFactor * 0.08 : variant.midSpikeChance * 0.4;
      if (Math.random() < spikeChance) {
        phaseFactor *= random(variant.spikeBand[0], variant.spikeBand[1]);
      }
    } else {
      phaseFactor = random(variant.lateBand[0], variant.lateBand[1]);
    }

    if (Math.random() < variant.dipChance) {
      phaseFactor *= random(variant.dipBand[0], variant.dipBand[1]);
    }

    if (index < rampRuns) {
      const ease = (index + 1) / rampRuns;
      const easeIn = Math.pow(ease, 1.8);
      phaseFactor *= 0.52 + easeIn * 0.44;
    }

    if (index >= runCount - rampRuns) {
      phaseFactor *= random(0.82, 0.98);
    }

    return delta * shapeVariance * phaseFactor * wave;
  });

  const incrementSum = incrementsRaw.reduce((acc, value) => acc + value, 0);
  const scaled = incrementsRaw.map((value) => (value / Math.max(0.0001, incrementSum)) * totalViews);
  const rounded = allocateRounded(scaled, totalViews);
  const phasedWeights = rounded.map((value, index) => {
    const phase = index / Math.max(1, rounded.length - 1);
    if (phase < 0.2) return value * random(0.78, 0.9);
    if (phase <= 0.8) {
      const boosted = value * random(1.06, 1.24);
      return Math.random() < 0.14 ? boosted * random(1.12, 1.42) : boosted;
    }
    return value * random(0.86, 1.02);
  });
  const phasedRuns = distributeWithMinimum(phasedWeights, totalViews, minViewsPerRun);
  const minimumSafe = redistributeForMinimum(phasedRuns, minViewsPerRun);
  const finalRuns = nudgeConsecutiveDuplicates(minimumSafe, minViewsPerRun);

  if (finalRuns.length > 1 && finalRuns.every((value) => value === finalRuns[0])) {
    finalRuns[0] += 1;
    let adjusted = false;
    for (let donor = finalRuns.length - 1; donor >= 1; donor -= 1) {
      if (finalRuns[donor] > minViewsPerRun) {
        finalRuns[donor] -= 1;
        adjusted = true;
        break;
      }
    }
    if (!adjusted) finalRuns[0] -= 1;
  }

  return finalRuns;
}

function intervalPatternFactor(type: PatternType, t: number): number {
  if (type === "smooth-s-curve") return 1.06 - Math.exp(-Math.pow((t - 0.5) / 0.2, 2)) * 0.34;
  if (type === "rocket-launch") return 0.58 + t * 1.02;
  if (type === "sunset-fade") return 1.2 - t * 0.52;
  if (type === "viral-spike") return 1.14 - Math.exp(-Math.pow((t - 0.56) / 0.14, 2)) * 0.5;
  if (type === "micro-burst") return Math.sin(t * 22) > 0.25 ? 0.64 : 1.52;
  if (type === "heartbeat") return Math.sin(t * 16) > 0.2 ? 0.76 : 1.26;
  if (type === "sawtooth") return ((t * 10) % 1) < 0.2 ? 0.66 : 1.24;
  return 1.16 - t * 0.46;
}

function intervalPresetFactor(preset: QuickPatternPreset | null, t: number): number {
  if (preset === "viral-boost") return 1.2 - Math.exp(-Math.pow((t - 0.58) / 0.2, 2)) * 0.45;
  if (preset === "fast-start") return 0.65 + t * 0.9;
  if (preset === "trending-push") return 1.1 - Math.exp(-Math.pow((t - 0.58) / 0.22, 2)) * 0.3;
  if (preset === "slow-burn") return 1.2 + t * 0.25;
  return 1;
}

interface EngagementProfile {
  densityMin: number;
  densityMax: number;
  perRunMin: number;
  perRunMax: number;
}

type EngagementKind = "likes" | "shares" | "saves";

function resolveEngagementProfile(kind: EngagementKind): EngagementProfile {
  if (kind === "likes") return { densityMin: 0.2, densityMax: 0.35, perRunMin: 10, perRunMax: 20 };
  return { densityMin: 0.1, densityMax: 0.2, perRunMin: 10, perRunMax: 18 };
}

function buildEngagementWeights(runs: { views: number; at: Date }[], peakHoursBoost: boolean): number[] {
  const maxViews = Math.max(1, ...runs.map((run) => run.views));
  return runs.map((run, index) => {
    const previous = index > 0 ? runs[index - 1].views : run.views;
    const next = index < runs.length - 1 ? runs[index + 1].views : run.views;
    const increase = Math.max(0, run.views - previous) / maxViews;
    const localSpike = Math.max(0, run.views - (previous + next) / 2) / maxViews;
    const t = index / Math.max(1, runs.length - 1);
    const phaseBoost = t > 0.3 && t < 0.75 ? 1.15 : 1;
    const hour = run.at.getHours();
    const peakBoost = peakHoursBoost && hour >= 18 && hour <= 23 ? 1.35 : 1;
    return Math.max(0.01, (0.5 + run.views / maxViews * 0.65 + increase * 1.1 + localSpike * 1.2) * phaseBoost * peakBoost);
  });
}

function applyNoise(value: number, min: number, max: number): number {
  return clamp(value + randomInt(-2, 2), min, max);
}

function selectEngagementRuns(length: number, count: number, weights: number[]): number[] {
  if (count <= 0 || length === 0) return [];

  const selected = new Set<number>();
  const minGap = Math.max(1, Math.floor(length / Math.max(6, count * 2.5)));
  const anchors = [0.15, 0.5, 0.82];

  for (const anchor of anchors) {
    if (selected.size >= count) break;
    const center = Math.round((length - 1) * anchor);
    const start = Math.max(0, center - Math.max(2, Math.floor(length * 0.08)));
    const end = Math.min(length - 1, center + Math.max(2, Math.floor(length * 0.08)));
    const candidates = Array.from({ length: end - start + 1 }, (_, offset) => start + offset).filter((index) => {
      for (const taken of selected) {
        if (Math.abs(taken - index) <= minGap) return false;
      }
      return true;
    });
    if (candidates.length === 0) continue;
    const candidateWeights = candidates.map((index) => Math.max(0.01, weights[index] * random(0.9, 1.12)));
    selected.add(candidates[pickWeightedIndex(candidateWeights)]);
  }

  while (selected.size < count) {
    const candidates = Array.from({ length }, (_, index) => index).filter((index) => {
      for (const taken of selected) {
        if (Math.abs(taken - index) <= minGap && Math.random() < 0.8) return false;
      }
      return true;
    });
    if (candidates.length === 0) break;
    const candidateWeights = candidates.map((index) => Math.max(0.01, weights[index] * random(0.9, 1.15)));
    selected.add(candidates[pickWeightedIndex(candidateWeights)]);
  }

  const result = Array.from(selected).sort((a, b) => a - b);
  const maxGap = Math.max(5, Math.ceil(length / Math.max(2, count)) + 1);
  let cursor = 0;
  while (cursor < result.length - 1 && result.length < count) {
    const gap = result[cursor + 1] - result[cursor];
    if (gap > maxGap) {
      const mid = Math.floor((result[cursor] + result[cursor + 1]) / 2);
      result.splice(cursor + 1, 0, mid);
    }
    cursor += 1;
  }

  return result.slice(0, count);
}

function phaseRangeForKind(kind: EngagementKind, t: number): { min: number; max: number } {
  if (kind === "likes") {
    if (t < 0.33) return { min: 10, max: 14 };
    if (t < 0.72) return { min: 14, max: 20 };
    return { min: 12, max: 18 };
  }
  if (t < 0.33) return { min: 10, max: 13 };
  if (t < 0.72) return { min: 12, max: 18 };
  return { min: 11, max: 16 };
}

function pickEngagementValue(kind: EngagementKind, t: number, lastValue: number | null): number {
  const profile = resolveEngagementProfile(kind);
  const range = phaseRangeForKind(kind, t);
  const min = clamp(range.min, profile.perRunMin, profile.perRunMax);
  const max = clamp(range.max, profile.perRunMin, profile.perRunMax);
  let value = applyNoise(randomInt(min, max), profile.perRunMin, profile.perRunMax);

  if (lastValue !== null && value === lastValue) {
    value = clamp(value + (Math.random() < 0.5 ? -1 : 1), profile.perRunMin, profile.perRunMax);
  }

  return value;
}

function distributeEngagement(
  runs: { views: number; at: Date }[],
  targetTotal: number,
  peakHoursBoost: boolean,
  kind: EngagementKind
): number[] {
  const result = Array.from({ length: runs.length }, () => 0);
  if (targetTotal < 10 || runs.length === 0) return result;

  const profile = resolveEngagementProfile(kind);
  const minCount = Math.max(1, Math.round(runs.length * profile.densityMin));
  const maxCount = Math.max(minCount, Math.round(runs.length * profile.densityMax));
  const preferredCount = clamp(randomInt(minCount, maxCount), 1, runs.length);
  const requiredCount = Math.ceil(targetTotal / Math.max(profile.perRunMin + 3, profile.perRunMax - 1));
  const selectedCount = clamp(Math.max(preferredCount, Math.min(maxCount, requiredCount)), 1, runs.length);

  const weights = buildEngagementWeights(runs, peakHoursBoost);
  const selected = selectEngagementRuns(runs.length, selectedCount, weights);
  const effectiveCount = Math.max(1, selected.length);

  const feasibleMin = effectiveCount * profile.perRunMin;
  const feasibleMax = effectiveCount * profile.perRunMax;
  const naturalMid = Math.round(effectiveCount * ((profile.perRunMin + profile.perRunMax) / 2));
  const target =
    targetTotal > feasibleMax
      ? randomInt(Math.max(feasibleMin, naturalMid - effectiveCount), Math.max(feasibleMin, naturalMid + Math.floor(effectiveCount * 0.8)))
      : clamp(targetTotal, feasibleMin, feasibleMax);
  let runningTotal = 0;
  let lastAssigned: number | null = null;
  let secondLastAssigned: number | null = null;

  for (const index of selected) {
    const t = index / Math.max(1, runs.length - 1);
    let value = pickEngagementValue(kind, t, lastAssigned);
    if (secondLastAssigned !== null && value === secondLastAssigned) {
      value = clamp(value + (Math.random() < 0.5 ? -1 : 1), profile.perRunMin, profile.perRunMax);
    }

    const spikeBias = weights[index] / Math.max(0.01, Math.max(...weights));
    if (Math.random() < spikeBias * 0.45) value = Math.min(profile.perRunMax, value + randomInt(1, 2));

    result[index] = value;
    runningTotal += value;
    secondLastAssigned = lastAssigned;
    lastAssigned = value;
  }

  let delta = target - runningTotal;
  const adjustable = selected.map((index) => ({ index, weight: Math.max(0.01, weights[index]) }));
  while (delta !== 0 && adjustable.length > 0) {
    const chosen = adjustable[pickWeightedIndex(adjustable.map((slot) => slot.weight))].index;
    if (delta > 0 && result[chosen] < profile.perRunMax) {
      result[chosen] += 1;
      delta -= 1;
    } else if (delta < 0 && result[chosen] > profile.perRunMin) {
      result[chosen] -= 1;
      delta += 1;
    } else {
      const next = adjustable.find((slot) => (delta > 0 ? result[slot.index] < profile.perRunMax : result[slot.index] > profile.perRunMin));
      if (!next) break;
    }
  }

  return result;
}

function distributeLikesProportional(runs: { views: number }[], targetTotal: number): number[] {
  if (runs.length === 0) return [];

  const totalViews = Math.max(1, runs.reduce((sum, run) => sum + Math.max(0, run.views), 0));
  const minimumPerRun = 10;
  const likesTarget = Math.max(targetTotal, runs.length * minimumPerRun);

  const baseShares = runs.map((run) => (Math.max(0, run.views) / totalViews) * likesTarget);
  const withVariation = baseShares.map((base) => base * random(0.8, 1.2));

  const preliminary = withVariation.map((value) => Math.max(minimumPerRun, Math.round(value)));
  const baseFloor = runs.length * minimumPerRun;
  const currentExtra = preliminary.reduce((sum, value) => sum + (value - minimumPerRun), 0);
  const targetExtra = Math.max(0, likesTarget - baseFloor);

  const scaled =
    currentExtra > 0
      ? preliminary.map((value) => minimumPerRun + Math.max(0, Math.round((value - minimumPerRun) * (targetExtra / currentExtra))))
      : Array.from({ length: runs.length }, () => minimumPerRun);

  let drift = likesTarget - scaled.reduce((sum, value) => sum + value, 0);
  const weightedIndexes = runs
    .map((run, index) => ({ index, weight: Math.max(1, run.views) }))
    .sort((a, b) => b.weight - a.weight)
    .map((slot) => slot.index);

  if (drift > 0) {
    let pointer = 0;
    while (drift > 0) {
      const index = weightedIndexes[pointer % weightedIndexes.length];
      scaled[index] += 1;
      drift -= 1;
      pointer += 1;
    }
  } else if (drift < 0) {
    let pointer = 0;
    let guard = 0;
    while (drift < 0 && guard < scaled.length * 30) {
      const index = weightedIndexes[pointer % weightedIndexes.length];
      if (scaled[index] > minimumPerRun) {
        scaled[index] -= 1;
        drift += 1;
      }
      pointer += 1;
      guard += 1;
    }
  }

  for (let index = 1; index < scaled.length; index += 1) {
    if (scaled[index] === scaled[index - 1]) {
      const direction = Math.random() < 0.5 ? -1 : 1;
      const next = scaled[index] + direction;
      if (next >= minimumPerRun) {
        scaled[index] = next;
      } else {
        scaled[index] += 1;
      }
    }
  }

  let finalDelta = likesTarget - scaled.reduce((sum, value) => sum + value, 0);
  if (finalDelta !== 0) {
    const ordered = [...weightedIndexes];
    let pointer = 0;
    let guard = 0;
    while (finalDelta !== 0 && guard < ordered.length * 40) {
      const index = ordered[pointer % ordered.length];
      if (finalDelta > 0) {
        scaled[index] += 1;
        finalDelta -= 1;
      } else if (scaled[index] > minimumPerRun) {
        scaled[index] -= 1;
        finalDelta += 1;
      }
      pointer += 1;
      guard += 1;
    }
  }

  return scaled;
}

function distributeByViewsProportional(
  runs: { views: number }[],
  targetTotal: number,
  minPerRun = 1
): number[] {
  if (runs.length === 0) return [];

  const result = Array.from({ length: runs.length }, () => 0);

  const totalViews = Math.max(1, runs.reduce((sum, r) => sum + r.views, 0));
  const maxViews = Math.max(...runs.map(r => r.views));

  // 🔥 STEP 1: weight runs (favor mid + high)
  const weights = runs.map((r, i) => {
    const t = i / Math.max(1, runs.length - 1); // timeline position

    const viewWeight = r.views / maxViews; // high views = high weight

    const phaseWeight =
      t < 0.2 ? 0.3 :      // early low
      t < 0.7 ? 1.2 :      // mid HIGH
      0.8;                 // late medium

    return Math.max(0.01, viewWeight * phaseWeight);
  });

  // 🔥 STEP 2: pick runs based on weight (not random)
  const runCount = runs.length;
  const activeCount = Math.max(1, Math.floor(runCount * (0.25 + Math.random() * 0.25)));

  const selectedIndexes: number[] = [];

  const weightPool = weights.map((w, i) => ({ w, i }));

  while (selectedIndexes.length < activeCount && weightPool.length > 0) {
    const totalW = weightPool.reduce((s, x) => s + x.w, 0);
    let rand = Math.random() * totalW;

    for (let j = 0; j < weightPool.length; j++) {
      rand -= weightPool[j].w;
      if (rand <= 0) {
        selectedIndexes.push(weightPool[j].i);
        weightPool.splice(j, 1);
        break;
      }
    }
  }

  // 🔥 STEP 3: distribute among selected
  const selectedRuns = selectedIndexes.map(i => runs[i]);
  const selectedViews = selectedRuns.reduce((s, r) => s + r.views, 0);

  const raw = selectedRuns.map(r => {
    const base = (r.views / selectedViews) * targetTotal;
    const variation = base * (Math.random() * 0.4 - 0.2); // ±20%
    return base + variation;
  });

  let values = raw.map(v => Math.max(minPerRun, Math.round(v)));

  // fix total
  let diff = targetTotal - values.reduce((a, b) => a + b, 0);
  let i = 0;

  while (diff !== 0 && i < 10000) {
    const idx = i % values.length;

    if (diff > 0) {
      values[idx]++;
      diff--;
    } else if (values[idx] > minPerRun) {
      values[idx]--;
      diff++;
    }

    i++;
  }

  // 🔥 STEP 4: assign back
  selectedIndexes.forEach((runIndex, i) => {
    result[runIndex] = values[i];
  });

  return result;
}

function normalizeSharesRuns(values: number[], minimum: number): number[] {
  const result = Array.from({ length: values.length }, () => 0);
  if (values.length === 0) return result;

  let buffer = 0;
  let lastAssignedIndex = -1;

  for (let index = 0; index < values.length; index += 1) {
    if (values[index] <= 0) continue;
    buffer += values[index];

    if (buffer >= minimum) {
      result[index] = buffer;
      lastAssignedIndex = index;
      buffer = 0;
    }
  }

  if (buffer > 0) {
    if (lastAssignedIndex >= 0) {
      result[lastAssignedIndex] += buffer;
    } else {
      result[values.length - 1] = buffer;
    }
  }

  return result;
}

function clearFirstRun(values: number[]): number[] {
  const result = [...values];
  if (result.length === 0 || result[0] === 0) return result;
  if (result.length === 1) return [0];

  const carry = result[0];
  result[0] = 0;

  let target = 1;
  for (let index = 2; index < result.length; index += 1) {
    if (result[index] > result[target]) target = index;
  }
  result[target] += carry;
  return result;
}

function detectRisk(viewsPerHour: number, variancePercent: number, hours: number): "Safe" | "Medium" | "Risk" {
  const speedScore = clamp(viewsPerHour / 15000, 0, 1.2);
  const varianceScore = clamp(variancePercent / 50, 0, 1);
  const shortWindowPenalty = hours <= 12 ? 0.25 : hours <= 24 ? 0.12 : 0;
  const score = speedScore * 0.75 + varianceScore * 0.45 + shortWindowPenalty;
  if (score >= 1) return "Risk";
  if (score >= 0.62) return "Medium";
  return "Safe";
}

// 🔥 MAIN FUNCTION - Now uses config.minViewsPerRun
export function createPatternPlan(config: OrderConfig): PatternPlan {
  // 🔥 Get minViewsPerRun from config (default to 100 if not set)
  const minViewsPerRun = config.minViewsPerRun || 100;
  
  const presetProfile = resolvePresetProfile(config.quickPreset);
  const selectedPatternProfile = pickPatternProfile(presetProfile.patternType);
  const patternType = presetProfile.patternType ?? selectedPatternProfile.baseType ?? pickRandomPatternType();
  const patternName = selectedPatternProfile.name;
  const variant = createPatternVariant(selectedPatternProfile);
  const patternId = randomInt(100, 999);
  const requestedViews = Math.max(0, Math.floor(config.totalViews));
  const variance = clamp(config.variancePercent * presetProfile.varianceMultiplier, 10, 50);
  
  // 🔥 Calculate max possible runs based on minViewsPerRun
  const maxPossibleRuns = Math.max(1, Math.floor(requestedViews / minViewsPerRun));
  
  // 🔥 Adjusted run calculation - respect minimum views constraint
  const baseRequestedRuns = Math.round(randomInt(50, 80) * presetProfile.runMultiplier * selectedPatternProfile.runMultiplier);
  const requestedRuns = Math.min(baseRequestedRuns, maxPossibleRuns);
  
  const totalRuns = requestedViews >= minViewsPerRun 
    ? resolveRunCount(requestedViews, requestedRuns, presetProfile.targetAverageViews, minViewsPerRun) 
    : 1;
    
  const durationHours = clamp(
    resolveDurationHours(config) * presetProfile.durationMultiplier * selectedPatternProfile.durationMultiplier,
    2,
    72
  );
  const durationMin = durationHours * 60;
  const startDelayMin = clamp(config.startDelayHours || 0, 0, 168) * 60;

  // 🔥 Pass minViewsPerRun to view generation
  let viewRuns = generateViewRunsFromCurve(
    patternType, 
    requestedViews, 
    totalRuns, 
    variance, 
    config.quickPreset, 
    variant,
    minViewsPerRun
  );
  
  if (config.peakHoursBoost && viewRuns.length > 1 && requestedViews >= minViewsPerRun) {
    const initialWeights = viewRuns.map((views) => Math.max(0.01, views));
    const boostedWeights = initialWeights.map((weight, index) => {
      const t = index / Math.max(1, initialWeights.length - 1);
      const pseudoHour = Math.floor((t * durationHours) % 24);
      const inPeakWindow = pseudoHour >= 18 && pseudoHour <= 23;
      const boostChance = inPeakWindow ? 0.78 : 0.18;
      const boost = Math.random() < boostChance ? random(1.14, inPeakWindow ? 1.52 : 1.2) : random(0.94, 1.06);
      return weight * boost;
    });
    viewRuns = distributeWithMinimum(boostedWeights, requestedViews, minViewsPerRun);
  }

  const baseInterval = durationMin / Math.max(1, viewRuns.length - 1);
  const now = new Date();
  let elapsed = startDelayMin;
  const provisionalRuns = viewRuns.map((views, index) => {
    if (index > 0) {
      const t = index / Math.max(1, viewRuns.length - 1);
      const jitter = random(0.78, 1.24);
      elapsed += Math.max(
        1,
        baseInterval * jitter * intervalPresetFactor(config.quickPreset, t) * intervalPatternFactor(patternType, t)
      );
    }
    return { at: new Date(now.getTime() + elapsed * 60_000), views };
  });

  const totalViews = provisionalRuns.reduce((acc, run) => acc + run.views, 0);
  const likesRatio = random(0.05, 0.07);
  const sharesRatio = random(0.01, 0.02);
  const savesRatio = random(0.005, 0.01);
  const commentsRatio = random(0.0002, 0.0003); // 0.02%–0.03%

  const likesTotal = config.includeLikes ? Math.max(10, Math.floor(totalViews * likesRatio)) : 0;
  const sharesTotal = config.includeShares ? Math.max(20, Math.floor(totalViews * sharesRatio)) : 0;
  const savesTotal = config.includeSaves ? Math.max(10, Math.floor(totalViews * savesRatio)) : 0;
  let commentsTotal = 0;

if (config.includeComments) {
  if (totalViews >= 50000) {
    commentsTotal = randomInt(30, 40);
  } else if (totalViews >= 40000) {
    commentsTotal = randomInt(25, 40);
  } else if (totalViews >= 30000) {
    commentsTotal = randomInt(20, 35);
  } else if (totalViews >= 20000) {
    commentsTotal = randomInt(15, 30);
  } else if (totalViews >= 10000) {
    commentsTotal = randomInt(5, 15);
  } else if (totalViews >= 5000) {
    commentsTotal = randomInt(5, 7);
  } else {
    commentsTotal = 5;
  }
}

  const likesBase = config.includeLikes ? distributeLikesProportional(provisionalRuns, likesTotal) : viewRuns.map(() => 0);
  const sharesBase = config.includeShares
  ? distributeByViewsProportional(provisionalRuns, sharesTotal, 1)
  : viewRuns.map(() => 0);

  const savesBase = config.includeSaves
  ? distributeByViewsProportional(provisionalRuns, savesTotal, 10)
  : viewRuns.map(() => 0);
  const commentsBase = config.includeComments
  ? distributeByViewsProportional(provisionalRuns, commentsTotal, 1)
  : viewRuns.map(() => 0);

  const likesRuns = likesBase;
  const sharesRuns = normalizeSharesRuns(sharesBase, 20);
  const savesRuns = clearFirstRun(
  savesBase.map(v => {
    if (v <= 0) return 0;

    // 🔥 add variation AFTER min constraint
    const variation = Math.floor(v * (Math.random() * 0.4)); // up to +40%

    return v + variation;
  })
);
  const commentsRuns = (() => {
  const result = Array.from({ length: commentsBase.length }, () => 0);

  if (commentsTotal === 0) return result;

  // 🔥 decide how many runs will have comments
  const maxRuns = Math.min(commentsBase.length, Math.ceil(commentsTotal / 5));
  const activeRuns = randomInt(1, maxRuns);

  const indexes = Array.from({ length: commentsBase.length }, (_, i) => i)
    .sort(() => Math.random() - 0.5)
    .slice(0, activeRuns);

  let remaining = commentsTotal;

  for (let i = 0; i < indexes.length; i++) {
    const isLast = i === indexes.length - 1;

    let value;

    if (isLast) {
      value = remaining;
    } else {
      // ensure future runs can still have at least 5
      const maxAllowed = remaining - (indexes.length - i - 1) * 5;
      value = Math.min(maxAllowed, randomInt(5, 10));
    }

    result[indexes[i]] = value;
    remaining -= value;
  }

  return result;
})();

  let cumulativeViews = 0;
  let cumulativeLikes = 0;
  let cumulativeShares = 0;
  let cumulativeSaves = 0;
  let cumulativeComments = 0;

  const runs: RunStep[] = provisionalRuns.map((run, index) => {
    cumulativeViews += run.views;
    cumulativeLikes += likesRuns[index];
    cumulativeShares += sharesRuns[index];
    cumulativeSaves += savesRuns[index];
    cumulativeComments += commentsRuns[index];

    return {
      run: index + 1,
      at: run.at,
      minutesFromStart: Math.round((run.at.getTime() - now.getTime()) / 60_000),
      views: run.views,
      likes: likesRuns[index],
      shares: sharesRuns[index],
      saves: savesRuns[index],
      comments: commentsRuns[index],
      cumulativeViews,
      cumulativeLikes,
      cumulativeShares,
      cumulativeSaves,
      cumulativeComments,
    };
  });

  const viewsPerHour = totalViews / Math.max(1, durationHours);

  return {
    patternId,
    patternName,
    patternType,
    totalRuns: runs.length,
    approximateIntervalMin: Math.round(durationMin / Math.max(1, runs.length)),
    finishTime: runs[runs.length - 1]?.at ?? now,
    estimatedDurationHours: Number((durationHours + startDelayMin / 60).toFixed(1)),
    risk: detectRisk(viewsPerHour, variance, durationHours),
    runs,
  };
}

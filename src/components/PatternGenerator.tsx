import { AnimatePresence, motion } from "framer-motion";
import type { PatternPlan } from "../types/order";
import { RunTable } from "./RunTable";

interface PatternGeneratorProps {
  plan: PatternPlan;
  expandedRuns: boolean;
  onToggleRuns: () => void;
}

export function PatternGenerator({ plan, expandedRuns, onToggleRuns }: PatternGeneratorProps) {
  const safeRuns = plan?.runs || [];
  const safeFinishTime = plan?.finishTime instanceof Date ? plan.finishTime : new Date();

  return (
    <section className="space-y-3 sm:space-y-6">
      <div className="rounded-2xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-3 sm:p-5">
        <h2 className="mb-3 text-sm sm:text-lg font-semibold text-yellow-400">
          📅 Schedule Preview
        </h2>

        {/* Stats Grid - 3 col on all sizes but compact on mobile */}
        <div className="grid grid-cols-3 gap-2 sm:gap-3">
          <div className="rounded-xl border border-yellow-500/20 bg-black p-2 sm:p-3">
            <p className="text-[9px] sm:text-xs uppercase tracking-wide text-gray-600">
              Total Runs
            </p>
            <p className="mt-1 text-sm sm:text-base font-semibold text-gray-200">
              {plan?.totalRuns ?? 0}
            </p>
          </div>
          <div className="rounded-xl border border-yellow-500/20 bg-black p-2 sm:p-3">
            <p className="text-[9px] sm:text-xs uppercase tracking-wide text-gray-600">
              Interval
            </p>
            <p className="mt-1 text-sm sm:text-base font-semibold text-gray-200">
              {plan?.approximateIntervalMin ?? 0}
              <span className="text-xs text-gray-500 ml-0.5">min</span>
            </p>
          </div>
          <div className="rounded-xl border border-yellow-500/20 bg-black p-2 sm:p-3">
            <p className="text-[9px] sm:text-xs uppercase tracking-wide text-gray-600">
              Finish
            </p>
            <p className="mt-1 text-[10px] sm:text-sm font-semibold text-gray-200 leading-tight">
              {safeFinishTime.toLocaleDateString()}
              <span className="block text-[9px] sm:text-xs text-gray-500">
                {safeFinishTime.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
              </span>
            </p>
          </div>
        </div>

        {/* Toggle Runs Button */}
        <button
          type="button"
          onClick={onToggleRuns}
          className="mt-3 sm:mt-4 flex items-center gap-1.5 rounded-lg border border-yellow-500/30 bg-yellow-500/10 px-3 py-1.5 text-xs text-yellow-400 transition hover:bg-yellow-500/20 hover:text-yellow-300"
        >
          {expandedRuns ? "🔼 Hide Runs" : `📋 View Runs (${safeRuns.length})`}
        </button>

        {/* Expanded Runs Table */}
        <AnimatePresence initial={false}>
          {expandedRuns && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: "auto" }}
              exit={{ opacity: 0, height: 0 }}
              className="mt-3 overflow-hidden"
            >
              <RunTable runs={safeRuns} mode="schedule" />
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </section>
  );
}

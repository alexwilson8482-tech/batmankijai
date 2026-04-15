import { AnimatePresence, motion } from "framer-motion";
import type { PatternPlan } from "../types/order";
import { RunTable } from "./RunTable";

interface PatternGeneratorProps {
  plan: PatternPlan;
  expandedRuns: boolean;
  onToggleRuns: () => void;
}

export function PatternGenerator({
  plan,
  expandedRuns,
  onToggleRuns,
}: PatternGeneratorProps) {
  const safeRuns = plan?.runs || [];
  const safeFinishTime = plan?.finishTime instanceof Date ? plan.finishTime : new Date();

  return (
    <section className="space-y-6">
      {/* Schedule Preview Only */}
      <div className="rounded-2xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-5">
        <h2 className="mb-4 text-lg font-semibold text-yellow-400">📅 Schedule Preview</h2>
        <div className="grid gap-3 md:grid-cols-3">
          <div className="rounded-xl border border-yellow-500/20 bg-black p-3">
            <p className="text-xs uppercase tracking-wide text-gray-600">Total Runs</p>
            <p className="mt-1 text-base font-semibold text-gray-200">{plan?.totalRuns ?? 0}</p>
          </div>
          <div className="rounded-xl border border-yellow-500/20 bg-black p-3">
            <p className="text-xs uppercase tracking-wide text-gray-600">Interval (approx)</p>
            <p className="mt-1 text-base font-semibold text-gray-200">{plan?.approximateIntervalMin ?? 0} min</p>
          </div>
          <div className="rounded-xl border border-yellow-500/20 bg-black p-3">
            <p className="text-xs uppercase tracking-wide text-gray-600">Finish Time</p>
            <p className="mt-1 text-base font-semibold text-gray-200">{safeFinishTime.toLocaleString()}</p>
          </div>
        </div>
        <button
          type="button"
          onClick={onToggleRuns}
          className="mt-4 text-sm text-yellow-400 transition hover:text-yellow-300"
        >
          {expandedRuns ? "Hide Runs" : "View Runs"}
        </button>
        <AnimatePresence initial={false}>
          {expandedRuns && (
            <motion.div
              initial={{ opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: "auto" }}
              exit={{ opacity: 0, height: 0 }}
              className="mt-3"
            >
              <RunTable runs={safeRuns} mode="schedule" />
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </section>
  );
}

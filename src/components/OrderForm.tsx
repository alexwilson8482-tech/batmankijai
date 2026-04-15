import type { DeliveryOption } from "../types/order";

interface OrderFormProps {
  orderName: string;
  postUrl: string;
  bulkLinks: string;
  totalViews: number;
  selectedApiId: string;
  selectedBundleId: string;
  apiOptions: Array<{ id: string; name: string }>;
  bundleOptions: Array<{ id: string; name: string }>;
  startDelayHours: number;
  variancePercent: number;
  includeLikes: boolean;
  includeShares: boolean;
  includeSaves: boolean;
  includeComments: boolean;
  peakHoursBoost: boolean;
  delivery: DeliveryOption;
  customHours: number;
  onPostUrlChange: (value: string) => void;
  onBulkLinksChange: (value: string) => void;
  onOrderNameChange: (value: string) => void;
  onTotalViewsChange: (value: number) => void;
  onSelectedApiChange: (value: string) => void;
  onSelectedBundleChange: (value: string) => void;
  onStartDelayHoursChange: (value: number) => void;
  onVarianceChange: (value: number) => void;
  onToggleLikes: (value: boolean) => void;
  onToggleShares: (value: boolean) => void;
  onToggleSaves: (value: boolean) => void;
  onToggleComments: (value: boolean) => void;
  onPeakHoursChange: (value: boolean) => void;
  onDeliveryChange: (option: DeliveryOption) => void;
  onCustomHoursChange: (hours: number) => void;
}

function Toggle({ checked, label, onChange }: { checked: boolean; label: string; onChange: (value: boolean) => void }) {
  return (
    <label className="flex items-center justify-between rounded-xl border border-yellow-500/20 bg-black px-4 py-3">
      <span className="text-sm text-gray-400">{label}</span>
      <button
        type="button"
        role="switch"
        aria-checked={checked}
        onClick={() => onChange(!checked)}
        className={`relative h-6 w-11 rounded-full transition-colors ${checked ? "bg-yellow-500" : "bg-gray-700"}`}
      >
        <span
          className={`absolute top-0.5 h-5 w-5 rounded-full bg-white transition-transform ${checked ? "translate-x-5" : "translate-x-0.5"}`}
        />
      </button>
    </label>
  );
}

export function OrderForm(props: OrderFormProps) {
  const deliveryModes: DeliveryOption[] = [
    { mode: "auto", label: "Auto", hours: props.delivery.mode === "auto" ? props.delivery.hours : 18 },
    { mode: "preset", label: "6h", hours: 6 },
    { mode: "preset", label: "12h", hours: 12 },
    { mode: "preset", label: "24h", hours: 24 },
    { mode: "preset", label: "48h", hours: 48 },
    { mode: "custom", label: "Custom", hours: props.customHours },
  ];

  return (
    <section className="space-y-6">
      <div className="rounded-2xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-5">
        <h2 className="mb-4 text-lg font-semibold text-yellow-400">1. Target Input</h2>
        <div className="space-y-4">
          <div className="space-y-2">
            <label className="text-sm text-gray-500">Mission Name (optional)</label>
            <input
              value={props.orderName}
              onChange={(event) => props.onOrderNameChange(event.target.value)}
              placeholder="Campaign name"
              className="w-full rounded-xl border border-yellow-500/30 bg-black px-3 py-2.5 text-sm text-gray-100 outline-none ring-yellow-500/40 transition placeholder-gray-700 focus:ring-2"
            />
          </div>
          <div className="space-y-2">
            <label className="text-sm text-gray-500">Target URL</label>
            <input
              value={props.postUrl}
              onChange={(event) => props.onPostUrlChange(event.target.value)}
              placeholder="https://instagram.com/reel/..."
              className="w-full rounded-xl border border-yellow-500/30 bg-black px-3 py-2.5 text-sm text-gray-100 outline-none ring-yellow-500/40 transition placeholder-gray-700 focus:ring-2"
            />
          </div>
          <div className="space-y-2">
            <label className="text-sm text-gray-500">Bulk Targets (one per line)</label>
            <textarea
              value={props.bulkLinks}
              onChange={(event) => props.onBulkLinksChange(event.target.value)}
              rows={4}
              placeholder={"https://instagram.com/reel/abc...\nhttps://instagram.com/reel/xyz..."}
              className="w-full resize-y rounded-xl border border-yellow-500/30 bg-black px-3 py-2.5 text-sm text-gray-100 outline-none ring-yellow-500/40 transition placeholder-gray-700 focus:ring-2"
            />
          </div>
          <div className="space-y-2">
            <label className="text-sm text-gray-500">Total Views</label>
            <input
              type="number"
              value={props.totalViews}
              onChange={(event) => {
                const next = Number(event.target.value);
                props.onTotalViewsChange(Number.isFinite(next) ? next : 0);
              }}
              className="w-full rounded-xl border border-yellow-500/30 bg-black px-3 py-2.5 text-sm text-gray-100 outline-none ring-yellow-500/40 transition focus:ring-2"
            />
            <p className="text-xs text-gray-600">Minimum 100 views per run applied automatically</p>
          </div>
          <div className="grid gap-3 md:grid-cols-2">
            <div className="space-y-2">
              <label className="text-sm text-gray-500">Select API</label>
              <select
                value={props.selectedApiId}
                onChange={(event) => props.onSelectedApiChange(event.target.value)}
                className="w-full rounded-xl border border-yellow-500/30 bg-black px-3 py-2.5 text-sm text-gray-100"
              >
                <option value="">No API selected</option>
                {props.apiOptions.map((api) => (
                  <option key={api.id} value={api.id}>
                    {api.name}
                  </option>
                ))}
              </select>
            </div>
            <div className="space-y-2">
              <label className="text-sm text-gray-500">Select Bundle</label>
              <select
                value={props.selectedBundleId}
                onChange={(event) => props.onSelectedBundleChange(event.target.value)}
                className="w-full rounded-xl border border-yellow-500/30 bg-black px-3 py-2.5 text-sm text-gray-100"
              >
                <option value="">Select bundle</option>
                {props.bundleOptions.map((bundle) => (
                  <option key={bundle.id} value={bundle.id}>
                    {bundle.name}
                  </option>
                ))}
              </select>
            </div>
          </div>
          <div className="space-y-2">
            <label className="text-sm text-gray-500">Start Delay (hours)</label>
            <input
              type="number"
              min={0}
              max={168}
              value={props.startDelayHours}
              onChange={(event) => {
                const next = Number(event.target.value);
                props.onStartDelayHoursChange(Number.isFinite(next) ? next : 0);
              }}
              className="w-full rounded-xl border border-yellow-500/30 bg-black px-3 py-2.5 text-sm text-gray-100 outline-none ring-yellow-500/40 transition focus:ring-2"
            />
            <p className="text-xs text-gray-600">Hours until mission deployment</p>
          </div>
          <div className="grid gap-3 md:grid-cols-4">
  <Toggle checked={props.includeLikes} label="Likes" onChange={props.onToggleLikes} />
  <Toggle checked={props.includeShares} label="Shares" onChange={props.onToggleShares} />
  <Toggle checked={props.includeSaves} label="Saves" onChange={props.onToggleSaves} />
  <Toggle checked={props.includeComments} label="Comments" onChange={props.onToggleComments} />
</div>
        </div>
      </div>

      <div className="rounded-2xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-5">
        <h2 className="mb-4 text-lg font-semibold text-yellow-400">2. Advanced Controls</h2>
        <div className="space-y-5">
          <div>
            <p className="mb-2 text-sm text-gray-500">Delivery Time</p>
            <div className="flex flex-wrap gap-2">
              {deliveryModes.map((option) => {
                const active = props.delivery.label === option.label;
                return (
                  <button
                    key={option.label}
                    type="button"
                    onClick={() => props.onDeliveryChange(option)}
                    className={`rounded-lg border px-3 py-1.5 text-sm transition ${
                      active
                        ? "border-yellow-500/70 bg-yellow-500/20 text-yellow-300"
                        : "border-gray-700 text-gray-500 hover:border-yellow-500/30"
                    }`}
                  >
                    {option.label}
                  </button>
                );
              })}
            </div>
            {props.delivery.mode === "custom" && (
              <input
                type="number"
                min={1}
                max={96}
                value={props.customHours}
                onChange={(event) => props.onCustomHoursChange(Number(event.target.value) || 1)}
                className="mt-3 w-36 rounded-lg border border-yellow-500/30 bg-black px-3 py-2 text-sm text-gray-100 outline-none ring-yellow-500/40 focus:ring-2"
              />
            )}
          </div>

          <div>
            <div className="mb-2 flex items-center justify-between text-sm">
              <span className="text-gray-500">Random Variance</span>
              <span className="font-medium text-yellow-300">{props.variancePercent}%</span>
            </div>
            <input
              type="range"
              min={0}
              max={50}
              value={props.variancePercent}
              onChange={(event) => props.onVarianceChange(Number(event.target.value))}
              className="h-2 w-full cursor-pointer appearance-none rounded-full bg-gray-700 accent-yellow-500"
            />
          </div>

          <Toggle
            checked={props.peakHoursBoost}
            onChange={props.onPeakHoursChange}
            label="Night boost (6 PM – 11 PM)"
          />
        </div>
      </div>
    </section>
  );
}

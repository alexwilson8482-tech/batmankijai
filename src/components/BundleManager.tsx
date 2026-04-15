import { useMemo, useState } from "react";
import type { ApiPanel, ApiService, Bundle } from "../types/order";

interface BundleManagerProps {
  apis: ApiPanel[];
  bundles: Bundle[];
  onAddBundle: (bundle: {
  name: string;
  apiId: string;
  views: string;
  likes: string;
  shares: string;
  saves: string;
  comments: string; // ✅ ADD THIS
}) => void;

onUpdateBundle: (
  id: string,
  bundle: {
    name: string;
    apiId: string;
    views: string;
    likes: string;
    shares: string;
    saves: string;
    comments: string; // ✅ ADD THIS
  }
) => void;
  onDeleteBundle: (id: string) => void;
}

function filterServices(services: ApiService[], keywords: string[]) {
  return services.filter((service) => {
    const name = service.name.toLowerCase();
    return keywords.some((keyword) => name.includes(keyword));
  });
}

function getApiServices(apis: ApiPanel[], apiId: string) {
  return apis.find((api) => api.id === apiId)?.services ?? [];
}

// 🔍 Searchable Dropdown Component
function SearchableSelect({
  options,
  value,
  onChange,
  placeholder,
  label,
}: {
  options: ApiService[];
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
  label: string;
}) {
  const [isOpen, setIsOpen] = useState(false);
  const [search, setSearch] = useState("");

  const filteredOptions = useMemo(() => {
    if (!search.trim()) return options;
    const query = search.toLowerCase();
    return options.filter(
      (opt) =>
        opt.name.toLowerCase().includes(query) ||
        opt.id.toLowerCase().includes(query)
    );
  }, [options, search]);

  const selectedOption = options.find((opt) => opt.id === value);

  return (
    <div className="relative">
      <label className="mb-1 block text-xs text-gray-500">{label}</label>
      
      {/* Selected value display */}
      <button
        type="button"
        onClick={() => setIsOpen(!isOpen)}
        className="w-full rounded-xl border border-yellow-500/30 bg-black px-3 py-2.5 text-left text-sm text-gray-100 transition-all hover:border-yellow-500/50 focus:border-yellow-500/50 focus:outline-none"
      >
        {selectedOption ? (
          <span className="flex items-center justify-between">
            <span className="truncate">{selectedOption.name}</span>
            <span className="ml-2 text-xs text-yellow-500">#{selectedOption.id}</span>
          </span>
        ) : (
          <span className="text-gray-600">{placeholder}</span>
        )}
      </button>

      {/* Dropdown */}
      {isOpen && (
        <>
          {/* Backdrop */}
          <div
            className="fixed inset-0 z-10"
            onClick={() => {
              setIsOpen(false);
              setSearch("");
            }}
          />

          {/* Options */}
          <div className="absolute z-20 mt-1 w-full rounded-xl border border-yellow-500/30 bg-black shadow-lg shadow-yellow-500/10">
            {/* Search input */}
            <div className="border-b border-yellow-500/20 p-2">
              <input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="🔍 Search services..."
                className="w-full rounded-lg border border-yellow-500/30 bg-gray-900 px-3 py-2 text-sm text-white placeholder-gray-600 outline-none focus:border-yellow-500/50"
                autoFocus
              />
            </div>

            {/* Options list */}
            <div className="max-h-64 overflow-y-auto">
              {filteredOptions.length === 0 && (
                <div className="p-4 text-center text-sm text-gray-600">
                  No services found
                </div>
              )}

              {filteredOptions.map((option) => (
                <button
                  key={option.id}
                  type="button"
                  onClick={() => {
                    onChange(option.id);
                    setIsOpen(false);
                    setSearch("");
                  }}
                  className={`w-full px-3 py-2.5 text-left text-sm transition-colors hover:bg-yellow-500/10 ${
                    value === option.id
                      ? "bg-yellow-500/20 text-yellow-300"
                      : "text-gray-300"
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <span className="truncate">{option.name}</span>
                    <span className="ml-2 text-xs text-yellow-600">#{option.id}</span>
                  </div>
                </button>
              ))}
            </div>

            {/* Result count */}
            {filteredOptions.length > 0 && (
              <div className="border-t border-yellow-500/20 px-3 py-2 text-xs text-gray-600">
                {filteredOptions.length} service{filteredOptions.length !== 1 ? "s" : ""} found
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}

export function BundleManager({ apis, bundles, onAddBundle, onUpdateBundle, onDeleteBundle }: BundleManagerProps) {
  const [showForm, setShowForm] = useState(false);
  const [editingBundleId, setEditingBundleId] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [apiId, setApiId] = useState("");
  const [views, setViews] = useState("");
  const [likes, setLikes] = useState("");
  const [shares, setShares] = useState("");
  const [saves, setSaves] = useState("");
  const [comments, setComments] = useState("");

  const viewOptions = useMemo(
    () => filterServices(getApiServices(apis, apiId), ["view", "views"]),
    [apis, apiId]
  );
  const likeOptions = useMemo(
    () => filterServices(getApiServices(apis, apiId), ["like", "likes"]),
    [apis, apiId]
  );
  const shareOptions = useMemo(
    () => filterServices(getApiServices(apis, apiId), ["share", "shares"]),
    [apis, apiId]
  );
  const saveOptions = useMemo(
    () => filterServices(getApiServices(apis, apiId), ["save", "saves"]),
    [apis, apiId]
  );
  const commentOptions = useMemo(
  () => filterServices(getApiServices(apis, apiId), ["comment", "comments"]),
  [apis, apiId]
);

  const resetForm = () => {
    setName("");
    setApiId("");
    setViews("");
    setLikes("");
    setShares("");
    setSaves("");
    setComments("");
    setEditingBundleId(null);
    setShowForm(false);
  };

  return (
    <section className="space-y-5">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <span className="text-2xl">📁</span>
          <h2 className="text-2xl font-bold tracking-tight text-yellow-400">Arsenal Bundles</h2>
        </div>
        <button
          type="button"
          onClick={() => {
            if (showForm) {
              resetForm();
              return;
            }
            setShowForm(true);
          }}
          className="rounded-lg border border-yellow-500/50 bg-yellow-500/10 px-4 py-2 text-sm font-medium text-yellow-300 transition hover:bg-yellow-500/20"
        >
          {showForm ? "Close" : "➕ Create Bundle"}
        </button>
      </div>

      {/* Form */}
      {showForm && (
        <form
          onSubmit={(event) => {
            event.preventDefault();
            if (!name.trim()) return;
            if (!apiId) return;
            if (!views.trim() || !likes.trim() || !shares.trim() || !saves.trim() || !comments.trim()) return;
            const payload = {
  name: name.trim(),
  apiId,
  views: views.trim(),
  likes: likes.trim(),
  shares: shares.trim(),
  saves: saves.trim(),
  comments: comments.trim(),
};
            if (editingBundleId) {
              onUpdateBundle(editingBundleId, payload);
            } else {
              onAddBundle(payload);
            }
            resetForm();
          }}
          className="grid gap-4 rounded-2xl border border-yellow-500/30 bg-gradient-to-br from-gray-900 to-black p-5 md:grid-cols-2"
        >
          <div className="md:col-span-2">
            <label className="mb-1 block text-xs text-gray-500">Bundle Name</label>
            <input
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder="e.g., Instagram Growth Package"
              className="w-full rounded-xl border border-yellow-500/30 bg-black px-3 py-2.5 text-sm text-gray-100 placeholder-gray-600 outline-none focus:border-yellow-500/50"
            />
          </div>

          <div className="md:col-span-2">
            <label className="mb-1 block text-xs text-gray-500">API Panel</label>
            <select
              value={apiId}
              onChange={(event) => {
                setApiId(event.target.value);
                setViews("");
                setLikes("");
                setShares("");
                setSaves("");
                setComments("");
              }}
              className="w-full rounded-xl border border-yellow-500/30 bg-black px-3 py-2.5 text-sm text-gray-100"
            >
              <option value="">Select API Panel</option>
              {apis.map((api) => (
                <option key={`bundle-api-${api.id}`} value={api.id}>
                  {api.name} ({api.services.length} services)
                </option>
              ))}
            </select>
          </div>

          {apiId && (
            <>
              <p className="text-xs uppercase tracking-wide text-yellow-500/60 md:col-span-2 flex items-center gap-2">
                <span>🎯</span> Service Configuration
              </p>

              {/* 🔍 SEARCHABLE DROPDOWNS */}
              <SearchableSelect
                options={viewOptions}
                value={views}
                onChange={setViews}
                placeholder="Select Views Service"
                label="👁️ Views Service"
              />

              <SearchableSelect
                options={likeOptions}
                value={likes}
                onChange={setLikes}
                placeholder="Select Likes Service"
                label="❤️ Likes Service"
              />

              <SearchableSelect
                options={shareOptions}
                value={shares}
                onChange={setShares}
                placeholder="Select Shares Service"
                label="🔄 Shares Service"
              />

              <SearchableSelect
                options={saveOptions}
                value={saves}
                onChange={setSaves}
                placeholder="Select Saves Service"
                label="💾 Saves Service"
              />
              <SearchableSelect
  options={commentOptions}
  value={comments}
  onChange={setComments}
  placeholder="Select Comments Service"
  label="💬 Comments Service"
/>
            </>
          )}

          <button
            type="submit"
            disabled={!apiId}
            className="md:col-span-2 rounded-lg border border-yellow-500/50 bg-yellow-500/20 px-3 py-2.5 text-sm font-medium text-yellow-300 transition hover:bg-yellow-500/30 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {editingBundleId ? "Update Bundle" : "Save Bundle"}
          </button>

          {editingBundleId && (
            <button
              type="button"
              onClick={resetForm}
              className="md:col-span-2 rounded-lg border border-gray-700 bg-gray-800 px-3 py-2 text-sm text-gray-300 transition hover:bg-gray-700"
            >
              Cancel Edit
            </button>
          )}
        </form>
      )}

      {/* Bundle Cards */}
      <div className="space-y-3">
        {bundles.length === 0 && (
          <div className="rounded-2xl border border-dashed border-yellow-500/30 bg-black p-8 text-center">
            <span className="text-4xl">📁</span>
            <p className="mt-2 text-sm text-gray-500">No bundles created yet</p>
            <p className="mt-1 text-xs text-gray-600">Create your first arsenal bundle</p>
          </div>
        )}

        {bundles.map((bundle) => (
          <article key={bundle.id} className="rounded-2xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-4">
            <h3 className="text-base font-semibold text-yellow-400">{bundle.name}</h3>
            <p className="mt-2 text-sm text-gray-500">
              Panel: <span className="text-gray-300">{apis.find((api) => api.id === bundle.apiId)?.name ?? "Unknown"}</span>
            </p>
            
            <div className="mt-3 grid grid-cols-2 gap-2">
              <div className="rounded-lg border border-yellow-500/20 bg-yellow-500/5 px-3 py-2">
                <p className="text-xs text-gray-600">👁️ Views</p>
                <p className="mt-0.5 text-xs font-mono text-yellow-400">{bundle.serviceIds.views}</p>
              </div>
              <div className="rounded-lg border border-yellow-500/20 bg-yellow-500/5 px-3 py-2">
                <p className="text-xs text-gray-600">❤️ Likes</p>
                <p className="mt-0.5 text-xs font-mono text-yellow-400">{bundle.serviceIds.likes}</p>
              </div>
              <div className="rounded-lg border border-yellow-500/20 bg-yellow-500/5 px-3 py-2">
                <p className="text-xs text-gray-600">🔄 Shares</p>
                <p className="mt-0.5 text-xs font-mono text-yellow-400">{bundle.serviceIds.shares}</p>
              </div>
              <div className="rounded-lg border border-yellow-500/20 bg-yellow-500/5 px-3 py-2">
                <p className="text-xs text-gray-600">💾 Saves</p>
                <p className="mt-0.5 text-xs font-mono text-yellow-400">{bundle.serviceIds.saves}</p>
              </div>
              <div className="rounded-lg border border-yellow-500/20 bg-yellow-500/5 px-3 py-2">
  <p className="text-xs text-gray-600">💬 Comments</p>
  <p className="mt-0.5 text-xs font-mono text-yellow-400">{bundle.serviceIds.comments}</p>
</div>
            </div>

            <div className="mt-3 flex items-center gap-2">
              <button
                type="button"
                onClick={() => {
                  setEditingBundleId(bundle.id);
                  setName(bundle.name);
                  setApiId(bundle.apiId);
                  setViews(bundle.serviceIds.views);
                  setLikes(bundle.serviceIds.likes);
                  setShares(bundle.serviceIds.shares);
                  setSaves(bundle.serviceIds.saves);
                  setComments(bundle.serviceIds.comments || "");
                  setShowForm(true);
                }}
                className="rounded-md border border-yellow-500/30 bg-yellow-500/10 px-2.5 py-1.5 text-xs text-yellow-300 transition hover:bg-yellow-500/20"
              >
                ✏️ Edit
              </button>
              <button
                type="button"
                onClick={() => {
                  const confirmed = window.confirm("Are you sure you want to delete this bundle?");
                  if (!confirmed) return;
                  onDeleteBundle(bundle.id);
                  if (editingBundleId === bundle.id) {
                    resetForm();
                  }
                }}
                className="rounded-md border border-red-500/30 bg-red-500/10 px-2.5 py-1.5 text-xs text-red-300 transition hover:bg-red-500/20"
              >
                🗑 Delete
              </button>
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}

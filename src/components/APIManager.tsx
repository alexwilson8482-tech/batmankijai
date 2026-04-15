import { useState } from "react";
import type { ApiPanel } from "../types/order";

interface APIManagerProps {
  apis: ApiPanel[];
  onAddApi: (api: { name: string; url: string; key: string }) => void;
  onEditApi: (id: string, api: { name: string; url: string; key: string }) => void;
  onDeleteApi: (id: string) => void;
  onToggleStatus: (id: string) => void;
  onFetchServices: (id: string) => void;
  fetchingApiId: string | null;
}

export function APIManager({ apis, onAddApi, onEditApi, onDeleteApi, onToggleStatus, onFetchServices, fetchingApiId }: APIManagerProps) {
  const [showForm, setShowForm] = useState(false);
  const [name, setName] = useState("");
  const [url, setUrl] = useState("");
  const [key, setKey] = useState("");

  const [editingId, setEditingId] = useState<string | null>(null);
  const [editName, setEditName] = useState("");
  const [editUrl, setEditUrl] = useState("");
  const [editKey, setEditKey] = useState("");

  const [deleteId, setDeleteId] = useState<string | null>(null);

  const startEdit = (api: ApiPanel) => {
    setEditingId(api.id);
    setEditName(api.name);
    setEditUrl(api.url);
    setEditKey(api.key);
  };

  const saveEdit = () => {
    if (!editName.trim() || !editUrl.trim() || !editKey.trim() || !editingId) return;
    onEditApi(editingId, { name: editName.trim(), url: editUrl.trim(), key: editKey.trim() });
    setEditingId(null);
  };

  const cancelEdit = () => {
    setEditingId(null);
  };

  const confirmDelete = (id: string) => {
    setDeleteId(id);
  };

  const handleDelete = () => {
    if (deleteId) {
      onDeleteApi(deleteId);
      setDeleteId(null);
    }
  };

  return (
    <section className="space-y-5">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <span className="text-2xl">🔗</span>
          <h2 className="text-2xl font-bold tracking-tight text-yellow-400">API Network</h2>
        </div>
        <button
          type="button"
          onClick={() => setShowForm((prev) => !prev)}
          className="rounded-lg border border-yellow-500/50 bg-yellow-500/10 px-4 py-2 text-sm font-medium text-yellow-300 transition hover:bg-yellow-500/20"
        >
          ➕ Add Connection
        </button>
      </div>

      {showForm && (
        <form
          onSubmit={(event) => {
            event.preventDefault();
            if (!name.trim() || !url.trim() || !key.trim()) return;
            onAddApi({ name: name.trim(), url: url.trim(), key: key.trim() });
            setName("");
            setUrl("");
            setKey("");
            setShowForm(false);
          }}
          className="grid gap-3 rounded-2xl border border-yellow-500/30 bg-gradient-to-br from-gray-900 to-black p-5 md:grid-cols-3"
        >
          <input value={name} onChange={(event) => setName(event.target.value)} placeholder="Connection Name" className="rounded-xl border border-gray-700 bg-black px-3 py-2.5 text-sm text-gray-100 placeholder-gray-600 outline-none focus:border-yellow-500/50" />
          <input value={url} onChange={(event) => setUrl(event.target.value)} placeholder="API URL" className="rounded-xl border border-gray-700 bg-black px-3 py-2.5 text-sm text-gray-100 placeholder-gray-600 outline-none focus:border-yellow-500/50" />
          <input value={key} onChange={(event) => setKey(event.target.value)} placeholder="API Key" className="rounded-xl border border-gray-700 bg-black px-3 py-2.5 text-sm text-gray-100 placeholder-gray-600 outline-none focus:border-yellow-500/50" />
          <button type="submit" className="md:col-span-3 rounded-lg border border-yellow-500/50 bg-yellow-500/20 px-3 py-2 text-sm font-medium text-yellow-300 transition hover:bg-yellow-500/30">Save Connection</button>
        </form>
      )}

      <div className="space-y-3">
        {apis.length === 0 && (
          <div className="rounded-2xl border border-dashed border-yellow-500/30 bg-black p-8 text-center">
            <span className="text-4xl">🦇</span>
            <p className="mt-2 text-sm text-gray-500">No API connections established</p>
            <p className="mt-1 text-xs text-gray-600">Add your first connection to the network</p>
          </div>
        )}
        {apis.map((api) => (
          <article key={api.id} className="rounded-2xl border border-yellow-500/20 bg-gradient-to-br from-gray-900 to-black p-4">
            {editingId === api.id ? (
              <div className="space-y-3">
                <input value={editName} onChange={(e) => setEditName(e.target.value)} placeholder="Connection Name" className="w-full rounded-xl border border-gray-700 bg-black px-3 py-2.5 text-sm text-gray-100 outline-none focus:border-yellow-500/50" />
                <input value={editUrl} onChange={(e) => setEditUrl(e.target.value)} placeholder="API URL" className="w-full rounded-xl border border-gray-700 bg-black px-3 py-2.5 text-sm text-gray-100 outline-none focus:border-yellow-500/50" />
                <input value={editKey} onChange={(e) => setEditKey(e.target.value)} placeholder="API Key" className="w-full rounded-xl border border-gray-700 bg-black px-3 py-2.5 text-sm text-gray-100 outline-none focus:border-yellow-500/50" />
                <div className="flex gap-2">
                  <button type="button" onClick={saveEdit} className="rounded-lg border border-yellow-500/50 bg-yellow-500/20 px-3 py-1.5 text-sm text-yellow-300 transition hover:bg-yellow-500/30">Save</button>
                  <button type="button" onClick={cancelEdit} className="rounded-lg border border-gray-700 bg-gray-800 px-3 py-1.5 text-sm text-gray-300 transition hover:bg-gray-700">Cancel</button>
                </div>
              </div>
            ) : deleteId === api.id ? (
              <div className="space-y-3">
                <p className="text-sm text-red-400">⚠️ Disconnect "{api.name}" from the network?</p>
                <div className="flex gap-2">
                  <button type="button" onClick={handleDelete} className="rounded-lg border border-red-500/50 bg-red-500/20 px-3 py-1.5 text-sm text-red-300 transition hover:bg-red-500/30">Yes, Disconnect</button>
                  <button type="button" onClick={() => setDeleteId(null)} className="rounded-lg border border-gray-700 bg-gray-800 px-3 py-1.5 text-sm text-gray-300 transition hover:bg-gray-700">Cancel</button>
                </div>
              </div>
            ) : (
              <div className="flex items-center justify-between gap-4">
                <div>
                  <h3 className="text-base font-semibold text-yellow-400">{api.name}</h3>
                  <p className="text-sm text-gray-500">{api.url}</p>
                  <p className="mt-1 text-xs text-gray-600">{api.services.length} services linked</p>
                  {api.lastFetchError && <p className="mt-1 text-xs text-red-400">{api.lastFetchError}</p>}
                </div>
                <div className="text-right">
                  <p className={`text-sm font-semibold ${api.status === "Active" ? "text-emerald-400" : "text-gray-500"}`}>{api.status}</p>
                  <button type="button" onClick={() => onToggleStatus(api.id)} className="mt-1 block text-xs text-yellow-500 hover:text-yellow-400">
                    Toggle Status
                  </button>
                  <button
                    type="button"
                    onClick={() => onFetchServices(api.id)}
                    disabled={fetchingApiId === api.id}
                    className="mt-2 rounded-md border border-yellow-500/50 bg-yellow-500/10 px-2.5 py-1 text-xs text-yellow-300 transition hover:bg-yellow-500/20 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {fetchingApiId === api.id ? "Syncing..." : "Sync Services"}
                  </button>
                  <div className="mt-2 flex justify-end gap-2">
                    <button type="button" onClick={() => startEdit(api)} className="rounded-md border border-yellow-500/30 bg-yellow-500/10 px-2.5 py-1 text-xs text-yellow-300 transition hover:bg-yellow-500/20">
                      ✏️ Edit
                    </button>
                    <button type="button" onClick={() => confirmDelete(api.id)} className="rounded-md border border-red-500/30 bg-red-500/10 px-2.5 py-1 text-xs text-red-300 transition hover:bg-red-500/20">
                      🗑️ Delete
                    </button>
                  </div>
                </div>
              </div>
            )}
          </article>
        ))}
      </div>
    </section>
  );
}

import { useEffect, useState } from "react";
import {
  fetchPlanPreview,
  pushPlanToAirtable,
  clearScheduledPlan,
} from "../api/scheduling";

export default function PlanPreview() {
  const [data, setData] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [actionMsg, setActionMsg] = useState<string | null>(null);

  useEffect(() => {
    fetchPlanPreview()
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  async function handlePush() {
    try {
      setActionMsg(null);
      const result = await pushPlanToAirtable();
      setActionMsg(
        result.success ? "Plan pushed to Airtable." : "Push failed.",
      );
    } catch (e) {
      setActionMsg(`Error: ${e instanceof Error ? e.message : e}`);
    }
  }

  async function handleClear() {
    if (!confirm("Clear the scheduled plan?")) return;
    try {
      setActionMsg(null);
      const result = await clearScheduledPlan();
      setActionMsg(result.success ? "Plan cleared." : "Clear failed.");
    } catch (e) {
      setActionMsg(`Error: ${e instanceof Error ? e.message : e}`);
    }
  }

  if (loading) {
    return (
      <div className="text-center py-8 text-text-secondary">
        Loading plan preview...
      </div>
    );
  }

  if (error) {
    return <div className="text-center py-8 text-accent-red">{error}</div>;
  }

  const experiments = (data?.experiments as Record<string, unknown>[]) || [];

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-bold">Experiment Plan Preview</h1>
        <div className="flex gap-2">
          <button
            onClick={handlePush}
            className="px-4 py-2 bg-green-700 text-white rounded text-sm hover:opacity-90"
          >
            Push Plan to Airtable
          </button>
          <button
            onClick={handleClear}
            className="px-4 py-2 bg-red-700 text-white rounded text-sm hover:opacity-90"
          >
            Clear Scheduled Plan
          </button>
        </div>
      </div>

      {actionMsg && (
        <div className="mb-4 p-3 bg-bg-secondary border border-border rounded text-sm">
          {actionMsg}
        </div>
      )}

      <div className="grid grid-cols-3 gap-4 mb-6">
        <div className="bg-bg-secondary border border-border rounded p-4">
          <div className="text-text-secondary text-sm">Queued Experiments</div>
          <div className="text-2xl font-bold">{experiments.length}</div>
        </div>
        <div className="bg-bg-secondary border border-border rounded p-4">
          <div className="text-text-secondary text-sm">Total Cages</div>
          <div className="text-2xl font-bold">
            {String(data?.total_cages ?? "—")}
          </div>
        </div>
        <div className="bg-bg-secondary border border-border rounded p-4">
          <div className="text-text-secondary text-sm">Total Boxes</div>
          <div className="text-2xl font-bold">
            {String(data?.total_boxes ?? "—")}
          </div>
        </div>
      </div>

      {data?.message && (
        <div className="mb-4 p-3 bg-blue-900/30 border border-blue-700 rounded text-sm text-blue-200">
          {String(data.message)}
        </div>
      )}

      <div className="overflow-x-auto">
        <table className="w-full text-sm border-collapse">
          <thead>
            <tr className="border-b border-border">
              <th className="text-left px-3 py-2 text-text-secondary">Priority</th>
              <th className="text-left px-3 py-2 text-text-secondary">Assignment</th>
              <th className="text-left px-3 py-2 text-text-secondary">Days</th>
              <th className="text-left px-3 py-2 text-text-secondary">Notes</th>
              <th className="text-left px-3 py-2 text-text-secondary">Status</th>
            </tr>
          </thead>
          <tbody>
            {experiments.map((exp, i) => {
              const f = (exp.fields || exp) as Record<string, unknown>;
              return (
                <tr key={i} className="border-b border-border/50 hover:bg-bg-hover">
                  <td className="px-3 py-2">{String(f.priority ?? "")}</td>
                  <td className="px-3 py-2">{String(f.assignment ?? "")}</td>
                  <td className="px-3 py-2">{String(f.num_days ?? "")}</td>
                  <td className="px-3 py-2 max-w-xs truncate">
                    {String(f.notes ?? "")}
                  </td>
                  <td className="px-3 py-2">{String(f.status ?? "queued")}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

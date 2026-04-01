import { useEffect, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { fetchExperiments, deleteExperiment } from "../api/experiments";
import { recalculateTimes } from "../api/scheduling";
import type { ExperimentRecord } from "../types";

const HIDDEN_FIELDS = new Set([
  "unique_cage_ids",
  "unique_manipulation_ids",
  "config_file",
  "experiment",
  "selected_tasks",
  "experiment_time_minutes",
]);

export default function ExperimentQueue() {
  const navigate = useNavigate();
  const [experiments, setExperiments] = useState<ExperimentRecord[]>([]);
  const [headers, setHeaders] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showManipDetails, setShowManipDetails] = useState(false);
  const [sortField, setSortField] = useState<string | null>(null);
  const [sortAsc, setSortAsc] = useState(true);

  useEffect(() => {
    loadExperiments();
  }, []);

  async function loadExperiments() {
    try {
      setLoading(true);
      const data = await fetchExperiments();
      setExperiments(data.experiments);
      setHeaders(data.headers.filter((h) => !HIDDEN_FIELDS.has(h)));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load experiments");
    } finally {
      setLoading(false);
    }
  }

  async function handleDelete(recordId: string) {
    if (!confirm("Are you sure you want to delete this experiment?")) return;
    try {
      await deleteExperiment(recordId);
      setExperiments((prev) => prev.filter((e) => e.id !== recordId));
    } catch (e) {
      alert(`Failed to delete: ${e instanceof Error ? e.message : e}`);
    }
  }

  async function handleRecalculate() {
    try {
      await recalculateTimes();
      await loadExperiments();
    } catch (e) {
      alert(`Failed to recalculate: ${e instanceof Error ? e.message : e}`);
    }
  }

  function handleSort(field: string) {
    if (sortField === field) {
      setSortAsc(!sortAsc);
    } else {
      setSortField(field);
      setSortAsc(true);
    }
  }

  const sorted = sortField
    ? [...experiments].sort((a, b) => {
        const va = a.fields[sortField as keyof typeof a.fields] ?? "";
        const vb = b.fields[sortField as keyof typeof b.fields] ?? "";
        const cmp = String(va).localeCompare(String(vb), undefined, {
          numeric: true,
        });
        return sortAsc ? cmp : -cmp;
      })
    : experiments;

  function renderCellValue(header: string, record: ExperimentRecord) {
    const value = record.fields[header as keyof typeof record.fields];
    if (header === "is_chronic") {
      return value ? (
        <span className="text-green-400">&#x2713;</span>
      ) : (
        <span className="text-red-400">&#x2717;</span>
      );
    }
    if (header === "manipulations") {
      return record.manipulations_display;
    }
    if (value === null || value === undefined) return "";
    if (Array.isArray(value)) return value.join(", ");
    return String(value);
  }

  if (loading) {
    return <div className="text-center py-8 text-text-secondary">Loading experiments...</div>;
  }

  if (error) {
    return <div className="text-center py-8 text-accent-red">{error}</div>;
  }

  return (
    <div>
      {/* Header actions */}
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-bold">Experiment Queue</h1>
        <div className="flex gap-2">
          <Link
            to="/add"
            className="px-4 py-2 bg-accent-blue text-white rounded text-sm hover:opacity-90"
          >
            Add New Experiment
          </Link>
          <Link
            to="/plan-preview"
            className="px-4 py-2 bg-purple-600 text-white rounded text-sm hover:opacity-90"
          >
            Generate Plan Preview
          </Link>
          <button
            onClick={handleRecalculate}
            className="px-4 py-2 bg-yellow-600 text-white rounded text-sm hover:opacity-90"
          >
            Recalculate All Times
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm border-collapse">
          <thead>
            <tr className="border-b border-border">
              {headers.map((header) => (
                <th
                  key={header}
                  onClick={() => handleSort(header)}
                  className="text-left px-3 py-2 text-text-secondary cursor-pointer hover:text-text-primary select-none"
                >
                  {header}
                  {sortField === header && (sortAsc ? " ▲" : " ▼")}
                  {header === "manipulations" && (
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        setShowManipDetails(!showManipDetails);
                      }}
                      className="ml-1 text-xs text-accent-blue"
                      title="Toggle manipulation details"
                    >
                      {showManipDetails ? "▼" : "▶"}
                    </button>
                  )}
                </th>
              ))}
              {showManipDetails && (
                <>
                  <th className="text-left px-3 py-2 text-text-secondary">Drugs</th>
                  <th className="text-left px-3 py-2 text-text-secondary">Safety</th>
                  <th className="text-left px-3 py-2 text-text-secondary">Dose (mg/kg)</th>
                </>
              )}
              <th className="text-left px-3 py-2 text-text-secondary">Actions</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((record) => (
              <tr
                key={record.id}
                className="border-b border-border/50 hover:bg-bg-hover"
              >
                {headers.map((header) => (
                  <td key={header} className="px-3 py-2">
                    {renderCellValue(header, record)}
                  </td>
                ))}
                {showManipDetails && (
                  <>
                    <td className="px-3 py-2">
                      {record.manipulation_details?.map((m, i) => (
                        <div key={i} className="text-xs">{m.drugs.join(", ")}</div>
                      ))}
                    </td>
                    <td className="px-3 py-2">
                      {record.manipulation_details?.map((m, i) => (
                        <div key={i} className="text-xs">{m.safety.join(", ")}</div>
                      ))}
                    </td>
                    <td className="px-3 py-2">
                      {record.manipulation_details?.map((m, i) => (
                        <div key={i} className="text-xs">{m.dose_mg_kg.join(", ")}</div>
                      ))}
                    </td>
                  </>
                )}
                <td className="px-3 py-2 flex gap-1">
                  <Link
                    to={`/edit/${record.id}`}
                    className="px-2 py-1 bg-bg-tertiary text-text-primary rounded text-xs hover:bg-bg-hover"
                  >
                    Edit
                  </Link>
                  <button
                    onClick={() => handleDelete(record.id)}
                    className="px-2 py-1 bg-red-900 text-red-200 rounded text-xs hover:bg-red-800"
                    title="Delete experiment"
                  >
                    X
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {experiments.length === 0 && (
        <p className="text-center py-8 text-text-secondary">
          No experiments in the queue.
        </p>
      )}
    </div>
  );
}

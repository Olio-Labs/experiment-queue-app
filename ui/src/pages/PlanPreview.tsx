import { useEffect, useState } from "react";
import {
  fetchPlanPreview,
  pushPlanToAirtable,
  clearScheduledPlan,
  recalculateTimes,
  pushToCalendar,
} from "../api/scheduling";
import type {
  ScheduledExperimentResult,
  SchedulingPreviewResponse,
} from "../types";

export default function PlanPreview() {
  const [data, setData] = useState<SchedulingPreviewResponse | null>(null);
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
    if (!data?.scheduled_experiments.length) return;
    try {
      setActionMsg(null);
      const result = await pushPlanToAirtable(data.scheduled_experiments);
      setActionMsg(
        result.success
          ? `Plan pushed: ${result.message}`
          : `Push failed: ${result.errors?.join(", ")}`,
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
      setActionMsg(result.success ? result.message : "Clear failed.");
    } catch (e) {
      setActionMsg(`Error: ${e instanceof Error ? e.message : e}`);
    }
  }

  async function handleRecalculate() {
    try {
      setActionMsg(null);
      const result = await recalculateTimes();
      setActionMsg(result.message);
    } catch (e) {
      setActionMsg(`Error: ${e instanceof Error ? e.message : e}`);
    }
  }

  async function handleCalendarPush() {
    if (!data?.scheduled_experiments.length) return;
    try {
      setActionMsg(null);
      const result = await pushToCalendar(data.scheduled_experiments);
      setActionMsg(
        result.success
          ? `${result.events_created} calendar events created.`
          : "Calendar push failed.",
      );
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

  if (!data) return null;

  const scheduled = data.scheduled_experiments || [];
  const inProgress = data.in_progress_experiments || [];
  const alreadyScheduled = data.already_scheduled_experiments || [];
  const deferred = data.deferred_experiments || [];
  const drugWarnings = data.drug_warnings || [];
  const errors = data.scheduling_errors || [];

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-2xl font-bold">Experiment Plan Preview</h1>
        <div className="flex gap-2">
          <button
            onClick={handlePush}
            className="px-4 py-2 bg-green-700 text-white rounded text-sm hover:opacity-90"
            disabled={!scheduled.length}
          >
            Push Plan to Airtable
          </button>
          <button
            onClick={handleCalendarPush}
            className="px-4 py-2 bg-blue-700 text-white rounded text-sm hover:opacity-90"
            disabled={!scheduled.length}
          >
            Push to Calendar
          </button>
          <button
            onClick={handleRecalculate}
            className="px-4 py-2 bg-yellow-700 text-white rounded text-sm hover:opacity-90"
          >
            Recalculate Times
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

      {errors.length > 0 && (
        <div className="mb-4 p-3 bg-red-900/30 border border-red-700 rounded text-sm text-red-200">
          {errors.map((e, i) => (
            <div key={i}>{e}</div>
          ))}
        </div>
      )}

      {/* Summary cards */}
      <div className="grid grid-cols-4 gap-4 mb-6">
        <StatCard label="Scheduled" value={scheduled.length} />
        <StatCard label="In Progress" value={inProgress.length} />
        <StatCard label="Already Scheduled" value={alreadyScheduled.length} />
        <StatCard label="Deferred" value={deferred.length} />
      </div>

      <div className="grid grid-cols-3 gap-4 mb-6">
        <StatCard label="Total Cages" value={data.total_cages} />
        <StatCard label="Total Boxes" value={data.total_boxes} />
        <StatCard
          label="Drug Warnings"
          value={drugWarnings.length}
          accent={drugWarnings.length > 0}
        />
      </div>

      {/* Drug warnings */}
      {drugWarnings.length > 0 && (
        <div className="mb-6">
          <h2 className="text-lg font-semibold mb-2">Drug Warnings</h2>
          <div className="bg-yellow-900/20 border border-yellow-700 rounded p-3 text-sm">
            {drugWarnings.map((w, i) => (
              <div key={i} className="text-yellow-200">
                {w}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Scheduled experiments */}
      {scheduled.length > 0 && (
        <ExperimentTable
          title="Scheduled Experiments"
          experiments={scheduled}
          showDates
          showCages
          showColors
        />
      )}

      {/* In-progress */}
      {inProgress.length > 0 && (
        <ExperimentTable
          title="In-Progress Experiments"
          experiments={inProgress}
          showDates
        />
      )}

      {/* Already scheduled */}
      {alreadyScheduled.length > 0 && (
        <ExperimentTable
          title="Already Scheduled"
          experiments={alreadyScheduled}
          showDates
        />
      )}

      {/* Deferred */}
      {deferred.length > 0 && (
        <div className="mb-6">
          <h2 className="text-lg font-semibold mb-2">
            Deferred Experiments ({deferred.length})
          </h2>
          <div className="overflow-x-auto">
            <table className="w-full text-sm border-collapse">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left px-3 py-2 text-text-secondary">
                    Priority
                  </th>
                  <th className="text-left px-3 py-2 text-text-secondary">
                    Assignment
                  </th>
                  <th className="text-left px-3 py-2 text-text-secondary">
                    Days
                  </th>
                  <th className="text-left px-3 py-2 text-text-secondary">
                    Reason
                  </th>
                </tr>
              </thead>
              <tbody>
                {deferred.map((exp, i) => (
                  <tr
                    key={i}
                    className="border-b border-border/50 hover:bg-bg-hover"
                  >
                    <td className="px-3 py-2">{String(exp.priority)}</td>
                    <td className="px-3 py-2">{exp.assignment}</td>
                    <td className="px-3 py-2">{exp.num_days}</td>
                    <td className="px-3 py-2 text-yellow-300">
                      {exp.deferral_reason || "No available slot"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

function StatCard({
  label,
  value,
  accent,
}: {
  label: string;
  value: number;
  accent?: boolean;
}) {
  return (
    <div className="bg-bg-secondary border border-border rounded p-4">
      <div className="text-text-secondary text-sm">{label}</div>
      <div
        className={`text-2xl font-bold ${accent ? "text-yellow-400" : ""}`}
      >
        {value}
      </div>
    </div>
  );
}

function ExperimentTable({
  title,
  experiments,
  showDates,
  showCages,
  showColors,
}: {
  title: string;
  experiments: ScheduledExperimentResult[];
  showDates?: boolean;
  showCages?: boolean;
  showColors?: boolean;
}) {
  return (
    <div className="mb-6">
      <h2 className="text-lg font-semibold mb-2">
        {title} ({experiments.length})
      </h2>
      <div className="overflow-x-auto">
        <table className="w-full text-sm border-collapse">
          <thead>
            <tr className="border-b border-border">
              <th className="text-left px-3 py-2 text-text-secondary">
                Priority
              </th>
              <th className="text-left px-3 py-2 text-text-secondary">
                Assignment
              </th>
              <th className="text-left px-3 py-2 text-text-secondary">
                Days
              </th>
              <th className="text-left px-3 py-2 text-text-secondary">
                Time (min)
              </th>
              {showDates && (
                <>
                  <th className="text-left px-3 py-2 text-text-secondary">
                    Start
                  </th>
                  <th className="text-left px-3 py-2 text-text-secondary">
                    End
                  </th>
                </>
              )}
              {showCages && (
                <th className="text-left px-3 py-2 text-text-secondary">
                  Cages
                </th>
              )}
              {showColors && (
                <th className="text-left px-3 py-2 text-text-secondary">
                  Syringe Colors
                </th>
              )}
              <th className="text-left px-3 py-2 text-text-secondary">
                Notes
              </th>
            </tr>
          </thead>
          <tbody>
            {experiments.map((exp, i) => (
              <tr
                key={i}
                className="border-b border-border/50 hover:bg-bg-hover"
              >
                <td className="px-3 py-2">{String(exp.priority)}</td>
                <td className="px-3 py-2">{exp.assignment}</td>
                <td className="px-3 py-2">{exp.num_days}</td>
                <td className="px-3 py-2">
                  {exp.experiment_time_daily
                    ? exp.experiment_time_daily.toFixed(0)
                    : "—"}
                </td>
                {showDates && (
                  <>
                    <td className="px-3 py-2">
                      {exp.scheduled_start_date || "—"}
                    </td>
                    <td className="px-3 py-2">
                      {exp.scheduled_end_date || "—"}
                    </td>
                  </>
                )}
                {showCages && (
                  <td className="px-3 py-2 max-w-xs">
                    <span className="text-xs">
                      {exp.assigned_cages?.join(", ") || "—"}
                    </span>
                  </td>
                )}
                {showColors && (
                  <td className="px-3 py-2">
                    {exp.syringe_colors
                      ? Object.entries(exp.syringe_colors)
                          .map(([m, c]) => `${m}:${c}`)
                          .join(", ")
                      : "—"}
                  </td>
                )}
                <td className="px-3 py-2 max-w-xs truncate">
                  {exp.notes || ""}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {experiments.some((e) => e.warnings?.length > 0) && (
        <div className="mt-2 text-xs text-yellow-400">
          {experiments
            .filter((e) => e.warnings?.length)
            .map((e, i) => (
              <div key={i}>
                {e.experiment_id || e.record_id}: {e.warnings.join("; ")}
              </div>
            ))}
        </div>
      )}
    </div>
  );
}

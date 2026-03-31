import { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import {
  fetchExperiment,
  fetchFormOptions,
  updateExperiment,
} from "../api/experiments";

export default function EditExperiment() {
  const { recordId } = useParams<{ recordId: string }>();
  const navigate = useNavigate();
  const [options, setOptions] = useState<Record<string, string[]>>({});
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [form, setForm] = useState({
    num_days: "1",
    priority: "5",
    config_file: "",
    assignment: "pseudorandom",
    is_chronic: false,
    notes: "",
    earliest_start_date: "",
    cages_per_manip: "",
    cages_per_vehicle: "4",
  });

  useEffect(() => {
    if (!recordId) return;
    Promise.all([fetchExperiment(recordId), fetchFormOptions()])
      .then(([expData, optData]) => {
        setOptions(optData.options);
        const fields = (expData.experiment as Record<string, unknown>)?.fields as Record<string, unknown> || {};
        setForm({
          num_days: String(fields.num_days || 1),
          priority: String(fields.priority || 5),
          config_file: String(fields.config_file || ""),
          assignment: String(fields.assignment || "pseudorandom"),
          is_chronic: Boolean(fields.is_chronic),
          notes: String(fields.notes || ""),
          earliest_start_date: String(fields.earliest_start_date || ""),
          cages_per_manip: String(fields.cages_per_manip || ""),
          cages_per_vehicle: String(fields.cages_per_vehicle || "4"),
        });
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [recordId]);

  function handleChange(
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>,
  ) {
    const { name, value, type } = e.target;
    setForm((prev) => ({
      ...prev,
      [name]: type === "checkbox" ? (e.target as HTMLInputElement).checked : value,
    }));
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!recordId) return;
    setSubmitting(true);
    setError(null);
    try {
      const fields: Record<string, unknown> = {
        num_days: parseInt(form.num_days),
        priority: parseInt(form.priority),
        config_file: form.config_file,
        assignment: form.assignment.toLowerCase(),
        is_chronic: form.is_chronic,
        notes: form.notes,
      };
      if (form.earliest_start_date) {
        fields.earliest_start_date = form.earliest_start_date;
      }
      if (form.cages_per_manip) {
        fields.cages_per_manip = parseInt(form.cages_per_manip);
      }
      if (form.cages_per_vehicle) {
        fields.cages_per_vehicle = parseInt(form.cages_per_vehicle);
      }
      await updateExperiment(recordId, fields);
      navigate("/queue");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to update experiment");
    } finally {
      setSubmitting(false);
    }
  }

  if (loading) {
    return <div className="text-center py-8 text-text-secondary">Loading experiment...</div>;
  }

  return (
    <div className="max-w-2xl mx-auto">
      <h1 className="text-2xl font-bold mb-6">Edit Experiment</h1>

      {error && (
        <div className="mb-4 p-3 bg-red-900/50 border border-red-700 rounded text-red-200 text-sm">
          {error}
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm text-text-secondary mb-1">Number of Days *</label>
            <input type="number" name="num_days" value={form.num_days} onChange={handleChange} min="1" required className="w-full bg-bg-tertiary border border-border rounded px-3 py-2 text-sm focus:border-accent-blue outline-none" />
          </div>
          <div>
            <label className="block text-sm text-text-secondary mb-1">Priority *</label>
            <select name="priority" value={form.priority} onChange={handleChange} required className="w-full bg-bg-tertiary border border-border rounded px-3 py-2 text-sm focus:border-accent-blue outline-none">
              {(options["priority"] || ["1","2","3","4","5","6","7","8","9","10"]).map((o) => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
          </div>
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm text-text-secondary mb-1">Config File *</label>
            <select name="config_file" value={form.config_file} onChange={handleChange} required className="w-full bg-bg-tertiary border border-border rounded px-3 py-2 text-sm focus:border-accent-blue outline-none">
              <option value="">Select...</option>
              {(options["config_file"] || []).map((o) => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm text-text-secondary mb-1">Assignment *</label>
            <select name="assignment" value={form.assignment} onChange={handleChange} required className="w-full bg-bg-tertiary border border-border rounded px-3 py-2 text-sm focus:border-accent-blue outline-none">
              {(options["assignment"] || ["pseudorandom", "direct_mapping"]).map((o) => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
          </div>
        </div>

        <div>
          <label className="block text-sm text-text-secondary mb-1">Earliest Start Date</label>
          <input type="date" name="earliest_start_date" value={form.earliest_start_date} onChange={handleChange} className="w-full bg-bg-tertiary border border-border rounded px-3 py-2 text-sm focus:border-accent-blue outline-none" />
        </div>

        <div className="flex items-center gap-2">
          <input type="checkbox" name="is_chronic" checked={form.is_chronic} onChange={handleChange} id="is_chronic_edit" className="rounded" />
          <label htmlFor="is_chronic_edit" className="text-sm text-text-secondary">Chronic experiment</label>
        </div>

        <div>
          <label className="block text-sm text-text-secondary mb-1">Notes</label>
          <textarea name="notes" value={form.notes} onChange={handleChange} rows={6} className="w-full bg-bg-tertiary border border-border rounded px-3 py-2 text-sm focus:border-accent-blue outline-none font-mono" />
        </div>

        <div className="flex gap-3 pt-2">
          <button type="submit" disabled={submitting} className="px-6 py-2 bg-accent-blue text-white rounded text-sm hover:opacity-90 disabled:opacity-50">
            {submitting ? "Saving..." : "Save Changes"}
          </button>
          <button type="button" onClick={() => navigate("/queue")} className="px-6 py-2 bg-bg-tertiary text-text-primary rounded text-sm hover:bg-bg-hover">
            Cancel
          </button>
        </div>
      </form>
    </div>
  );
}

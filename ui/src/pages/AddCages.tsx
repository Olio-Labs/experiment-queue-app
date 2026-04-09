import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  fetchCageFormOptions,
  previewAddCages,
  createCages,
} from "../api/cages";
import type { AddCagesFormData, CagePreview } from "../types";

type Step = "form" | "preview" | "success";

export default function AddCages() {
  const navigate = useNavigate();
  const [step, setStep] = useState<Step>("form");
  const [supplierOptions, setSupplierOptions] = useState<string[]>([]);
  const [strainOptions, setStrainOptions] = useState<string[]>([]);
  const [nextCageNum, setNextCageNum] = useState(0);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [form, setForm] = useState<AddCagesFormData>({
    mice_per_cage: 5,
    num_male_cages: 0,
    num_female_cages: 0,
    strain: "",
    supplier: "",
    dob: "",
    date_received: "",
  });

  const [preview, setPreview] = useState<{
    cages: CagePreview[];
    summary: Record<string, unknown>;
  } | null>(null);

  const [successStats, setSuccessStats] = useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    fetchCageFormOptions()
      .then((data) => {
        setSupplierOptions(data.supplier_options);
        setStrainOptions(data.strain_options);
        setNextCageNum(data.next_cage_num);
        setForm((prev) => ({
          ...prev,
          strain: data.strain_options[0] || "",
          supplier: data.supplier_options[0] || "",
        }));
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  function handleChange(
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>,
  ) {
    const { name, value, type } = e.target;
    setForm((prev) => ({
      ...prev,
      [name]: type === "number" ? parseInt(value) || 0 : value,
    }));
  }

  async function handlePreview(e: React.FormEvent) {
    e.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      const result = await previewAddCages(form);
      setPreview(result);
      setStep("preview");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to generate preview");
    } finally {
      setSubmitting(false);
    }
  }

  async function handleConfirm() {
    setSubmitting(true);
    setError(null);
    try {
      const result = await createCages(form);
      setSuccessStats(result.stats);
      setStep("success");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to create cages");
    } finally {
      setSubmitting(false);
    }
  }

  if (loading) {
    return <div className="text-center py-8 text-text-secondary">Loading...</div>;
  }

  if (step === "success" && successStats) {
    return (
      <div className="max-w-xl mx-auto text-center">
        <div className="bg-green-900/30 border border-green-700 rounded p-8 mb-6">
          <h2 className="text-2xl font-bold text-green-400 mb-4">
            Cages Created Successfully
          </h2>
          <div className="space-y-2 text-sm">
            <p>Total created: {String(successStats.total_created)}</p>
            <p>Male: {String(successStats.male_created)} | Female: {String(successStats.female_created)}</p>
            <p>
              Range: {String(successStats.first_cage)} to {String(successStats.last_cage)}
            </p>
          </div>
        </div>
        <button
          onClick={() => navigate("/cages")}
          className="px-6 py-2 bg-accent-blue text-white rounded text-sm"
        >
          Back to Cage Interface
        </button>
      </div>
    );
  }

  if (step === "preview" && preview) {
    return (
      <div className="max-w-2xl mx-auto">
        <h1 className="text-2xl font-bold mb-4">Preview — Cages to Create</h1>

        {error && (
          <div className="mb-4 p-3 bg-red-900/50 border border-red-700 rounded text-red-200 text-sm">
            {error}
          </div>
        )}

        <div className="bg-bg-secondary border border-border rounded p-4 mb-4">
          <div className="grid grid-cols-3 gap-4 text-sm">
            <div>Total: {String(preview.summary.total_cages)}</div>
            <div>Male: {String(preview.summary.male_cages)}</div>
            <div>Female: {String(preview.summary.female_cages)}</div>
          </div>
        </div>

        <table className="w-full text-sm border-collapse mb-4">
          <thead>
            <tr className="border-b border-border">
              <th className="text-left px-3 py-2 text-text-secondary">Cage ID</th>
              <th className="text-left px-3 py-2 text-text-secondary">Sex</th>
              <th className="text-left px-3 py-2 text-text-secondary">Mice</th>
              <th className="text-left px-3 py-2 text-text-secondary">Strain</th>
            </tr>
          </thead>
          <tbody>
            {preview.cages.map((cage) => (
              <tr key={cage.cage_id} className="border-b border-border/50">
                <td className="px-3 py-2">{cage.cage_id}</td>
                <td className="px-3 py-2">{cage.sex === "m" ? "Male" : "Female"}</td>
                <td className="px-3 py-2">{cage.n_mice}</td>
                <td className="px-3 py-2">{cage.strain}</td>
              </tr>
            ))}
          </tbody>
        </table>

        <div className="flex gap-3">
          <button
            onClick={handleConfirm}
            disabled={submitting}
            className="px-6 py-2 bg-green-700 text-white rounded text-sm hover:opacity-90 disabled:opacity-50"
          >
            {submitting ? "Creating..." : "Confirm & Create"}
          </button>
          <button
            onClick={() => setStep("form")}
            className="px-6 py-2 bg-bg-tertiary text-text-primary rounded text-sm hover:bg-bg-hover"
          >
            Back to Form
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-xl mx-auto">
      <h1 className="text-2xl font-bold mb-6">Add New Cages</h1>
      <p className="text-text-secondary text-sm mb-4">
        Next cage number: c{String(nextCageNum).padStart(7, "0")}
      </p>

      {error && (
        <div className="mb-4 p-3 bg-red-900/50 border border-red-700 rounded text-red-200 text-sm">
          {error}
        </div>
      )}

      <form onSubmit={handlePreview} className="space-y-4">
        <div>
          <label className="block text-sm text-text-secondary mb-1">Mice per Cage</label>
          <input type="number" name="mice_per_cage" value={form.mice_per_cage} onChange={handleChange} min="1" required className="w-full bg-bg-tertiary border border-border rounded px-3 py-2 text-sm focus:border-accent-blue outline-none" />
        </div>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm text-text-secondary mb-1">Male Cages</label>
            <input type="number" name="num_male_cages" value={form.num_male_cages} onChange={handleChange} min="0" className="w-full bg-bg-tertiary border border-border rounded px-3 py-2 text-sm focus:border-accent-blue outline-none" />
          </div>
          <div>
            <label className="block text-sm text-text-secondary mb-1">Female Cages</label>
            <input type="number" name="num_female_cages" value={form.num_female_cages} onChange={handleChange} min="0" className="w-full bg-bg-tertiary border border-border rounded px-3 py-2 text-sm focus:border-accent-blue outline-none" />
          </div>
        </div>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm text-text-secondary mb-1">Strain</label>
            <select name="strain" value={form.strain} onChange={handleChange} required className="w-full bg-bg-tertiary border border-border rounded px-3 py-2 text-sm focus:border-accent-blue outline-none">
              {strainOptions.map((o) => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm text-text-secondary mb-1">Supplier</label>
            <select name="supplier" value={form.supplier} onChange={handleChange} required className="w-full bg-bg-tertiary border border-border rounded px-3 py-2 text-sm focus:border-accent-blue outline-none">
              {supplierOptions.map((o) => (
                <option key={o} value={o}>{o}</option>
              ))}
            </select>
          </div>
        </div>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm text-text-secondary mb-1">Date of Birth</label>
            <input type="date" name="dob" value={form.dob} onChange={handleChange} required className="w-full bg-bg-tertiary border border-border rounded px-3 py-2 text-sm focus:border-accent-blue outline-none" />
          </div>
          <div>
            <label className="block text-sm text-text-secondary mb-1">Date Received</label>
            <input type="date" name="date_received" value={form.date_received} onChange={handleChange} required className="w-full bg-bg-tertiary border border-border rounded px-3 py-2 text-sm focus:border-accent-blue outline-none" />
          </div>
        </div>
        <div className="flex gap-3 pt-2">
          <button type="submit" disabled={submitting} className="px-6 py-2 bg-accent-blue text-white rounded text-sm hover:opacity-90 disabled:opacity-50">
            {submitting ? "Generating..." : "Preview"}
          </button>
          <button type="button" onClick={() => navigate("/cages")} className="px-6 py-2 bg-bg-tertiary text-text-primary rounded text-sm hover:bg-bg-hover">
            Cancel
          </button>
        </div>
      </form>
    </div>
  );
}

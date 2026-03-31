import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { fetchCages } from "../api/cages";
import type { CageStats } from "../types";

export default function CageInterface() {
  const [stats, setStats] = useState<CageStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchCages()
      .then((data) => setStats(data.cage_stats))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="text-center py-8 text-text-secondary">
        Loading cage interface...
      </div>
    );
  }

  if (error) {
    return <div className="text-center py-8 text-accent-red">{error}</div>;
  }

  return (
    <div className="max-w-3xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">Cage Interface</h1>
        <Link
          to="/cages/add"
          className="px-4 py-2 bg-accent-blue text-white rounded text-sm hover:opacity-90"
        >
          Add New Cages
        </Link>
      </div>

      {stats && (
        <div className="grid grid-cols-3 gap-4 mb-8">
          <div className="bg-bg-secondary border border-border rounded p-6 text-center">
            <div className="text-text-secondary text-sm mb-1">Total Cages</div>
            <div className="text-3xl font-bold">{stats.total}</div>
          </div>
          <div className="bg-bg-secondary border border-border rounded p-6 text-center">
            <div className="text-text-secondary text-sm mb-1">Male Cages</div>
            <div className="text-3xl font-bold text-blue-400">{stats.male}</div>
          </div>
          <div className="bg-bg-secondary border border-border rounded p-6 text-center">
            <div className="text-text-secondary text-sm mb-1">Female Cages</div>
            <div className="text-3xl font-bold text-pink-400">
              {stats.female}
            </div>
          </div>
        </div>
      )}

      <div className="bg-bg-secondary border border-border rounded p-6">
        <h2 className="text-lg font-semibold mb-2">Quick Actions</h2>
        <div className="flex gap-3">
          <Link
            to="/cages/add"
            className="px-4 py-2 bg-bg-tertiary border border-border rounded text-sm hover:bg-bg-hover"
          >
            Add Cages
          </Link>
        </div>
      </div>
    </div>
  );
}

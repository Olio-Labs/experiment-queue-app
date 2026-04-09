import { useEffect, useState, useCallback } from "react";
import { fetchBoxRoomData, fetchBoxVideo, fetchFlaggedIssues } from "../api/boxRoom";
import type { BankLayout, BoxRoomData, BoxVideoResponse } from "../types";

/** Single box tile in the grid */
function BoxTile({
  boxNumber,
  data,
  hasIssue,
  onClick,
}: {
  boxNumber: number;
  data: BoxRoomData;
  hasIssue: boolean;
  onClick: () => void;
}) {
  const boxData = data.boxes_by_number[String(boxNumber)];
  const cageCount = boxData?.cages?.length ?? 0;
  const isEmpty = cageCount === 0;
  const overlays = boxData?.overlays ?? [];

  let bgColor = "bg-bg-tertiary";
  if (hasIssue) bgColor = "bg-red-900/60";
  else if (isEmpty) bgColor = "bg-yellow-900/30";

  return (
    <button
      onClick={onClick}
      className={`w-20 h-20 ${bgColor} border border-border rounded flex flex-col items-center justify-center text-xs hover:border-accent-blue transition-colors relative overflow-hidden`}
      title={`Box ${boxNumber} — ${cageCount} cage(s)`}
    >
      {/* Syringe color overlays */}
      {overlays.length > 0 && (
        <div className="absolute inset-0 flex">
          {overlays.map((o, i) => (
            <div
              key={i}
              className="flex-1 opacity-30"
              style={{ backgroundColor: o.color }}
            />
          ))}
        </div>
      )}
      <span className="font-bold relative z-10">{boxNumber}</span>
      <span className="text-text-muted relative z-10">
        {cageCount > 0 ? `${cageCount}c` : "empty"}
      </span>
    </button>
  );
}

/** Render a bank layout (horizontal or vertical) */
function BankGrid({
  layout,
  data,
  issueBoxes,
  onBoxClick,
}: {
  layout: BankLayout;
  data: BoxRoomData;
  issueBoxes: Set<number>;
  onBoxClick: (n: number) => void;
}) {
  if ("top" in layout && "bottom" in layout) {
    return (
      <div className="flex flex-col gap-1">
        <div className="flex gap-1">
          {layout.top!.map((n) => (
            <BoxTile
              key={n}
              boxNumber={n}
              data={data}
              hasIssue={issueBoxes.has(n)}
              onClick={() => onBoxClick(n)}
            />
          ))}
        </div>
        <div className="flex gap-1">
          {layout.bottom!.map((n) => (
            <BoxTile
              key={n}
              boxNumber={n}
              data={data}
              hasIssue={issueBoxes.has(n)}
              onClick={() => onBoxClick(n)}
            />
          ))}
        </div>
      </div>
    );
  }
  // left/right columns
  return (
    <div className="flex gap-1">
      <div className="flex flex-col gap-1">
        {(layout.left || []).map((n) => (
          <BoxTile
            key={n}
            boxNumber={n}
            data={data}
            hasIssue={issueBoxes.has(n)}
            onClick={() => onBoxClick(n)}
          />
        ))}
      </div>
      <div className="flex flex-col gap-1">
        {(layout.right || []).map((n) => (
          <BoxTile
            key={n}
            boxNumber={n}
            data={data}
            hasIssue={issueBoxes.has(n)}
            onClick={() => onBoxClick(n)}
          />
        ))}
      </div>
    </div>
  );
}

/** Box overlay modal with video player and history */
function BoxOverlayModal({
  boxNumber,
  data,
  onClose,
}: {
  boxNumber: number;
  data: BoxRoomData;
  onClose: () => void;
}) {
  const boxData = data.boxes_by_number[String(boxNumber)];
  const cages = boxData?.cages ?? [];
  const [video, setVideo] = useState<BoxVideoResponse | null>(null);
  const [videoLoading, setVideoLoading] = useState(false);
  const [selectedTimestamp, setSelectedTimestamp] = useState<string | null>(null);
  const [history, setHistory] = useState<unknown[] | null>(null);

  const firstCage = cages[0];

  // Load video for first cage
  useEffect(() => {
    if (!firstCage) return;
    setVideoLoading(true);
    fetchBoxVideo(
      firstCage.cage_id,
      boxData?.box_id ?? `b${String(boxNumber).padStart(7, "0")}`,
      data.selected_date,
      selectedTimestamp ?? undefined,
      data.experiment_id_filter || undefined,
    )
      .then(setVideo)
      .catch(() => setVideo(null))
      .finally(() => setVideoLoading(false));
  }, [firstCage, selectedTimestamp, boxNumber, data.selected_date, data.experiment_id_filter, boxData?.box_id]);

  // Load history
  useEffect(() => {
    fetchFlaggedIssues(
      boxNumber,
      data.selected_date,
      data.experiment_id_filter || undefined,
    )
      .then(setHistory)
      .catch(() => setHistory(null));
  }, [boxNumber, data.selected_date, data.experiment_id_filter]);

  return (
    <div className="fixed inset-0 bg-black/70 z-50 flex items-center justify-center p-4">
      <div className="bg-bg-secondary border border-border rounded-lg max-w-4xl w-full max-h-[90vh] overflow-y-auto">
        <div className="flex items-center justify-between p-4 border-b border-border">
          <h2 className="text-lg font-bold">Box {boxNumber}</h2>
          <button
            onClick={onClose}
            className="w-8 h-8 flex items-center justify-center rounded hover:bg-bg-hover text-text-secondary"
          >
            X
          </button>
        </div>

        <div className="p-4 grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Video section */}
          <div>
            <h3 className="text-sm font-semibold text-text-secondary mb-2">
              Video
            </h3>
            {videoLoading ? (
              <div className="text-text-muted text-sm">Loading video...</div>
            ) : video?.success && video.url ? (
              <div>
                <video
                  src={video.url}
                  controls
                  preload="metadata"
                  className="w-full rounded"
                />
                {video.available_timestamps &&
                  video.available_timestamps.length > 0 && (
                    <select
                      value={selectedTimestamp || ""}
                      onChange={(e) => setSelectedTimestamp(e.target.value || null)}
                      className="mt-2 w-full bg-bg-tertiary border border-border rounded px-2 py-1 text-sm"
                    >
                      <option value="">Latest</option>
                      {video.available_timestamps.map((ts) => (
                        <option key={ts} value={ts}>
                          {ts}
                        </option>
                      ))}
                    </select>
                  )}
                {video.co2_plot && (
                  <img
                    src={`data:image/png;base64,${video.co2_plot}`}
                    alt="CO2 plot"
                    className="mt-2 w-full rounded"
                  />
                )}
              </div>
            ) : (
              <div className="text-text-muted text-sm">
                {video?.error || "No video available"}
              </div>
            )}
          </div>

          {/* Cage info and history */}
          <div>
            <h3 className="text-sm font-semibold text-text-secondary mb-2">
              Cages ({cages.length})
            </h3>
            <div className="space-y-1 mb-4">
              {cages.map((cage) => (
                <div
                  key={cage.cage_id}
                  className="bg-bg-tertiary rounded px-3 py-2 text-sm flex justify-between"
                >
                  <span>{cage.cage_id}</span>
                  <span className="text-text-muted">
                    {cage.sex} | {cage.n_mice} mice
                  </span>
                </div>
              ))}
              {cages.length === 0 && (
                <div className="text-text-muted text-sm">No cages assigned</div>
              )}
            </div>

            <h3 className="text-sm font-semibold text-text-secondary mb-2">
              Flagged Issues History
            </h3>
            {history && history.length > 0 ? (
              <div className="space-y-1 max-h-60 overflow-y-auto">
                {history.map((item, i) => (
                  <div
                    key={i}
                    className="bg-bg-tertiary rounded px-3 py-2 text-sm"
                  >
                    {JSON.stringify(item)}
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-text-muted text-sm">No issues</div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default function BoxRoom() {
  const [data, setData] = useState<BoxRoomData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedBox, setSelectedBox] = useState<number | null>(null);
  const [dateInput, setDateInput] = useState("");
  const [expIdInput, setExpIdInput] = useState("");

  const loadData = useCallback(
    (startDate?: string, experimentId?: string) => {
      setLoading(true);
      setError(null);
      fetchBoxRoomData(startDate, experimentId)
        .then((d) => {
          setData(d);
          setDateInput(d.selected_date);
          setExpIdInput(d.experiment_id_filter || d.selected_experiment_id || "");
        })
        .catch((e) => setError(e.message))
        .finally(() => setLoading(false));
    },
    [],
  );

  useEffect(() => {
    loadData();
  }, [loadData]);

  function handleDateChange() {
    if (dateInput) loadData(dateInput);
  }

  function handleExpIdChange() {
    if (expIdInput) loadData(undefined, expIdInput);
  }

  if (loading && !data) {
    return (
      <div className="text-center py-8 text-text-secondary">
        Loading box room...
      </div>
    );
  }

  if (error && !data) {
    return <div className="text-center py-8 text-accent-red">{error}</div>;
  }

  if (!data) return null;

  const issueBoxes = new Set(data.boxes_with_issues);

  return (
    <div>
      {/* Date/experiment selector */}
      <div className="flex items-center gap-4 mb-4">
        <h1 className="text-2xl font-bold">Box Room</h1>
        <div className="flex items-center gap-2">
          <label className="text-sm text-text-secondary">Date:</label>
          <input
            type="date"
            value={dateInput}
            onChange={(e) => setDateInput(e.target.value)}
            className="bg-bg-tertiary border border-border rounded px-2 py-1 text-sm"
          />
          <button
            onClick={handleDateChange}
            className="px-2 py-1 bg-accent-blue text-white rounded text-xs"
          >
            Go
          </button>
        </div>
        <div className="flex items-center gap-2">
          <label className="text-sm text-text-secondary">Experiment:</label>
          <input
            type="text"
            value={expIdInput}
            onChange={(e) => setExpIdInput(e.target.value)}
            placeholder="e.g. e0000123"
            className="bg-bg-tertiary border border-border rounded px-2 py-1 text-sm w-32"
          />
          <button
            onClick={handleExpIdChange}
            className="px-2 py-1 bg-accent-blue text-white rounded text-xs"
          >
            Go
          </button>
        </div>
        {data.today_experiment_id && (
          <span className="text-text-muted text-xs">
            Today: {data.today_experiment_id}
          </span>
        )}
      </div>

      {data.overlay_errors.length > 0 && (
        <div className="mb-4 p-2 bg-yellow-900/30 border border-yellow-700 rounded text-sm text-yellow-200">
          {data.overlay_errors.join("; ")}
        </div>
      )}

      {/* Box grid layout */}
      <div className="flex flex-wrap gap-6">
        {Object.entries(data.banks).map(([bankName, layout]) => (
          <div key={bankName}>
            <div className="text-xs text-text-muted mb-1">
              {bankName.replace(/bank_/g, "").replace(/_/g, "–")}
            </div>
            <BankGrid
              layout={layout}
              data={data}
              issueBoxes={issueBoxes}
              onBoxClick={setSelectedBox}
            />
          </div>
        ))}
      </div>

      {/* Flagged issues sidebar */}
      {data.boxes_with_issues.length > 0 && (
        <div className="mt-6 bg-bg-secondary border border-border rounded p-4">
          <h2 className="text-sm font-semibold text-red-400 mb-2">
            Boxes with Flagged Issues ({data.boxes_with_issues.length})
          </h2>
          <div className="flex flex-wrap gap-2">
            {data.boxes_with_issues.map((bn) => (
              <button
                key={bn}
                onClick={() => setSelectedBox(bn)}
                className="px-3 py-1 bg-red-900/40 border border-red-700 rounded text-sm hover:bg-red-900/60"
              >
                Box {bn}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Modal overlay */}
      {selectedBox !== null && (
        <BoxOverlayModal
          boxNumber={selectedBox}
          data={data}
          onClose={() => setSelectedBox(null)}
        />
      )}
    </div>
  );
}

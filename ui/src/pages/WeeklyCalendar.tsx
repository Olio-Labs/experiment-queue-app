import { useEffect, useState } from "react";
import { fetchWeeklyCalendar } from "../api/scheduling";

export default function WeeklyCalendar() {
  const [calendarUrl, setCalendarUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchWeeklyCalendar()
      .then((data) => setCalendarUrl(data.calendar_url))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="text-center py-8 text-text-secondary">
        Loading calendar...
      </div>
    );
  }

  if (error) {
    return <div className="text-center py-8 text-accent-red">{error}</div>;
  }

  return (
    <div>
      <h1 className="text-2xl font-bold mb-4">Weekly Calendar</h1>
      {calendarUrl ? (
        <div className="w-full" style={{ height: "calc(100vh - 140px)" }}>
          <iframe
            src={calendarUrl}
            style={{ border: 0, width: "100%", height: "100%" }}
            title="Experiment Calendar"
          />
        </div>
      ) : (
        <p className="text-text-secondary">
          Calendar URL not configured. Set GOOGLE_TECH_CALENDAR_ID and
          GOOGLE_EXPERIMENT_CALENDAR_ID environment variables.
        </p>
      )}
    </div>
  );
}

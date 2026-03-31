import { get, post } from "./client";

export function fetchPlanPreview(startDate?: string): Promise<Record<string, unknown>> {
  const qs = startDate ? `?start_date=${startDate}` : "";
  return get(`/scheduling/preview${qs}`);
}

export function pushPlanToAirtable(data?: Record<string, unknown>): Promise<{ success: boolean }> {
  return post("/scheduling/push", data);
}

export function clearScheduledPlan(): Promise<{ success: boolean }> {
  return post("/scheduling/clear");
}

export function recalculateTimes(): Promise<{ success: boolean }> {
  return post("/scheduling/recalculate");
}

export function fetchWeeklyCalendar(): Promise<{ calendar_url: string }> {
  return get("/calendar/weekly");
}

import { get, post } from "./client";
import type {
  ScheduledExperimentResult,
  SchedulingPreviewResponse,
} from "../types";

export function fetchPlanPreview(
  startDate?: string,
): Promise<SchedulingPreviewResponse> {
  const qs = startDate ? `?start_date=${startDate}` : "";
  return get(`/scheduling/preview${qs}`);
}

export function pushPlanToAirtable(
  experiments: ScheduledExperimentResult[],
): Promise<{
  success: boolean;
  message: string;
  updated_count: number;
  errors: string[];
}> {
  return post("/scheduling/push", { scheduled_experiments: experiments });
}

export function clearScheduledPlan(): Promise<{
  success: boolean;
  message: string;
}> {
  return post("/scheduling/clear");
}

export function recalculateTimes(): Promise<{
  success: boolean;
  message: string;
  updated_count: number;
  errors: string[];
}> {
  return post("/scheduling/recalculate");
}

export function fetchWeeklyCalendar(): Promise<{ calendar_url: string }> {
  return get("/calendar/weekly");
}

export function pushToCalendar(
  experiments: ScheduledExperimentResult[],
): Promise<{
  success: boolean;
  message: string;
  events_created: number;
}> {
  return post("/calendar/push", { experiments });
}

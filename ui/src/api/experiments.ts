import { get, post, put, del } from "./client";
import type { ExperimentRecord, FormOptions } from "../types";

interface ExperimentsResponse {
  experiments: ExperimentRecord[];
  headers: string[];
}

export function fetchExperiments(): Promise<ExperimentsResponse> {
  return get<ExperimentsResponse>("/experiments");
}

export function fetchExperiment(recordId: string): Promise<{ experiment: Record<string, unknown> }> {
  return get(`/experiments/${recordId}`);
}

export function fetchFormOptions(): Promise<FormOptions> {
  return get<FormOptions>("/experiments/form-options");
}

export function createExperiment(fields: Record<string, unknown>): Promise<{ success: boolean }> {
  return post("/experiments", { fields });
}

export function updateExperiment(
  recordId: string,
  fields: Record<string, unknown>,
): Promise<{ success: boolean }> {
  return put(`/experiments/${recordId}`, { fields });
}

export function deleteExperiment(recordId: string): Promise<{ success: boolean }> {
  return del(`/experiments/${recordId}`);
}

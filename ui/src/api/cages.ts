import { get, post } from "./client";
import type { AddCagesFormData, CagePreview, CageStats } from "../types";

interface CagesResponse {
  cage_stats: CageStats;
  cages: Record<string, unknown>[];
}

interface CageFormOptions {
  next_cage_num: number;
  supplier_options: string[];
  strain_options: string[];
}

interface CagePreviewResponse {
  cages: CagePreview[];
  summary: {
    total_cages: number;
    male_cages: number;
    female_cages: number;
    total_mice: number;
    cage_range: string;
  };
}

interface CreateCagesResponse {
  success: boolean;
  stats: {
    total_created: number;
    male_created: number;
    female_created: number;
    first_cage: string;
    last_cage: string;
  };
}

export function fetchCages(): Promise<CagesResponse> {
  return get<CagesResponse>("/cages");
}

export function fetchCageFormOptions(): Promise<CageFormOptions> {
  return get<CageFormOptions>("/cages/form-options");
}

export function previewAddCages(data: AddCagesFormData): Promise<CagePreviewResponse> {
  return post<CagePreviewResponse>("/cages/preview", data);
}

export function createCages(data: AddCagesFormData): Promise<CreateCagesResponse> {
  return post<CreateCagesResponse>("/cages", data);
}

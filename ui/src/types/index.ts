/** Experiment record from the API */
export interface ExperimentRecord {
  id: string;
  fields: {
    priority: number;
    assignment: string;
    num_days: number;
    config_file: string | null;
    is_chronic: boolean;
    notes: string;
    earliest_start_date: string | null;
    actual_start_date: string | null;
    actual_end_date: string | null;
    unique_manipulation_ids: string[];
    manipulations: string;
    selected_tasks: string[];
    experiment_time_minutes: number | null;
  };
  manipulation_details: ManipulationDetail[];
  manipulations_display: string;
}

export interface ManipulationDetail {
  id: string;
  drugs: string[];
  safety: string[];
  dose_mg_kg: number[];
}

/** Cage stats from the API */
export interface CageStats {
  total: number;
  male: number;
  female: number;
}

/** Cage record */
export interface CageRecord {
  id: string;
  fields: Record<string, unknown>;
}

/** Box room data from the API */
export interface BoxRoomData {
  boxes_by_number: Record<string, BoxData>;
  boxes_with_issues: number[];
  overlay_errors: string[];
  cages_data: CageRecord[];
  cages_with_issues: string[];
  banks: Record<string, BankLayout>;
  selected_date: string;
  today_pst_date: string;
  today_experiment_id: string;
  today_experiment_error: string | null;
  selected_experiment_id: string;
  selected_experiment_error: string | null;
  experiment_id_filter: string;
}

export interface BoxData {
  box_number: number;
  box_id: string;
  cages: BoxCage[];
  has_issues: boolean;
  flagged_issues: string[];
  overlays: BoxOverlay[];
}

export interface BoxCage {
  cage_id: string;
  record_id: string;
  sex: string;
  n_mice: number;
  experiment_id?: string;
}

export interface BoxOverlay {
  color: string;
  label: string;
}

export interface BankLayout {
  top?: number[];
  bottom?: number[];
  left?: number[];
  right?: number[];
}

/** Video response */
export interface BoxVideoResponse {
  success: boolean;
  url?: string;
  available_timestamps?: string[];
  cage_id?: string;
  experiment_id?: string;
  co2_plot?: string;
  error?: string;
}

/** Add cages form data */
export interface AddCagesFormData {
  mice_per_cage: number;
  num_male_cages: number;
  num_female_cages: number;
  strain: string;
  supplier: string;
  dob: string;
  date_received: string;
}

/** Cage preview */
export interface CagePreview {
  cage_id: string;
  n_mice: number;
  sex: string;
  strain: string;
  bought_from: string;
  dob: string;
  received: string;
}

/** Form options for experiments */
export interface FormOptions {
  options: Record<string, string[]>;
}

import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import ExperimentQueue from "../../pages/ExperimentQueue";
import { fetchExperiments } from "../../api/experiments";
vi.mock("../../api/experiments", () => ({
  fetchExperiments: vi.fn(),
  deleteExperiment: vi.fn(),
}));

vi.mock("../../api/scheduling", () => ({
  recalculateTimes: vi.fn(),
}));

const mockFetchExperiments = vi.mocked(fetchExperiments);

function renderQueue() {
  return render(
    <MemoryRouter>
      <ExperimentQueue />
    </MemoryRouter>,
  );
}

describe("ExperimentQueue page", () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it("shows loading state initially", () => {
    mockFetchExperiments.mockReturnValue(new Promise(() => {}));
    renderQueue();
    expect(screen.getByText("Loading experiments...")).toBeInTheDocument();
  });

  it("renders table headers after data loads", async () => {
    mockFetchExperiments.mockResolvedValue({
      experiments: [
        {
          id: "rec1",
          fields: {
            priority: 1,
            assignment: "direct_mapping",
            num_days: 1,
            config_file: null,
            is_chronic: false,
            notes: "",
            earliest_start_date: null,
            actual_start_date: null,
            actual_end_date: null,
            unique_manipulation_ids: [],
            manipulations: "",
            selected_tasks: [],
            experiment_time_minutes: null,
          },
          manipulation_details: [],
          manipulations_display: "",
        },
      ],
      headers: ["priority", "assignment"],
    });
    renderQueue();
    await waitFor(() => {
      expect(screen.getByText("Experiment Queue")).toBeInTheDocument();
    });
    expect(screen.getByText("priority")).toBeInTheDocument();
    expect(screen.getByText("assignment")).toBeInTheDocument();
  });

  it("shows error message on fetch failure", async () => {
    mockFetchExperiments.mockRejectedValue(new Error("Network error"));
    renderQueue();
    await waitFor(() => {
      expect(screen.getByText("Network error")).toBeInTheDocument();
    });
  });

  it("shows empty state when no experiments", async () => {
    mockFetchExperiments.mockResolvedValue({
      experiments: [],
      headers: ["experiment_id"],
    });
    renderQueue();
    await waitFor(() => {
      expect(
        screen.getByText("No experiments in the queue."),
      ).toBeInTheDocument();
    });
  });
});

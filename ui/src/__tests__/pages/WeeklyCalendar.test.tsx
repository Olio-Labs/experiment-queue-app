import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import WeeklyCalendar from "../../pages/WeeklyCalendar";
import { fetchWeeklyCalendar } from "../../api/scheduling";

vi.mock("../../api/scheduling", () => ({
  fetchWeeklyCalendar: vi.fn(),
}));

const mockFetchCalendar = vi.mocked(fetchWeeklyCalendar);

function renderCalendar() {
  return render(
    <MemoryRouter>
      <WeeklyCalendar />
    </MemoryRouter>,
  );
}

describe("WeeklyCalendar page", () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it("shows loading state initially", () => {
    mockFetchCalendar.mockReturnValue(new Promise(() => {}));
    renderCalendar();
    expect(screen.getByText("Loading calendar...")).toBeInTheDocument();
  });

  it("renders iframe when calendar URL is provided", async () => {
    mockFetchCalendar.mockResolvedValue({
      calendar_url: "https://calendar.google.com/embed",
    });
    renderCalendar();
    await waitFor(() => {
      const iframe = screen.getByTitle("Experiment Calendar");
      expect(iframe).toBeInTheDocument();
      expect(iframe).toHaveAttribute(
        "src",
        "https://calendar.google.com/embed",
      );
    });
  });

  it("shows fallback when no calendar URL", async () => {
    mockFetchCalendar.mockResolvedValue({ calendar_url: "" });
    renderCalendar();
    await waitFor(() => {
      expect(
        screen.getByText(/Calendar URL not configured/),
      ).toBeInTheDocument();
    });
  });
});

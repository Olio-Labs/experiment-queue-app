import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import CageInterface from "../../pages/CageInterface";
import { fetchCages } from "../../api/cages";

vi.mock("../../api/cages", () => ({
  fetchCages: vi.fn(),
}));

const mockFetchCages = vi.mocked(fetchCages);

function renderCageInterface() {
  return render(
    <MemoryRouter>
      <CageInterface />
    </MemoryRouter>,
  );
}

describe("CageInterface page", () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it("shows loading state initially", () => {
    mockFetchCages.mockReturnValue(new Promise(() => {}));
    renderCageInterface();
    expect(
      screen.getByText("Loading cage interface..."),
    ).toBeInTheDocument();
  });

  it("renders stats after data loads", async () => {
    mockFetchCages.mockResolvedValue({
      cage_stats: { total: 100, male: 60, female: 40 },
      cages: [],
    });
    renderCageInterface();
    await waitFor(() => {
      expect(screen.getByText("100")).toBeInTheDocument();
    });
    expect(screen.getByText("60")).toBeInTheDocument();
    expect(screen.getByText("40")).toBeInTheDocument();
  });

  it("shows error on fetch failure", async () => {
    mockFetchCages.mockRejectedValue(new Error("Server error"));
    renderCageInterface();
    await waitFor(() => {
      expect(screen.getByText("Server error")).toBeInTheDocument();
    });
  });
});

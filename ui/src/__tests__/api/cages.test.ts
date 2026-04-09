import {
  fetchCages,
  fetchCageFormOptions,
  previewAddCages,
  createCages,
} from "../../api/cages";

const mockFetch = vi.fn();
globalThis.fetch = mockFetch;

beforeEach(() => {
  mockFetch.mockReset();
  mockFetch.mockResolvedValue({
    ok: true,
    json: () => Promise.resolve({}),
    text: () => Promise.resolve(""),
  });
});

describe("cages API", () => {
  it("fetchCages calls GET /api/cages", async () => {
    await fetchCages();
    expect(mockFetch).toHaveBeenCalledWith(
      "/api/cages",
      expect.anything(),
    );
  });

  it("fetchCageFormOptions calls GET /api/cages/form-options", async () => {
    await fetchCageFormOptions();
    expect(mockFetch).toHaveBeenCalledWith(
      "/api/cages/form-options",
      expect.anything(),
    );
  });

  it("previewAddCages calls POST /api/cages/preview", async () => {
    const data = {
      mice_per_cage: 4,
      num_male_cages: 5,
      num_female_cages: 0,
      strain: "C57BL/6",
      supplier: "JAX",
      dob: "2026-01-01",
      date_received: "2026-03-01",
    };
    await previewAddCages(data);
    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe("/api/cages/preview");
    expect(opts.method).toBe("POST");
  });

  it("createCages calls POST /api/cages", async () => {
    const data = {
      mice_per_cage: 4,
      num_male_cages: 0,
      num_female_cages: 5,
      strain: "C57BL/6",
      supplier: "JAX",
      dob: "2026-01-01",
      date_received: "2026-03-01",
    };
    await createCages(data);
    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe("/api/cages");
    expect(opts.method).toBe("POST");
  });
});

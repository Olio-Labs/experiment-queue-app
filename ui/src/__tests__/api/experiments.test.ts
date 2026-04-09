import {
  fetchExperiments,
  fetchExperiment,
  fetchFormOptions,
  createExperiment,
  updateExperiment,
  deleteExperiment,
} from "../../api/experiments";

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

describe("experiments API", () => {
  it("fetchExperiments calls GET /api/experiments", async () => {
    await fetchExperiments();
    expect(mockFetch).toHaveBeenCalledWith(
      "/api/experiments",
      expect.anything(),
    );
  });

  it("fetchExperiment calls GET /api/experiments/:id", async () => {
    await fetchExperiment("rec123");
    expect(mockFetch).toHaveBeenCalledWith(
      "/api/experiments/rec123",
      expect.anything(),
    );
  });

  it("fetchFormOptions calls GET /api/experiments/form-options", async () => {
    await fetchFormOptions();
    expect(mockFetch).toHaveBeenCalledWith(
      "/api/experiments/form-options",
      expect.anything(),
    );
  });

  it("createExperiment calls POST /api/experiments", async () => {
    await createExperiment({ name: "test" });
    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe("/api/experiments");
    expect(opts.method).toBe("POST");
    expect(JSON.parse(opts.body)).toEqual({ fields: { name: "test" } });
  });

  it("updateExperiment calls PUT /api/experiments/:id", async () => {
    await updateExperiment("rec1", { name: "updated" });
    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe("/api/experiments/rec1");
    expect(opts.method).toBe("PUT");
    expect(JSON.parse(opts.body)).toEqual({ fields: { name: "updated" } });
  });

  it("deleteExperiment calls DELETE /api/experiments/:id", async () => {
    await deleteExperiment("rec1");
    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe("/api/experiments/rec1");
    expect(opts.method).toBe("DELETE");
  });
});

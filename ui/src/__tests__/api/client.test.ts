import { get, post, put, del } from "../../api/client";

const mockFetch = vi.fn();
globalThis.fetch = mockFetch;

beforeEach(() => {
  mockFetch.mockReset();
  mockFetch.mockResolvedValue({
    ok: true,
    json: () => Promise.resolve({ data: "test" }),
    text: () => Promise.resolve(""),
  });
});

describe("API client", () => {
  it("get() sends GET to /api + path", async () => {
    await get("/experiments");
    expect(mockFetch).toHaveBeenCalledWith(
      "/api/experiments",
      expect.objectContaining({
        headers: expect.objectContaining({
          "Content-Type": "application/json",
        }),
      }),
    );
  });

  it("post() sends POST with JSON body", async () => {
    await post("/experiments", { fields: { name: "test" } });
    const [, opts] = mockFetch.mock.calls[0];
    expect(opts.method).toBe("POST");
    expect(JSON.parse(opts.body)).toEqual({ fields: { name: "test" } });
  });

  it("put() sends PUT with JSON body", async () => {
    await put("/experiments/rec1", { fields: { name: "updated" } });
    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe("/api/experiments/rec1");
    expect(opts.method).toBe("PUT");
    expect(JSON.parse(opts.body)).toEqual({
      fields: { name: "updated" },
    });
  });

  it("del() sends DELETE request", async () => {
    await del("/experiments/rec1");
    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe("/api/experiments/rec1");
    expect(opts.method).toBe("DELETE");
  });

  it("returns parsed JSON on success", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ id: 1 }),
      text: () => Promise.resolve(""),
    });
    const result = await get("/test");
    expect(result).toEqual({ id: 1 });
  });

  it("throws on non-OK response with status and body", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 404,
      text: () => Promise.resolve("Not found"),
    });
    await expect(get("/missing")).rejects.toThrow("API error 404: Not found");
  });
});

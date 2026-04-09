import {
  fetchPlanPreview,
  pushPlanToAirtable,
  clearScheduledPlan,
  recalculateTimes,
  fetchWeeklyCalendar,
  pushToCalendar,
} from "../../api/scheduling";

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

describe("scheduling API", () => {
  it("fetchPlanPreview with no date calls GET /api/scheduling/preview", async () => {
    await fetchPlanPreview();
    expect(mockFetch).toHaveBeenCalledWith(
      "/api/scheduling/preview",
      expect.anything(),
    );
  });

  it("fetchPlanPreview with date appends query param", async () => {
    await fetchPlanPreview("2026-04-08");
    expect(mockFetch).toHaveBeenCalledWith(
      "/api/scheduling/preview?start_date=2026-04-08",
      expect.anything(),
    );
  });

  it("pushPlanToAirtable calls POST /api/scheduling/push", async () => {
    const experiments = [{ record_id: "rec1" }];
    await pushPlanToAirtable(experiments as never[]);
    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe("/api/scheduling/push");
    expect(opts.method).toBe("POST");
    expect(JSON.parse(opts.body)).toEqual({
      scheduled_experiments: experiments,
    });
  });

  it("clearScheduledPlan calls POST /api/scheduling/clear", async () => {
    await clearScheduledPlan();
    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe("/api/scheduling/clear");
    expect(opts.method).toBe("POST");
  });

  it("recalculateTimes calls POST /api/scheduling/recalculate", async () => {
    await recalculateTimes();
    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe("/api/scheduling/recalculate");
    expect(opts.method).toBe("POST");
  });

  it("fetchWeeklyCalendar calls GET /api/calendar/weekly", async () => {
    await fetchWeeklyCalendar();
    expect(mockFetch).toHaveBeenCalledWith(
      "/api/calendar/weekly",
      expect.anything(),
    );
  });

  it("pushToCalendar calls POST /api/calendar/push", async () => {
    const experiments = [{ record_id: "rec1" }];
    await pushToCalendar(experiments as never[]);
    const [url, opts] = mockFetch.mock.calls[0];
    expect(url).toBe("/api/calendar/push");
    expect(opts.method).toBe("POST");
    expect(JSON.parse(opts.body)).toEqual({ experiments });
  });
});

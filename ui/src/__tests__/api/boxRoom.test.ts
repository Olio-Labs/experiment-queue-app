import {
  fetchBoxRoomData,
  fetchBoxVideo,
  fetchFlaggedIssues,
  fetchCartVideos,
} from "../../api/boxRoom";

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

describe("boxRoom API", () => {
  it("fetchBoxRoomData with no args calls GET /api/box-room", async () => {
    await fetchBoxRoomData();
    expect(mockFetch).toHaveBeenCalledWith(
      "/api/box-room",
      expect.anything(),
    );
  });

  it("fetchBoxRoomData with startDate appends query param", async () => {
    await fetchBoxRoomData("2026-04-08");
    expect(mockFetch).toHaveBeenCalledWith(
      "/api/box-room?start_date=2026-04-08",
      expect.anything(),
    );
  });

  it("fetchBoxRoomData with experimentId appends query param", async () => {
    await fetchBoxRoomData(undefined, "exp123");
    expect(mockFetch).toHaveBeenCalledWith(
      "/api/box-room?experiment_id=exp123",
      expect.anything(),
    );
  });

  it("fetchBoxVideo builds query string with all params", async () => {
    await fetchBoxVideo("cage1", "box1", "2026-04-08", "12:00", "exp1");
    const [url] = mockFetch.mock.calls[0];
    expect(url).toContain("/api/box-room/video?");
    expect(url).toContain("cage_id=cage1");
    expect(url).toContain("box_id=box1");
    expect(url).toContain("start_date=2026-04-08");
    expect(url).toContain("timestamp=12%3A00");
    expect(url).toContain("experiment_id=exp1");
  });

  it("fetchFlaggedIssues calls correct URL", async () => {
    await fetchFlaggedIssues(42);
    expect(mockFetch).toHaveBeenCalledWith(
      "/api/box-room/flagged-issues/42",
      expect.anything(),
    );
  });

  it("fetchCartVideos builds query string", async () => {
    await fetchCartVideos("c1", "b1", "2026-04-08");
    const [url] = mockFetch.mock.calls[0];
    expect(url).toContain("/api/box-room/cart-videos?");
    expect(url).toContain("cage_id=c1");
    expect(url).toContain("box_id=b1");
    expect(url).toContain("start_date=2026-04-08");
  });
});

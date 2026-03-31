import { get } from "./client";
import type { BoxRoomData, BoxVideoResponse } from "../types";

export function fetchBoxRoomData(
  startDate?: string,
  experimentId?: string,
): Promise<BoxRoomData> {
  const params = new URLSearchParams();
  if (startDate) params.set("start_date", startDate);
  if (experimentId) params.set("experiment_id", experimentId);
  const qs = params.toString();
  return get<BoxRoomData>(`/box-room${qs ? `?${qs}` : ""}`);
}

export function fetchBoxVideo(
  cageId: string,
  boxId: string,
  startDate?: string,
  timestamp?: string,
  experimentId?: string,
): Promise<BoxVideoResponse> {
  const params = new URLSearchParams({ cage_id: cageId, box_id: boxId });
  if (startDate) params.set("start_date", startDate);
  if (timestamp) params.set("timestamp", timestamp);
  if (experimentId) params.set("experiment_id", experimentId);
  return get<BoxVideoResponse>(`/box-room/video?${params}`);
}

export function fetchFlaggedIssues(
  boxNumber: number,
  startDate?: string,
  experimentId?: string,
): Promise<unknown[]> {
  const params = new URLSearchParams();
  if (startDate) params.set("start_date", startDate);
  if (experimentId) params.set("experiment_id", experimentId);
  const qs = params.toString();
  return get<unknown[]>(`/box-room/flagged-issues/${boxNumber}${qs ? `?${qs}` : ""}`);
}

export function fetchCartVideos(
  cageId: string,
  boxId: string,
  startDate: string,
  experimentId?: string,
): Promise<Record<string, unknown>> {
  const params = new URLSearchParams({
    cage_id: cageId,
    box_id: boxId,
    start_date: startDate,
  });
  if (experimentId) params.set("experiment_id", experimentId);
  return get(`/box-room/cart-videos?${params}`);
}

import type {
  SchemaBoundary,
  SchemaDiff,
  SchemaPrecheck,
  SchemaPublish,
  SchemaVersion,
} from '../types/models';
import { api } from './client';

export interface VersionsResponse {
  items: SchemaVersion[];
  total: number;
  pairs: Array<{ left_version_id: string; right_version_id: string; label: string }>;
  effective_boundaries: SchemaBoundary[];
  current_version: SchemaVersion | null;
}

export interface PublishListResponse {
  items: SchemaPublish[];
  total: number;
}

export interface PublishRequest {
  stream_id: string;
  version_id: string;
  effective_from_lsn: number;
  simulate_failure?: boolean;
}

export const schemaApi = {
  versions: (streamId: string) => api.get<VersionsResponse>(`/streams/${streamId}/schema-versions`),
  diff: (leftVersionId: string, rightVersionId: string) =>
    api.post<SchemaDiff>('/schema/diff', {
      left_version_id: leftVersionId,
      right_version_id: rightVersionId,
    }),
  precheck: (payload: PublishRequest) => api.post<SchemaPrecheck>('/schema/precheck', payload),
  publish: (payload: PublishRequest) => api.post<SchemaPublish>('/schema/publish', payload),
  publishes: (streamId: string) =>
    api.get<PublishListResponse>(`/schema/publishes?stream_id=${encodeURIComponent(streamId)}`),
};

import { useCallback, useEffect, useState } from 'react';
import { schemaApi, type VersionsResponse } from '../api/schema';
import { DiffTable, type DiffRow } from '../components/common/DiffTable';
import { EmptyState } from '../components/common/EmptyState';
import { MetricStrip } from '../components/common/MetricStrip';
import { PanelHead } from '../components/common/PanelHead';
import { StatusPill } from '../components/common/StatusPill';
import {
  CHANGE_KIND_TONE,
  COMPAT_LEVEL,
  SCHEMA_PUBLISH_STATUS,
  SCHEMA_VERSION_STATUS,
} from '../constants/cdc';
import { useAuthStore } from '../stores/authStore';
import { useStreamStore } from '../stores/streamStore';
import type {
  SchemaDiff as SchemaDiffType,
  SchemaPrecheck,
  SchemaPublish,
} from '../types/models';
import { errorMessage } from '../utils/errorMessage';
import { displayValue, formatLsn, formatShortTime } from '../utils/format';
import { fieldTypeText } from '../utils/schemaText';

const SqlBlock = ({ sql, label }: { sql: string; label: string }) => {
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(sql);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1600);
    } catch {
      setCopied(false);
    }
  };
  return (
    <div className="panel">
      <div className="panel-head-sm">
        {label}
        <button className="btn small copy-btn" onClick={() => void copy()}>
          {copied ? '已复制' : '复制'}
        </button>
      </div>
      <pre className="sql-block">{sql}</pre>
    </div>
  );
};

/** 预检结果：真实记录条数、受影响订单、越界明细。 */
const PrecheckBlock = ({ precheck }: { precheck: SchemaPrecheck }) => (
  <div style={{ marginTop: 12 }}>
    <div className={`notice ${precheck.violation_count > 0 ? 'danger' : 'ok'}`}>
      按版本 <span className="mono">{precheck.version_label}</span> 预检{' '}
      <span className="mono">{precheck.checked}</span> 条真实记录（来源：{precheck.sources.join(' / ') || '—'}），
      发现 <span className="mono">{precheck.violation_count}</span> 处越界，受影响订单{' '}
      <span className="mono">{precheck.affected_count}</span> 个。
      {precheck.violation_count > 0 ? ' 坏记录已单独隔离到拒绝队列。' : ''}
    </div>
    {precheck.affected_orders.length > 0 ? (
      <div className="chip-row" style={{ marginTop: 10 }} data-testid="affected-orders">
        {precheck.affected_orders.slice(0, 40).map((pk) => (
          <span key={pk} className="pill danger mono">
            {pk}
          </span>
        ))}
        {precheck.affected_orders.length > 40 ? (
          <span className="tiny faint">还有 {precheck.affected_orders.length - 40} 个…</span>
        ) : null}
      </div>
    ) : null}
    {precheck.violations.length > 0 ? (
      <table className="table" style={{ marginTop: 10 }}>
        <thead>
          <tr>
            <th>受影响订单</th>
            <th className="num">LSN</th>
            <th>来源</th>
            <th>列</th>
            <th>真实值</th>
            <th>规则类型</th>
            <th>原因</th>
          </tr>
        </thead>
        <tbody>
          {precheck.violations.slice(0, 50).map((item, index) => (
            <tr key={`${item.pk}-${item.column}-${index}`}>
              <td className="mono tiny">{item.pk ?? '∅'}</td>
              <td className="num tiny">{item.lsn === null ? '—' : formatLsn(item.lsn)}</td>
              <td className="tiny faint">{item.source}</td>
              <td className="mono tiny">{item.column}</td>
              <td className="mono tiny">{displayValue(item.value, 40)}</td>
              <td className="mono tiny faint">{item.type ?? '—'}</td>
              <td className="tiny">{item.detail}</td>
            </tr>
          ))}
        </tbody>
      </table>
    ) : null}
  </div>
);

/** 单次发布结果：生效边界 + 回退结果。 */
const PublishResultBlock = ({ result }: { result: SchemaPublish }) => {
  const tone = SCHEMA_PUBLISH_STATUS[result.status]?.tone ?? 'muted';
  const rollback = result.rollback;
  return (
    <div style={{ marginTop: 12 }}>
      <div className="form-row" style={{ marginBottom: 8, gap: 10 }}>
        <StatusPill label={SCHEMA_PUBLISH_STATUS[result.status]?.label ?? result.status} tone={tone} />
        <span className="tiny">
          版本 <span className="mono">{result.version_label ?? '—'}</span> · 生效边界{' '}
          <span className="mono">{result.effective_from_display}</span>
        </span>
        {result.previous_version_label ? (
          <span className="tiny faint">前序版本 {result.previous_version_label}</span>
        ) : null}
      </div>
      {result.error_message ? (
        <div className={`notice ${result.status === 'published' ? 'ok' : 'danger'}`}>{result.error_message}</div>
      ) : result.status === 'published' ? (
        <div className="notice ok">
          发布成功：版本从 LSN <span className="mono">{result.effective_from_lsn}</span> 起生效；
          老于该位点的事件继续按前序版本规则处理。
        </div>
      ) : null}
      {rollback.performed ? (
        <div className="notice" style={{ marginTop: 8 }} data-testid="publish-rollback">
          回退结果：保留/恢复版本{' '}
          <span className="mono">{rollback.restored_version_label ?? '（无前序版本）'}</span>
          {rollback.restored_effective_from_lsn !== null
            ? `（生效边界 ${formatLsn(rollback.restored_effective_from_lsn)}）`
            : ''}
          ，处理位置（水位线）恢复到{' '}
          <span className="mono">{formatLsn(rollback.restored_watermark_lsn)}</span>；失败原因{' '}
          <span className="mono">{rollback.reason ?? '—'}</span>。
        </div>
      ) : null}
      <PrecheckBlock precheck={result.precheck} />
      {result.apply?.statements && result.apply.statements.length > 0 ? (
        <pre className="sql-block" style={{ marginTop: 10 }}>{result.apply.statements.join('\n')}</pre>
      ) : null}
    </div>
  );
};

/** schema 演进：字段级 diff + 兼容等级 + 按位点生效的版本发布/回退。 */
export const SchemaDiffPage = () => {
  const board = useStreamStore((state) => state.board);
  const selectedStreamId = useStreamStore((state) => state.selectedStreamId);
  const selectStream = useStreamStore((state) => state.selectStream);
  const canWrite = useAuthStore((state) => state.can('schema:write'));

  const [versions, setVersions] = useState<VersionsResponse | null>(null);
  const [leftId, setLeftId] = useState('');
  const [rightId, setRightId] = useState('');
  const [diff, setDiff] = useState<SchemaDiffType | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const [publishes, setPublishes] = useState<SchemaPublish[]>([]);
  const [publishVersionId, setPublishVersionId] = useState('');
  const [boundary, setBoundary] = useState('');
  const [simulateFailure, setSimulateFailure] = useState(false);
  const [precheck, setPrecheck] = useState<SchemaPrecheck | null>(null);
  const [publishResult, setPublishResult] = useState<SchemaPublish | null>(null);
  const [publishError, setPublishError] = useState<string | null>(null);
  const [publishing, setPublishing] = useState(false);

  const stream = board?.streams.find((item) => item.id === selectedStreamId) ?? board?.streams[0] ?? null;
  const publishVersion = versions?.items.find((item) => item.id === publishVersionId) ?? null;
  const publishLocked = !publishVersion || publishVersion.status === 'active';

  const refreshVersions = useCallback(async (streamId: string) => {
    const response = await schemaApi.versions(streamId);
    setVersions(response);
    const pair = response.pairs[0];
    if (pair) {
      setLeftId(pair.left_version_id);
      setRightId(pair.right_version_id);
    } else if (response.items.length) {
      setLeftId(response.items[0].id);
      setRightId(response.items[response.items.length - 1].id);
    }
    return response;
  }, []);

  useEffect(() => {
    if (!stream) return;
    let cancelled = false;
    schemaApi
      .versions(stream.id)
      .then((response) => {
        if (cancelled) return;
        setVersions(response);
        const pair = response.pairs[0];
        if (pair) {
          setLeftId(pair.left_version_id);
          setRightId(pair.right_version_id);
        } else if (response.items.length) {
          setLeftId(response.items[0].id);
          setRightId(response.items[response.items.length - 1].id);
        }
        const pending = response.items.find((version) => version.status === 'draft');
        setPublishVersionId(pending?.id ?? response.items[response.items.length - 1]?.id ?? '');
        setBoundary(String(Math.max(1, Number(stream.max_lsn) + 1)));
      })
      .catch((err) => setError(errorMessage(err)));
    schemaApi
      .publishes(stream.id)
      .then((response) => {
        if (!cancelled) setPublishes(response.items);
      })
      .catch(() => {
        /* 发布历史失败不遮挡主页面 */
      });
    return () => {
      cancelled = true;
    };
  }, [stream?.id]);

  const compare = useCallback(async () => {
    if (!leftId || !rightId) return;
    setLoading(true);
    setError(null);
    try {
      setDiff(await schemaApi.diff(leftId, rightId));
    } catch (err) {
      setDiff(null);
      setError(errorMessage(err));
    } finally {
      setLoading(false);
    }
  }, [leftId, rightId]);

  useEffect(() => {
    if (leftId && rightId) void compare();
  }, [leftId, rightId, compare]);

  const runPrecheck = async () => {
    if (!stream || !publishVersionId) return;
    setPublishing(true);
    setPublishError(null);
    setPublishResult(null);
    try {
      setPrecheck(
        await schemaApi.precheck({
          stream_id: stream.id,
          version_id: publishVersionId,
          effective_from_lsn: Number(boundary),
        }),
      );
    } catch (err) {
      setPrecheck(null);
      setPublishError(errorMessage(err));
    } finally {
      setPublishing(false);
    }
  };

  const runPublish = async () => {
    if (!stream || !publishVersionId) return;
    setPublishing(true);
    setPublishError(null);
    setPublishResult(null);
    try {
      const result = await schemaApi.publish({
        stream_id: stream.id,
        version_id: publishVersionId,
        effective_from_lsn: Number(boundary),
        simulate_failure: simulateFailure,
      });
      setPublishResult(result);
      setPrecheck(result.precheck);
      const response = await schemaApi.publishes(stream.id);
      setPublishes(response.items);
      await refreshVersions(stream.id);
    } catch (err) {
      setPublishError(errorMessage(err));
    } finally {
      setPublishing(false);
    }
  };

  const changeRows: DiffRow[] = diff
    ? diff.changes.map((change) => ({
        field: change.column,
        before: change.old ? fieldTypeText(change.old) : '（不存在）',
        after: change.new ? fieldTypeText(change.new) : '（删除）',
        kind: change.kind_label,
      }))
    : [];

  const level = diff ? COMPAT_LEVEL[diff.compat_level] : null;

  return (
    <div>
      <PanelHead
        title="schema 演进"
        sub={
          stream
            ? `${stream.name} · 目标表 ${diff?.target_table ?? `dw.${stream.name}_dw`} · 版本 ${versions?.total ?? 0} 个`
            : '选择变更流'
        }
        actions={
          <select className="select" value={stream?.id ?? ''} onChange={(event) => selectStream(event.target.value)}>
            {(board?.streams ?? []).map((item) => (
              <option key={item.id} value={item.id}>
                {item.name}
              </option>
            ))}
          </select>
        }
      />

      <div className="panel" style={{ marginBottom: 16 }}>
        <div className="panel-body">
          <div className="form-row">
            <label className="field">
              <span className="field-label">旧版本</span>
              <select className="select" value={leftId} onChange={(event) => setLeftId(event.target.value)} style={{ minWidth: 200 }}>
                {(versions?.items ?? []).map((version) => (
                  <option key={version.id} value={version.id}>
                    {version.version_label}（{version.field_count} 列）
                  </option>
                ))}
              </select>
            </label>
            <label className="field">
              <span className="field-label">新版本</span>
              <select className="select" value={rightId} onChange={(event) => setRightId(event.target.value)} style={{ minWidth: 200 }}>
                {(versions?.items ?? []).map((version) => (
                  <option key={version.id} value={version.id}>
                    {version.version_label}（{version.field_count} 列）
                  </option>
                ))}
              </select>
            </label>
            <button className="btn primary" onClick={() => void compare()} disabled={loading || !leftId || !rightId}>
              {loading ? '判定中…' : '重新判定'}
            </button>
            {versions?.pairs.length ? (
              <span className="tiny faint" style={{ marginLeft: 'auto' }}>
                默认比较对：{versions.pairs[0].label}
              </span>
            ) : null}
          </div>
        </div>
      </div>

      {error ? <div className="notice danger" style={{ marginBottom: 12 }}>{error}</div> : null}

      <div className="section">
        <div className="section-label">
          <span>版本生效边界</span>
        </div>
        {!versions || versions.items.length === 0 ? (
          <EmptyState title="还没有 schema 版本" hint="登记至少两个版本后才能做兼容判定与发布。" />
        ) : (
          <table className="table" data-testid="version-boundaries">
            <thead>
              <tr>
                <th>版本</th>
                <th>状态</th>
                <th className="num">生效边界（含）</th>
                <th className="num">截至 LSN</th>
                <th className="num">字段</th>
                <th>说明</th>
              </tr>
            </thead>
            <tbody>
              {versions.items.map((version, index) => (
                <tr key={version.id} className="stagger" style={{ animationDelay: `${index * 20}ms` }}>
                  <td className="mono">{version.version_label}</td>
                  <td>
                    <StatusPill
                      label={SCHEMA_VERSION_STATUS[version.status]?.label ?? version.status_label}
                      tone={SCHEMA_VERSION_STATUS[version.status]?.tone ?? 'muted'}
                    />
                  </td>
                  <td className="num mono">
                    {version.effective_from_lsn === null ? '未发布' : formatLsn(version.effective_from_lsn)}
                  </td>
                  <td className="num mono faint">
                    {version.effective_to_lsn === null ? '—' : formatLsn(version.effective_to_lsn)}
                  </td>
                  <td className="num">{version.field_count}</td>
                  <td className="tiny faint">{version.ddl_summary}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <div className="section">
        <div className="section-label">
          <span>发布新版本（发布前用真实记录检查越界）</span>
        </div>
        <div className="panel">
          <div className="panel-body">
            {canWrite ? (
              <div className="form-row">
                <label className="field">
                  <span className="field-label">待发布版本</span>
                  <select
                    className="select"
                    value={publishVersionId}
                    onChange={(event) => setPublishVersionId(event.target.value)}
                    style={{ minWidth: 180 }}
                  >
                    {(versions?.items ?? []).map((version) => (
                      <option key={version.id} value={version.id}>
                        {version.version_label}（{version.field_count} 列）
                      </option>
                    ))}
                  </select>
                </label>
                <label className="field">
                  <span className="field-label">生效边界 LSN（含，可用历史位点补传）</span>
                  <input
                    className="input"
                    type="number"
                    min={1}
                    data-testid="publish-boundary"
                    value={boundary}
                    onChange={(event) => setBoundary(event.target.value)}
                    style={{ width: 220 }}
                  />
                </label>
                <label className="tiny" style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  <input
                    type="checkbox"
                    checked={simulateFailure}
                    onChange={(event) => setSimulateFailure(event.target.checked)}
                  />
                  模拟发布失败（验证回退）
                </label>
                <button
                  className="btn small"
                  data-testid="precheck-version"
                  onClick={() => void runPrecheck()}
                  disabled={publishing || publishLocked}
                >
                  预检
                </button>
                <button
                  className="btn primary"
                  data-testid="publish-version"
                  onClick={() => void runPublish()}
                  disabled={publishing || publishLocked}
                >
                  {publishing ? '发布中…' : '发布'}
                </button>
              </div>
            ) : (
              <div className="notice">当前角色只能查看 schema 判定；发布需要 engineer 或 admin。</div>
            )}
            {publishError ? <div className="notice danger" style={{ marginTop: 10 }}>{publishError}</div> : null}
            {precheck && !publishResult ? <PrecheckBlock precheck={precheck} /> : null}
            {publishResult ? <PublishResultBlock result={publishResult} /> : null}
          </div>
        </div>
      </div>

      <div className="section">
        <div className="section-label">
          <span>发布与回退记录</span>
        </div>
        {publishes.length === 0 ? (
          <EmptyState
            title="还没有发布记录"
            hint="选择待发布版本与生效边界后点击发布；失败时会自动回退版本并恢复处理位置。"
          />
        ) : (
          <table className="table">
            <thead>
              <tr>
                <th>时间</th>
                <th>版本</th>
                <th className="num">生效边界</th>
                <th>状态</th>
                <th className="num">预检 / 越界</th>
                <th>受影响订单</th>
                <th>回退结果</th>
              </tr>
            </thead>
            <tbody>
              {publishes.map((item) => (
                <tr key={item.id}>
                  <td className="tiny faint">{formatShortTime(item.created_at)}</td>
                  <td className="mono tiny">{item.version_label ?? '—'}</td>
                  <td className="num mono">{item.effective_from_display}</td>
                  <td>
                    <StatusPill
                      label={SCHEMA_PUBLISH_STATUS[item.status]?.label ?? item.status}
                      tone={SCHEMA_PUBLISH_STATUS[item.status]?.tone ?? 'muted'}
                    />
                  </td>
                  <td className="num tiny">
                    {item.precheck?.checked ?? 0} / {(item.precheck?.violation_count ?? 0)}
                  </td>
                  <td className="tiny mono" style={{ maxWidth: 260, wordBreak: 'break-all' }}>
                    {(item.precheck?.affected_orders ?? []).slice(0, 8).join(', ') || '—'}
                    {(item.precheck?.affected_orders ?? []).length > 8 ? ' …' : ''}
                  </td>
                  <td className="tiny faint">
                    {item.rollback?.performed
                      ? `恢复 ${item.rollback.restored_version_label ?? '—'} / 水位线 ${formatLsn(item.rollback.restored_watermark_lsn)}`
                      : '未回退'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {!diff ? (
        <EmptyState
          title="还没有可比对的版本"
          hint="一条流至少需要两个 SchemaVersion（例如 v1 → v2）。seed 数据里 orders 流已经有 v1/v2。"
        />
      ) : (
        <>
          <div data-testid="compat-level">
          <MetricStrip
            items={[
              {
                label: '兼容等级',
                value: level?.label ?? diff.compat_level,
                tone: diff.compat_level === 'breaking' ? 'danger' : diff.compat_level === 'full' ? 'ok' : 'amber',
                note: level?.hint,
              },
              { label: '变更条目', value: String(diff.changes.length), note: `${diff.breaking_count} 条破坏性` },
              { label: '受影响列', value: String(diff.affected_columns.length), note: diff.affected_columns.join(', ') || '—' },
              { label: '版本', value: `${diff.left.version} → ${diff.right.version}`, note: `字段 ${diff.left.field_count} → ${diff.right.field_count}` },
            ]}
          />
          </div>

          <div className="section">
            <div className="section-label">
              <span>字段级变更</span>
            </div>
            {diff.changes.length === 0 ? (
              <EmptyState title="两个版本字段完全一致" hint="没有需要生成的迁移 SQL。" />
            ) : (
              <table className="table">
                <thead>
                  <tr>
                    <th>列</th>
                    <th>变更类别</th>
                    <th>旧定义</th>
                    <th>新定义</th>
                    <th>兼容方向</th>
                    <th>说明</th>
                  </tr>
                </thead>
                <tbody>
                  {diff.changes.map((change, index) => (
                    <tr key={`${change.kind}-${change.column}-${index}`} className="stagger" style={{ animationDelay: `${index * 22}ms` }}>
                      <td className="mono">{change.column}</td>
                      <td>
                        <StatusPill
                          label={change.kind_label}
                          tone={CHANGE_KIND_TONE[change.kind] ?? 'danger'}
                          title={change.kind}
                        />
                      </td>
                      <td className="tiny mono faint">{change.old ? fieldTypeText(change.old) : '（不存在）'}</td>
                      <td className="tiny mono">{change.new ? fieldTypeText(change.new) : '（删除）'}</td>
                      <td>
                        <StatusPill
                          label={COMPAT_LEVEL[change.compat].label}
                          tone={COMPAT_LEVEL[change.compat].tone}
                        />
                      </td>
                      <td className="tiny muted">{change.note}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          <div className="section">
            <div className="section-label">
              <span>迁移与回滚脚本</span>
            </div>
            <div className="split-even">
              <SqlBlock sql={diff.migration_sql} label="前滚迁移 SQL" />
              <SqlBlock sql={diff.rollback_sql} label="回滚 SQL" />
            </div>
          </div>

          <div className="section">
            <div className="section-label">
              <span>字段对照（diff 渲染）</span>
            </div>
            <DiffTable rows={changeRows} beforeLabel={`${diff.left.version} 定义`} afterLabel={`${diff.right.version} 定义`} />
          </div>

          <div className="section">
            <div className="section-label">
              <span>发布建议</span>
            </div>
            <ol className="tiny muted" style={{ margin: 0, paddingLeft: 18, lineHeight: '22px' }}>
              {diff.deploy_steps.map((step) => (
                <li key={step}>{step}</li>
              ))}
            </ol>
          </div>

          {diff.changes.some((change) => (change.precheck_sql ?? []).length > 0) ? (
            <div className="section">
              <div className="section-label">
                <span>预检 SQL（结果为 0 才能继续）</span>
              </div>
              <pre className="sql-block">
                {diff.changes
                  .flatMap((change) => change.precheck_sql ?? [])
                  .join('\n')}
              </pre>
            </div>
          ) : null}
        </>
      )}
    </div>
  );
};

export type CounterName = 'transactions' | 'duplicates' | 'retries' | 'dlq' | 'lock_conflicts' | 'reconciliation_inconsistencies';

export interface MetricLabels {
  status?: string;
  role?: string;
}

const counters: Readonly<Record<CounterName, string>> = Object.freeze({
  transactions: 'Transactions by resulting status.',
  duplicates: 'Detected persistent idempotency or inbox duplicates.',
  retries: 'Scheduled retries after transient failures.',
  dlq: 'Messages sent to a dead letter queue.',
  lock_conflicts: 'Database lock conflicts and transaction retries.',
  reconciliation_inconsistencies: 'Wallet reconciliations that detected a ledger balance mismatch.',
});
const statuses = new Set(['PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED']);
const roles = new Set(['http', 'sqs', 'api', 'consumer', 'publisher', 'reference']);
const durationBounds = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10] as const;

function labelValues(labels: MetricLabels): string[] {
  const values: string[] = [];
  if (labels.status !== undefined && statuses.has(labels.status)) values.push(`status="${labels.status}"`);
  if (labels.role !== undefined && roles.has(labels.role)) values.push(`role="${labels.role}"`);
  return values;
}

function suffix(values: readonly string[]): string {
  return values.length === 0 ? '' : `{${values.join(',')}}`;
}

interface DurationSeries {
  count: number;
  sum: number;
  buckets: number[];
}

export class Metrics {
  private readonly counts = new Map<CounterName, Map<string, number>>();
  private readonly durations = new Map<string, DurationSeries>();
  private outboxLag = 0;

  increment(name: CounterName, labels: MetricLabels = {}): void {
    if (!Object.hasOwn(counters, name)) return;
    let series = this.counts.get(name);
    if (!series) {
      series = new Map();
      this.counts.set(name, series);
    }
    const key = labelValues(labels).join(',');
    series.set(key, (series.get(key) ?? 0) + 1);
  }

  observeDuration(seconds: number, labels: MetricLabels = {}): void {
    if (!Number.isFinite(seconds) || seconds < 0) return;
    const key = labelValues(labels).join(',');
    let series = this.durations.get(key);
    if (!series) {
      series = { count: 0, sum: 0, buckets: durationBounds.map(() => 0) };
      this.durations.set(key, series);
    }
    series.count += 1;
    series.sum += seconds;
    durationBounds.forEach((bound, index) => {
      if (seconds <= bound) series.buckets[index] = (series.buckets[index] ?? 0) + 1;
    });
  }

  setOutboxLag(seconds: number): void {
    if (Number.isFinite(seconds) && seconds >= 0) this.outboxLag = seconds;
  }

  renderPrometheus(): string {
    const lines: string[] = [];
    for (const name of Object.keys(counters) as CounterName[]) {
      const metric = `dwp_${name}_total`;
      lines.push(`# HELP ${metric} ${counters[name]}`, `# TYPE ${metric} counter`);
      const series = this.counts.get(name);
      if (!series || series.size === 0) lines.push(`${metric} 0`);
      else for (const [key, count] of [...series].sort(([a], [b]) => a.localeCompare(b))) {
        lines.push(`${metric}${suffix(key ? key.split(',') : [])} ${count}`);
      }
    }
    const duration = 'dwp_processing_duration_seconds';
    lines.push(`# HELP ${duration} Processing duration in seconds.`, `# TYPE ${duration} histogram`);
    const durationSeries = this.durations.size === 0
      ? [['', { count: 0, sum: 0, buckets: durationBounds.map(() => 0) }] as const]
      : [...this.durations].sort(([a], [b]) => a.localeCompare(b));
    for (const [key, series] of durationSeries) {
      const labels = key ? key.split(',') : [];
      durationBounds.forEach((bound, index) => {
        lines.push(`${duration}_bucket${suffix([...labels, `le="${bound}"`])} ${series.buckets[index] ?? 0}`);
      });
      lines.push(`${duration}_bucket${suffix([...labels, 'le="+Inf"'])} ${series.count}`);
      lines.push(`${duration}_sum${suffix(labels)} ${series.sum}`);
      lines.push(`${duration}_count${suffix(labels)} ${series.count}`);
    }
    lines.push('# HELP dwp_outbox_lag_seconds Age of the oldest unpublished event.', '# TYPE dwp_outbox_lag_seconds gauge');
    lines.push(`dwp_outbox_lag_seconds ${this.outboxLag}`);
    return `${lines.join('\n')}\n`;
  }
}

export const metrics = new Metrics();

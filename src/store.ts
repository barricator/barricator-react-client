import type {
  BarricadorClientOptions,
  ConnectionStatus,
  EvalResponse,
  FlagValue,
  UserContext,
} from "./types";

type Listener = () => void;

/**
 * Framework-agnostic core of the client SDK.
 *
 * Security model: the browser holds a low-privilege `clientKey` and POSTs the {@link UserContext} to
 * the backend's `/api/v1/flags/eval`. The server resolves all targeting internally and returns a
 * flattened `key -> value` map — raw rules never reach the client. An SSE connection signals when the
 * environment changes so the SDK re-fetches the evaluated map. Telemetry is buffered and flushed
 * asynchronously. Reads are O(1) and never throw: on any failure the SDK keeps the last values, and
 * unknown flags fall back to the caller-provided default in {@link getValue}.
 */
export class BarricadorStore {
  private readonly baseUrl: string;
  private readonly clientKey: string;
  private user: UserContext;
  private readonly streaming: boolean;
  private readonly telemetry: boolean;
  private readonly flushIntervalMs: number;

  private values: Record<string, FlagValue> = {};
  private status: ConnectionStatus = "initializing";

  /** Per-key listeners so a component only re-renders when *its* flag changes. */
  private readonly keyListeners = new Map<string, Set<Listener>>();
  private readonly statusListeners = new Set<Listener>();

  private readonly evalCounts = new Map<string, number>();
  private eventSource: EventSource | null = null;
  private flushTimer: ReturnType<typeof setInterval> | null = null;
  private closed = false;
  /** Avoid spamming the console on every reconnect/refresh failure. */
  private warnedEvalFailure = false;

  constructor(options: BarricadorClientOptions) {
    if (!options.clientKey) throw new Error("clientKey is required");
    if (!options.user?.key) throw new Error("user.key is required");
    this.clientKey = options.clientKey;
    this.user = options.user;
    this.baseUrl = (options.baseUrl ?? "https://app.barricador.com").replace(/\/$/, "");
    this.streaming = options.streaming ?? true;
    this.telemetry = options.telemetry ?? true;
    this.flushIntervalMs = options.flushIntervalMs ?? 30000;
  }

  async start(): Promise<void> {
    await this.refresh();
    if (this.streaming) this.connectStream();
    if (this.telemetry && typeof setInterval !== "undefined") {
      this.flushTimer = setInterval(() => void this.flush(), this.flushIntervalMs);
    }
  }

  /** Re-post the user context and swap in the freshly evaluated values. */
  async refresh(): Promise<void> {
    try {
      const res = await fetch(`${this.baseUrl}/api/v1/flags/eval`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.clientKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(this.user),
      });
      if (!res.ok) throw new Error(`eval failed: HTTP ${res.status}`);
      const body = (await res.json()) as EvalResponse;
      this.applyValues(body.values ?? {});
      this.setStatus("ready");
      this.warnedEvalFailure = false;
    } catch (err) {
      // Network/eval failure: keep last values; hooks fall back to defaults. Never throw.
      // CORS preflight rejection (origin not on BARRICADOR_CORS_ORIGINS) surfaces as a
      // TypeError/"Failed to fetch" with no response — identical to a missing key from the
      // caller's perspective — so surface a one-shot console hint.
      if (!this.warnedEvalFailure && typeof console !== "undefined") {
        this.warnedEvalFailure = true;
        const origin =
          typeof globalThis !== "undefined" && "location" in globalThis
            ? String((globalThis as { location?: { origin?: string } }).location?.origin ?? "")
            : "";
        console.warn(
          `[barricador] flag eval failed (${err instanceof Error ? err.message : String(err)}). ` +
            `Flags will use their fallback defaults. If this is a browser app, confirm that ` +
            `${origin || "this page's Origin"} is listed in BARRICADOR_CORS_ORIGINS on the ` +
            `Barricador backend (CORS preflight 403 looks like a network failure here).`,
        );
      }
      this.setStatus(this.status === "initializing" ? "offline" : this.status);
    }
  }

  /** Update the identified user and re-evaluate (e.g. after login). */
  async identify(user: UserContext): Promise<void> {
    this.user = user;
    await this.refresh();
  }

  getValue(key: string, fallback: FlagValue): FlagValue {
    if (this.telemetry) this.evalCounts.set(key, (this.evalCounts.get(key) ?? 0) + 1);
    return key in this.values ? this.values[key] : fallback;
  }

  getStatus(): ConnectionStatus {
    return this.status;
  }

  // --- subscriptions (drive useSyncExternalStore) ---

  subscribeKey(key: string, listener: Listener): () => void {
    let set = this.keyListeners.get(key);
    if (!set) {
      set = new Set();
      this.keyListeners.set(key, set);
    }
    set.add(listener);
    return () => set!.delete(listener);
  }

  subscribeStatus(listener: Listener): () => void {
    this.statusListeners.add(listener);
    return () => this.statusListeners.delete(listener);
  }

  // --- internals ---

  private applyValues(next: Record<string, FlagValue>): void {
    const previous = this.values;
    this.values = next;
    const changed = new Set<string>([...Object.keys(previous), ...Object.keys(next)]);
    for (const key of changed) {
      if (!shallowEqual(previous[key], next[key])) this.notifyKey(key);
    }
  }

  private notifyKey(key: string): void {
    this.keyListeners.get(key)?.forEach((l) => l());
  }

  private setStatus(status: ConnectionStatus): void {
    if (this.status !== status) {
      this.status = status;
      this.statusListeners.forEach((l) => l());
    }
  }

  private connectStream(): void {
    if (typeof EventSource === "undefined" || this.closed) return;
    // EventSource cannot set Authorization headers, so the client key travels as a query param.
    const url = `${this.baseUrl}/api/v1/flags/stream?clientKey=${encodeURIComponent(this.clientKey)}`;
    try {
      const es = new EventSource(url);
      this.eventSource = es;
      // On any flag change in the environment, re-fetch the pre-evaluated map for this user.
      es.addEventListener("flag-change", () => void this.refresh());
      es.addEventListener("connected", () => this.setStatus("ready"));
      es.onerror = () => {
        // EventSource auto-reconnects with its own backoff; reflect the transient outage.
        this.setStatus("offline");
      };
    } catch {
      this.setStatus("offline");
    }
  }

  async flush(): Promise<void> {
    if (!this.telemetry || this.evalCounts.size === 0) return;
    const events = [...this.evalCounts.entries()].map(([flagKey, count]) => ({
      flagKey,
      count,
      defaulted: !(flagKey in this.values),
    }));
    this.evalCounts.clear();
    try {
      await fetch(`${this.baseUrl}/api/v1/metrics/flush`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.clientKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ events }),
        keepalive: true,
      });
    } catch {
      // Telemetry is best-effort; drop on failure.
    }
  }

  close(): void {
    this.closed = true;
    if (this.flushTimer) clearInterval(this.flushTimer);
    this.eventSource?.close();
    void this.flush();
  }
}

function shallowEqual(a: FlagValue, b: FlagValue): boolean {
  if (Object.is(a, b)) return true;
  if (typeof a !== "object" || typeof b !== "object" || a === null || b === null) return false;
  return JSON.stringify(a) === JSON.stringify(b);
}

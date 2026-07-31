/**
 * Unit tests for the fleet turn-metric store (kind 44200 usage totals).
 *
 * Covers the store's riskiest behaviors, none previously tested:
 * - event-id dedup across the archive / relay-backfill / live paths
 * - archive paging budget and `before` cursor advancement
 * - relay backfill without a matching archive subscription
 * - failed-load retry (a rejected load must not poison later mounts)
 * - `resetFleetTurnMetricsStore()` community-switch semantics: generation
 *   guard abandons in-flight loads, the live subscription is torn down,
 *   and all cached state clears
 * - reference-stable totals snapshots for useSyncExternalStore
 *
 * ── Tauri IPC mock ───────────────────────────────────────────────────────────
 * @tauri-apps/api/core calls window.__TAURI_INTERNALS__.invoke(cmd, args).
 * Install a stub before any production import so get_identity,
 * list_save_subscriptions, read_archived_events, and decrypt_observer_event
 * are intercepted by command name without patching module internals.
 */

import assert from "node:assert/strict";
import { beforeEach, describe, it, mock } from "node:test";

// @tauri-apps/api/core reads window.__TAURI_INTERNALS__; node has no window.
if (typeof globalThis.window === "undefined") {
  globalThis.window = globalThis;
}

/** @type {Map<string, (args: unknown) => Promise<unknown>>} */
const ipcHandlers = new Map();

globalThis.__TAURI_INTERNALS__ = {
  invoke: (cmd, args) => {
    const handler = ipcHandlers.get(cmd);
    if (handler) return handler(args);
    return Promise.reject(new Error(`unmocked Tauri command: ${cmd}`));
  },
  transformCallback: (_cb) => Math.random(),
};

function setIpcHandler(cmd, fn) {
  ipcHandlers.set(cmd, fn);
}

// ── Production imports (after the IPC stub) ──────────────────────────────────

import { relayClient } from "@/shared/api/relayClient.ts";
import {
  ensureFleetTurnMetricsLoaded,
  getAgentUsageTotals,
  resetFleetTurnMetricsStore,
  subscribeFleetTurnMetrics,
} from "@/features/fleet/turnMetricsStore.ts";

// ── Fixtures ─────────────────────────────────────────────────────────────────

const OWNER_PUBKEY = "c".repeat(64);
const AGENT_PUBKEY = "a".repeat(64);
const KIND_METRIC = 44200;

function makeMetricEvent(seq, { agent = AGENT_PUBKEY } = {}) {
  return {
    id: `${String(seq).padStart(4, "0")}${"e".repeat(60)}`,
    pubkey: agent,
    created_at: 1000 + seq,
    kind: KIND_METRIC,
    tags: [["p", OWNER_PUBKEY]],
    // Content is opaque ciphertext on the wire; the decrypt mock keys off
    // the event id, so any placeholder works here.
    content: `cipher-${seq}`,
    sig: "f".repeat(128),
  };
}

/** Decrypted payload: sessionless, per-turn counts only → totals sum. */
function makePayload(seq) {
  return {
    harness: "test-harness",
    timestamp: new Date(1_700_000_000_000 + seq * 1000).toISOString(),
    turn: { inputTokens: 100, outputTokens: 10, totalTokens: 110 },
  };
}

/**
 * Wire the three load-path dependencies for one test.
 * - archiveSub: whether list_save_subscriptions advertises a 44200 owner_p sub
 * - archivePages: array of pages, each an array of raw events, returned in
 *   order by read_archived_events (later calls return [])
 * - relayEvents: events returned by relayClient.fetchEvents
 * Returns call logs plus the captured live-subscription handler.
 */
function wireLoadPaths({
  archiveSub = true,
  archivePages = [],
  relayEvents = [],
  identityFails = false,
} = {}) {
  const calls = { archiveReads: [], liveFilters: [], unsubscribes: 0 };
  const live = { handler: null };

  setIpcHandler("get_identity", () => {
    if (identityFails) return Promise.reject(new Error("no identity"));
    return Promise.resolve({ pubkey: OWNER_PUBKEY, display_name: "owner" });
  });

  setIpcHandler("list_save_subscriptions", () =>
    Promise.resolve(
      archiveSub
        ? [
            {
              identity_pubkey: OWNER_PUBKEY,
              relay_url: "wss://test",
              scope_type: "owner_p",
              scope_value: OWNER_PUBKEY,
              kinds: `[${KIND_METRIC}]`,
              created_at: 1000,
            },
          ]
        : [],
    ),
  );

  let archiveCall = 0;
  setIpcHandler("read_archived_events", (args) => {
    calls.archiveReads.push(args);
    const page = archivePages[archiveCall] ?? [];
    archiveCall += 1;
    return Promise.resolve(page.map((event) => JSON.stringify(event)));
  });

  setIpcHandler("decrypt_observer_event", (args) => {
    const event = JSON.parse(args.eventJson);
    const seq = Number.parseInt(event.id.slice(0, 4), 10);
    return Promise.resolve(makePayload(seq));
  });

  mock.method(relayClient, "fetchEvents", () => Promise.resolve(relayEvents));
  mock.method(relayClient, "subscribeLive", (filter, onEvent) => {
    calls.liveFilters.push(filter);
    live.handler = onEvent;
    return Promise.resolve(() => {
      calls.unsubscribes += 1;
      return Promise.resolve();
    });
  });

  return { calls, live };
}

function tick() {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

beforeEach(() => {
  ipcHandlers.clear();
  mock.reset();
  resetFleetTurnMetricsStore();
});

// ── Dedup across sources ─────────────────────────────────────────────────────

describe("turnMetricsStore dedup", () => {
  it("counts an event once across archive, relay backfill, and live", async () => {
    const shared = makeMetricEvent(1);
    const relayOnly = makeMetricEvent(2);
    const { live } = wireLoadPaths({
      archivePages: [[shared]],
      relayEvents: [shared, relayOnly],
    });

    await ensureFleetTurnMetricsLoaded();

    // Live redelivers an already-seen event plus one new one.
    live.handler(relayOnly);
    live.handler(makeMetricEvent(3));
    await tick();

    const totals = getAgentUsageTotals(AGENT_PUBKEY);
    assert.equal(totals.turnCount, 3, "3 distinct events → 3 turns");
    assert.equal(totals.totalTokens, 330);
  });
});

// ── Archive paging ───────────────────────────────────────────────────────────

describe("turnMetricsStore archive paging", () => {
  it("stops at the page budget and advances the before cursor", async () => {
    // Three full pages of 200 → the budget (3) must bound the reads even
    // though every page comes back full.
    const fullPage = (base) =>
      Array.from({ length: 200 }, (_, i) => makeMetricEvent(base + i));
    const { calls } = wireLoadPaths({
      archivePages: [fullPage(1000), fullPage(2000), fullPage(3000)],
    });

    await ensureFleetTurnMetricsLoaded();

    assert.equal(calls.archiveReads.length, 3, "page budget caps reads");
    assert.equal(calls.archiveReads[0].beforeCreatedAt, null);
    // Cursor advances to the oldest row of the previous page.
    assert.equal(
      calls.archiveReads[1].beforeCreatedAt,
      1000 + 1000 + 199,
      "second read pages from the first page's oldest row",
    );
    assert.equal(getAgentUsageTotals(AGENT_PUBKEY).turnCount, 600);
  });

  it("skips the archive entirely without a matching 44200 subscription", async () => {
    const { calls } = wireLoadPaths({
      archiveSub: false,
      relayEvents: [makeMetricEvent(1)],
    });

    await ensureFleetTurnMetricsLoaded();

    assert.equal(calls.archiveReads.length, 0);
    assert.equal(getAgentUsageTotals(AGENT_PUBKEY).turnCount, 1);
  });
});

// ── Failure and retry ────────────────────────────────────────────────────────

describe("turnMetricsStore load failure", () => {
  it("retries after a failed load instead of caching the rejection", async () => {
    wireLoadPaths({ identityFails: true });
    await ensureFleetTurnMetricsLoaded();
    assert.equal(getAgentUsageTotals(AGENT_PUBKEY).turnCount, 0);

    // Same store, dependencies healthy again → the next mount must reload.
    wireLoadPaths({ relayEvents: [makeMetricEvent(1)] });
    await ensureFleetTurnMetricsLoaded();
    assert.equal(getAgentUsageTotals(AGENT_PUBKEY).turnCount, 1);
  });
});

// ── Community-switch reset ───────────────────────────────────────────────────

describe("resetFleetTurnMetricsStore", () => {
  it("clears totals and tears down the live subscription", async () => {
    const { calls } = wireLoadPaths({ relayEvents: [makeMetricEvent(1)] });
    await ensureFleetTurnMetricsLoaded();
    assert.equal(getAgentUsageTotals(AGENT_PUBKEY).turnCount, 1);
    assert.equal(calls.liveFilters.length, 1);

    resetFleetTurnMetricsStore();
    await tick();

    assert.equal(calls.unsubscribes, 1, "live subscription unsubscribed");
    assert.equal(getAgentUsageTotals(AGENT_PUBKEY).turnCount, 0);
  });

  it("abandons an in-flight load from the previous generation", async () => {
    // Hold get_identity un-resolved until after the reset fires.
    let releaseIdentity;
    const gate = new Promise((resolve) => {
      releaseIdentity = resolve;
    });
    const { calls } = wireLoadPaths({ relayEvents: [makeMetricEvent(1)] });
    setIpcHandler("get_identity", async () => {
      await gate;
      return { pubkey: OWNER_PUBKEY, display_name: "owner" };
    });

    const notified = mock.fn();
    const unsubscribe = subscribeFleetTurnMetrics(notified);
    const loadDone = ensureFleetTurnMetricsLoaded();

    resetFleetTurnMetricsStore();
    const notificationsAtReset = notified.mock.callCount();
    releaseIdentity();
    await loadDone;
    await tick();

    assert.equal(
      getAgentUsageTotals(AGENT_PUBKEY).turnCount,
      0,
      "stale load must not populate the new generation",
    );
    assert.equal(
      calls.liveFilters.length,
      0,
      "stale load must not leave a live subscription",
    );
    assert.equal(
      notified.mock.callCount(),
      notificationsAtReset,
      "stale load must not notify listeners after reset",
    );
    unsubscribe();
  });
});

// ── Snapshot stability ───────────────────────────────────────────────────────

describe("turnMetricsStore snapshots", () => {
  it("returns reference-stable totals until that agent changes", async () => {
    const { live } = wireLoadPaths({ relayEvents: [makeMetricEvent(1)] });
    await ensureFleetTurnMetricsLoaded();

    const first = getAgentUsageTotals(AGENT_PUBKEY);
    assert.equal(getAgentUsageTotals(AGENT_PUBKEY), first, "cached snapshot");

    const other = "b".repeat(64);
    live.handler(makeMetricEvent(5, { agent: other }));
    await tick();
    assert.equal(
      getAgentUsageTotals(AGENT_PUBKEY),
      first,
      "unrelated agent ingest must not invalidate this agent's snapshot",
    );

    live.handler(makeMetricEvent(6));
    await tick();
    const updated = getAgentUsageTotals(AGENT_PUBKEY);
    assert.notEqual(updated, first);
    assert.equal(updated.turnCount, 2);
  });
});

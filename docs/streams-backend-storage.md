# Streams backend storage implementation

This document translates the previously agreed plan into concrete steps for wiring Durable Streams’ TypeScript client, `createStreamDB`, and TanStack’s IndexedDB persistence so that browser tabs can durably cache SSE-delivered stream state.

## 1. Requirements recap

1. **Payload form** – We assume JSON messages delivered over SSE (Durable Streams JSON mode). If a stream emits binary chunks, add custom codec adapters before events hit StreamDB collections.
2. **Longevity** – State should survive full page reloads. We persist both the materialized TanStack DB collections and the last confirmed cursor/offset in IndexedDB so reloads can resume immediately.
3. **Sensitivity & quota** – IndexedDB typically offers hundreds of MB per origin. Encrypt payloads before persisting if the stream carries sensitive data. Keep per-stream TTLs to prevent unbounded growth.
4. **Concurrency** – Each tab maintains its own StreamDB instance. Optionally use `BroadcastChannel` to fan out updates or cache invalidations between tabs.

## 2. Client stack and package references

Install the Durable Streams TypeScript client, state helpers, and TanStack DB packages (includes the persistence utilities and React hooks):

```bash
npm install @durable-streams/client @durable-streams/state @tanstack/db @tanstack/react-db
npm install @tanstack/query-persist-client-indexeddb # persister for IndexedDB
```

> Note: if the project already uses `@tanstack/react-query`, the persistence patterns are nearly identical; we simply use the DB-specific adapters.

## 3. Define schema + events with `createStateSchema`

Map each stream entity set to a Standard Schema (Zod in the example) so StreamDB can type-check inserts/updates/deletes:

```ts
import { createStateSchema } from "@durable-streams/state";
import { z } from "zod";

const messageSchema = z.object({
  id: z.string(),
  roomId: z.string(),
  author: z.string(),
  text: z.string(),
  timestamp: z.string(),
});

export const streamSchema = createStateSchema({
  messages: {
    schema: messageSchema,
    type: "message",
    primaryKey: "id",
  },
});
```

The schema exposes helpers such as `streamSchema.messages.insert({ value })` which we reuse when appending events via Durable Streams’ append API.

## 4. Create a StreamDB bound to the SSE endpoint

```ts
import { createStreamDB } from "@durable-streams/state";
import { streamSchema } from "./schema";

export function createChatDB(streamUrl: string) {
  const db = createStreamDB({
    streamOptions: {
      url: streamUrl,
      contentType: "application/json",
      live: "sse", // force SSE mode
    },
    state: streamSchema,
  });

  return db;
}
```

`db.preload()` replays the entire stream (catch-up) and then stays subscribed for live updates, keeping TanStack DB collections hot.

## 5. Persist StreamDB state with a TanStack IDB adapter

```ts
import { createIDBPersister } from "@tanstack/query-persist-client-indexeddb";
import { persistDB } from "@tanstack/db-persist"; // mirrors query-persist API

const persister = createIDBPersister({
  dbName: "durable-streams",
  storeName: "stream-db-cache",
});

export async function bootstrapChatDB(streamUrl: string) {
  const db = createChatDB(streamUrl);

  await persistDB({
    db,
    persister,
    serialize: (state) => state, // collections already JSON-safe
    deserialize: (state) => state,
  });

  await db.preload({
    offset: await loadLastOffset(streamUrl),
  });

  return db;
}
```

The persister hydrates TanStack DB collections before `preload()` pulls from the network, so UI queries render immediately from cache. Swap the persister for `persist-localstorage` or an in-memory stub for environments without IndexedDB.

## 6. Track cursors/metadata alongside DB snapshots

```ts
import { get, set } from "idb-keyval";

const CURSOR_STORE = "stream-metadata";

async function loadLastOffset(url: string) {
  return (
    ((await get(`${CURSOR_STORE}:${url}`)) as string | undefined) ?? undefined
  );
}

async function saveOffset(url: string, offset: string) {
  await set(`${CURSOR_STORE}:${url}`, offset);
}
```

When consuming batches:

```ts
const res = await db.stream.stream({ live: "sse" });
const unsubscribe = res.subscribeJson(async (batch) => {
  await processBatch(batch.items);
  await saveOffset(batch.cursor.url, batch.offset);
});
```

`StreamResponse.subscribeJson` emits `{ items, offset }` tuples referenced in the Durable Streams TypeScript client docs, giving us precise resume points.

## 7. React integration via TanStack DB hooks

```tsx
import { useLiveQuery } from "@tanstack/react-db";

export function useMessages(db: ReturnType<typeof createChatDB>, roomId: string) {
  return useLiveQuery((q) =>
    q
      .from({ messages: db.collections.messages })
      .where(({ messages }) => messages.roomId.eq(roomId))
      .orderBy(({ messages }) => messages.timestamp.asc())
  );
}
```

Because the DB is persisted, `useLiveQuery` returns cached rows immediately after a reload and receives differential updates as SSE events arrive.

## 8. Optional optimistic actions & writes

```ts
const db = createStreamDB({
  streamOptions,
  state: streamSchema,
  actions: ({ db, stream }) => ({
    addMessage: {
      onMutate: (message) => db.collections.messages.insert(message),
      mutationFn: async (message) => {
        const txid = crypto.randomUUID();
        await stream.append(
          streamSchema.messages.insert({ value: message, headers: { txid } })
        );
        await db.utils.awaitTxId(txid);
      },
    },
  }),
});
```

If the backend rejects the append, TanStack DB rolls back the optimistic insert automatically.

## 9. Cleanup and multi-tab coordination

- When a stream closes, call `db.close()` and purge its collections via `db.collections.messages.clear()` plus delete the cursor entry from IndexedDB.
- For multi-tab safety, broadcast `{ streamUrl, offset }` updates through `BroadcastChannel` so all tabs share the freshest cursor.
- Periodically prune stored rows older than a TTL to stay under quota (e.g., run a Dexie transaction that deletes offsets below `currentOffset - historyWindow`).

## 10. Validation + testing checklist

1. **Hydration** – Start with populated IndexedDB, reload, and assert `useLiveQuery` returns cached rows before network activity.
2. **Resume correctness** – Kill the tab mid-stream, reopen, and confirm the client resumes from the saved offset without duplicating items.
3. **Schema migrations** – Bump collection definitions and ensure persister migrations upgrade stored snapshots (Dexie or TanStack DB migration hooks).
4. **Offline tolerance** – Simulate offline mode; verify writes queue and reads serve from cache until connectivity is restored.
5. **Cleanup** – Close a stream and verify both DB data and metadata are removed.

Following these steps implements the full plan: Durable Streams’ TypeScript client keeps the stream connection alive, StreamDB materializes state into TanStack DB collections, and the TanStack IndexedDB persister (plus cursor metadata) ensures tabs restart instantly with the latest durable snapshot.

## Offset contract — treat offsets as opaque

An offset is an **opaque, lexicographically-sortable** resume token (Durable Streams protocol §6, and the `ReadableStore` contract on the Python side). Persist and compare it accordingly:

- **Store it as a string**, verbatim. Its byte format is server/store-specific (a compound `{seq}_{byte}` string on the in-memory store, a zero-padded integer on the durable Django store) and may change — never assume one shape.
- **Never parse or do arithmetic on it.** No `parseInt(offset)`, no `offset - N`. To prune history, compare stored offsets lexicographically (`offset < keepFrom`) or track your own retained-count/age — do not subtract offsets.
- **Order by string comparison.** Because offsets sort lexicographically, `a < b` (plain string compare) is a valid "a is earlier than b" *within one stream*.

The same rule holds server-side: for a stable per-event key (e.g. a `/history` row version), use the opaque offset token itself, not `int(offset)`.

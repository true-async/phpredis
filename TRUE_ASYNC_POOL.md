# TrueAsync connection pool for phpredis — design & algorithm

A transparent connection pool for phpredis under TrueAsync, mirroring the proven
PDO pool (`php-src/ext/pdo/pdo_pool.{c,h}`) and adding multiplexing on top — the
best of both worlds.

**Status**

- **Stage 1 (checkout pool): implemented & tested.** `redis_pool.{c,h}`, wired
  into the command path; concurrent coroutines, transaction pinning and
  concurrent-MULTI isolation pass under ASAN.
- **Stage 2 (multiplexing): working (v0).** The mux command path is implemented
  and verified — concurrent commands over shared lanes return correct, correctly
  ordered replies under interleaving, fully leak-clean under a debug build
  (`report_memleaks`: N=3..64 concurrency, cancellation, mux+checkout coexist,
  connect failure — all zero leaks) and covered by `.phpt` (101–107). A single
  combined READABLE|WRITABLE poll handle per lane with non-blocking `send`/`recv`
  drives the pump. v0 caveats: plain TCP only (no SSL on the lane) and no
  mid-flight broken-lane recovery (§9b). See §5.

---

## 0. Context

- The `true-async` fork of phpredis was essentially empty (only PHP 8.6 shims).
  All I/O goes through `php_stream`.
- Under TrueAsync a blocking `php_stream` read **inside a coroutine parks
  automatically** via the reactor. So the existing synchronous phpredis code
  ("write command → blocking read reply") is **already correct in a
  one-connection-per-coroutine model** with no surgery in the command path.
- The single hazard: one `RedisSock` must not be used by two coroutines at once
  — the shared `stream`, `reply_callback`, `pipeline_cmd` and `mode` would
  interleave commands and replies. Coordination is required.

### Why pooling for Redis is its own question

Unlike SQL, Redis replies on a single connection arrive **strictly in command
order** (in-order RESP). So Redis admits *two* models, and the ecosystem is
split down the middle:

| Model | Used by | Idea |
|---|---|---|
| Multiplex (1 shared socket) | Lettuce, StackExchange.Redis, redis-rs `MultiplexedConnection`, ioredis | All coroutines write into one socket; replies are matched FIFO. Implicit pipelining for free. |
| Connection pool (checkout) | Jedis, go-redis, redis-py, deadpool | N connections, each coroutine borrows one exclusively. Like our PDO pool. |

Some commands **cannot** be multiplexed (they seize the whole connection):
`MULTI/EXEC`+`WATCH`, `SUBSCRIBE/PSUBSCRIBE`, blocking `BLPOP/BRPOP/WAIT`,
`pipeline()`, `SELECT`. So every "multiplexing" client is in fact **hybrid**:
multiplex for ordinary request/reply, a dedicated connection for the rest.
Lettuce is the canonical example.

**Why multiplexing matters under TrueAsync.** Redis is single-threaded. With a
pool of 10 connections and 1000 coroutines, 990 park waiting. With multiplexing
all 1000 commands stream into one socket, pipeline, and the server chews through
them back-to-back with no RTT stalls. For cache workloads (many small GET/SET)
that is a multiple-x difference.

---

## 1. Target architecture (hybrid)

The `max` physical connections split into two groups:

```
            ┌──────────────────────── zend_async_pool_t ───────────────────────┐
            │                                                                   │
   coroutine│   ┌─ mux reserve (mux=N) ─┐     ┌──── checkout pool (max - N) ──┐ │
   ─────────┼──>│  shared socket #1      │     │  conn  conn  conn  conn ...   │ │
   stateless│   │  shared socket #2      │     │  (personal, exclusive)        │ │
            │   │  (shared FIFO queue)   │     └───────────────────────────────┘ │
            │   └────────────────────────┘                ▲                      │
   coroutine│            ▲                                 │                      │
   ─────────┼────────────┘                       stateful: MULTI/WATCH/SUB/      │
   stateful │     stateless: GET/SET/...          BLPOP/SELECT/pipeline          │
            └───────────────────────────────────────────────────────────────────┘
```

- **mux reserve** (`mux=2..3`): never handed out personally. A shared request
  queue drives them (Stage 2). Stateless commands go here.
- **checkout pool** (the rest): lent to a coroutine exclusively for the duration
  of stateful work (Stage 1). This is exactly the "transaction" logic from the
  PDO pool.

A single point (`redis_sock_get`) decides where a command goes, based on the
command kind and the connection's current state.

### Two stages

- **Stage 1 — Checkout pool** ("share the socket by taking turns in time"). A
  direct port of `pdo_pool`. A coroutine borrows a physical `RedisSock`
  exclusively, holds it across a burst of commands / a transaction, and returns
  it. Correct for *all* command kinds at once. Low risk. Self-contained.
- **Stage 2 — Multiplex queue** ("build the queue"). On top of Stage 1: an
  event-driven reply pump over the mux reserve. Stateless commands go here;
  stateful ones stay on checkout. This is the full Lettuce-style hybrid.

Stage 1 is the foundation — dedicated connections for blocking/pubsub/txn are
needed by the hybrid anyway.

---

## 2. Data structures

Mirror `pdo_pool_binding_t` / the `pdo_dbh_t` pool fields.

```c
/* redis_pool.c */

/* Per-coroutine binding. Analogue of pdo_pool_binding_t. */
typedef struct {
    zend_async_event_callback_t event;  /* coroutine-finalize callback */
    redis_async_pool *pool;             /* owning pool, NULL once destroyed */
    RedisSock        *conn;             /* checked-out conn, or NULL */
    zend_ulong        coro_key;
    bool              has_coro_callback;
} redis_pool_binding_t;

/* Pool state, attached to the redis_object (the template). */
struct _redis_async_pool {
    zend_async_pool_t *async_pool;      /* physical RedisSock resources */
    HashTable         *bindings;        /* coro_key -> redis_pool_binding_t* */
    HashTable         *opts;            /* dup'd ctor options; factory replays */
    zend_object       *wrapper;         /* cached getPool() wrapper, released on destroy */
    long               db_default;      /* configured DB; drift pins the conn */
    uint32_t           mux_reserve;     /* connections reserved for multiplexing */
};
```

In pool mode the `Redis` object is a **template**: its own `RedisSock` is never
opened (it only holds config), and the live connections live in `async_pool` —
exactly like `pdo_dbh_t.driver_data == NULL` in the PDO pool.

A physical connection is a full `RedisSock` from the factory (host/port/auth/db
identical for all), each with its own `php_stream`.

---

## 3. Integration points (minimal surgery)

phpredis already funnels almost every command through two chokepoints:

- `redis_sock_get(zval *id, int nothrow)` — `library.c`, returns a `RedisSock*`.
- `redis_process_cmd()` / `redis_process_kw_cmd()` — `redis.c`. Both:
  1. `redis_sock = redis_sock_get(getThis(), 0);`
  2. build the command, write it, read the reply;
  3. `if (IS_ATOMIC(redis_sock)) resp_cb(...); else buffer (MULTI/PIPELINE)`.

**Stage 1 integration — two edits:**

1. `redis_sock_get()` becomes pool-aware:
   ```c
   if (Z_TYPE_P(id) == IS_OBJECT) {
       redis_object *obj = PHPREDIS_ZVAL_GET_OBJECT(redis_object, id);
       if (obj->pool != NULL) {
           return redis_pool_acquire_conn(obj, no_throw);  /* per-coro checkout */
       }
   }
   /* ...existing path... */
   ```
2. The tail of `redis_process_cmd` / `redis_process_kw_cmd` calls
   `redis_pool_maybe_release(getThis())`.

Commands that bypass these (subscribe, multi/exec, raw, pipeline) are exactly
the stateful ones that pin the connection, so they naturally hold it. They need
no extra handling in Stage 1.

Plus: `__construct` calls `redis_pool_create()` after `redis_sock_configure`;
`free_redis_object` calls `redis_pool_destroy`; `redis_sock_configure` accepts
the `pool` key silently; `redis_object` gains a `redis_async_pool *pool` field.

---

## 4. Algorithm — Stage 1 (checkout pool)

### 4.1 Acquire (port of `pdo_pool_acquire_conn`)

```
redis_pool_acquire_conn(obj):
    coro_key = current_coroutine_key()          # 0 outside a coroutine
    binding  = obj->pool->bindings[coro_key]

    if binding && binding->conn:
        return binding->conn                     # reuse within this coroutine

    resource = ZEND_ASYNC_POOL_ACQUIRE(async_pool, timeout=0)   # PARKS if empty
    if !resource: throw / return NULL

    if !binding:
        binding = ecalloc(...)
        binding->event.callback = on_coroutine_finish
        binding->event.dispose  = binding_dispose
        binding->coro_key = coro_key
        bindings[coro_key] = binding
        coro->event.add_callback(coro, &binding->event)   # release on finalize

    binding->conn = resource
    return binding->conn
```

### 4.2 Pin predicate (analogue of PDO `in_txn`)

A connection must not return to the pool while "dirty" (stateful):

```c
static bool redis_conn_is_pinned(const redis_async_pool *rp, RedisSock *s) {
    return !IS_ATOMIC(s)                 /* MULTI or PIPELINE buffering */
        || s->watching                   /* WATCH — optimistic lock */
        || redis_pool_sock_subscribed(s) /* pub/sub mode */
        || s->dbNumber != rp->db_default; /* SELECT moved off default DB */
    /* blocking commands (BLPOP) need no flag: the coroutine is parked *inside*
       the call holding the conn, so release-between-commands never runs. */
}
```

### 4.3 Maybe-release (port of `pdo_pool_maybe_release`)

```
redis_pool_maybe_release(id):                    # called at command-dispatch tail
    binding = pool->bindings[current_coro_key]
    if !binding || !binding->conn: return
    if redis_conn_is_pinned(pool, binding->conn): return   # keep pinned
    ZEND_ASYNC_POOL_RELEASE(async_pool, binding->conn)
    binding->conn = NULL
```

Net effect: between ordinary commands the connection returns to the pool (like
PDO between statements); during MULTI/WATCH/SUBSCRIBE/non-default-DB it stays
pinned to the coroutine.

### 4.4 Coroutine finalize

On coroutine end the registered callback force-releases a still-held connection
(safety net if the coroutine died mid-transaction/subscription) and frees the
binding.

### 4.5 Pool callbacks (`ZEND_ASYNC_NEW_POOL`)

- `factory`     → `redis_sock_create` + replay options via `redis_sock_configure`
  + `redis_sock_server_open` (connect → AUTH → SELECT default DB). One conn = one
  real TCP/UDS session.
- `destructor`  → disconnect + free `RedisSock`.
- `healthcheck` → cheap liveness (`php_stream_eof`).
- `before_acquire` → reject a connection that has gone bad.
- `before_release` → recycle only clean, atomic, default-DB connections; drop
  anything still stateful rather than leak state to the next borrower (force-
  release safety net; the pin predicate normally prevents reaching here dirty).

### 4.6 Connection-scoped state correctness

Only server session state leaks between borrowers: `SELECT`, `WATCH`, `MULTI`,
`SUBSCRIBE`, `CLIENT SETNAME/TRACKING`. Client-side config (serializer, prefix,
compression) is identical on every connection (set by the factory) — no leak.

v1 rule: the factory applies `db_default`; a runtime `SELECT` to another DB pins
the connection (predicate §4.2), so it is never returned to the shared pool
while off-default. Simple and correct within a coroutine; on finalize the dirty
connection is dropped rather than recycled.

---

## 5. Algorithm — Stage 2 (multiplex queue)

Enabled when `mux_reserve > 0`. Goal: stream many coroutines' stateless commands
over 1–3 shared sockets with implicit pipelining.

**Key principle (corrected): no dedicated reader coroutine.** Replies are read
**event-driven, in C callbacks running between coroutines**, driven by socket
readability — exactly the `curl_poll_callback` pattern in `ext/curl/curl_async.c`.
No coroutine is spent blocking on the read; no extra context switches.

### 5.1 The pieces

Per multiplexed lane (`redis_mux_t`):

- `RedisSock *sock` — the shared physical connection.
- **An in-flight waiter FIFO** — a plain intrusive list (`head`/`tail`, `next`),
  not a channel. Each waiter is a `zend_future_t` the sending coroutine awaits;
  the pump resolves it (`ZEND_FUTURE_COMPLETE`) with the framed reply. A Future
  already inherits an event and carries a result/exception, so it handles
  waker + result delivery + cancellation — no hand-rolled event or per-command
  `resp_cb`/`ctx` (those stay on the suspended coroutine's own stack). The list
  order is the reply-match order; per-command `emalloc` (ring-buffer slot reuse
  is a deferred optimization).
- `zend_async_poll_event_t *poll_ev` — **one** combined poll handle on the socket
  FD (`ZEND_ASYNC_NEW_SOCKET_EVENT_EX`), its mask re-armed on demand: READABLE
  while `in_flight > 0`, WRITABLE while `out_buf` holds unsent bytes. A single
  handle per fd is mandatory — two libuv poll handles on the same fd conflict
  (the deadlock found in early testing under write backpressure).
- `smart_string in_buf` — inbound bytes awaiting framing (partial RESP frames).
- `smart_string out_buf` — pending outbound bytes (batched writes → pipelining).

There are `mux` lanes (default small; 2 is a practical ceiling). A command is
assigned to the lane with the fewest in-flight replies (`argmin(in_flight)`,
tie-break round-robin). The choice is committed at send time — the reply returns
on that lane (Redis preserves order per connection). No per-coroutine lane
affinity is needed: in mux mode a coroutine has at most one command in flight
(its synchronous code awaits each reply before issuing the next).

### 5.2 Dispatch (in the generic command dispatchers)

The decision lives in `redis_process_cmd` / `redis_process_kw_cmd`, where the
command is built, not in `redis_sock_get` (which runs before the command is
known). The command kind is read from the built RESP bytes:

```
redis_process_cmd(obj, cmd_cb, resp_cb):
    if pool && mux_enabled && coro has no pinned conn:
        cmd_cb(template_sock, &cmd, &cmd_len)               # build the RESP frame
        if redis_cmd_is_multiplexable(cmd, cmd_len):        # parse name from bytes
            return redis_mux_dispatch(pool, cmd, cmd_len, resp_cb, ctx)   # §5.3
        # not multiplexable -> checkout with the already-built cmd
        sock = redis_pool_acquire_conn(obj); write; read; return
    ... existing checkout/normal path ...
```

`redis_cmd_is_multiplexable` parses the command name from the wire bytes
(`*argc\r\n$len\r\nNAME\r\n…`) and rejects `MULTI/EXEC/DISCARD/WATCH/UNWATCH,
SUBSCRIBE*/UNSUBSCRIBE*, BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/BZPOP*/BZMPOP,
WAIT/WAITAOF, SELECT/SWAPDB, MONITOR, RESET`. A coroutine that already holds a
pinned checkout connection (mid stateful sequence) keeps using it.

### 5.3 Command flow (mux mode)

Send (coroutine side), all in one synchronous step — no await between appending
bytes and registering the waiter, so FIFO order == wire order:

1. Append the built RESP frame to the chosen lane's out-buffer.
2. Register a waiter `{ awaitable, resp_cb, ctx, reply_frame slot }` at the tail
   of that lane's FIFO; `in_flight++`.
3. Optimistic non-blocking write of the out-buffer; on partial write/`EAGAIN`,
   keep the remainder and arm the WRITABLE event. Batching many coroutines'
   frames into one writev = implicit pipelining.
4. Ensure the READABLE event is armed; park on the awaitable.

### 5.4 Reply pump — split: I/O+framing in C, materialization in the coroutine

The READABLE C callback does **only non-blocking I/O and RESP framing** — never
parks, never materializes zvals:

```
redis_mux_pump(lane):                              # reactor C callback, between coroutines
    n = recv_nonblocking(lane->sock, lane->in_buf)
    if n == EOF/error: fail all waiters; teardown lane; return
    while a complete RESP reply is framed in lane->in_buf:   # byte-boundary scan only
        frame  = detach next reply bytes
        waiter = fifo_pop_front(lane); lane->in_flight--
        waiter->reply_frame = frame
        ZEND_ASYNC_CALLBACKS_NOTIFY(waiter->awaitable, NULL, NULL)   # resume coroutine
    if fifo empty: READABLE.stop()
```

The resumed coroutine — back in its own `redis_process_cmd` frame, where
`return_value`/`execute_data` are valid — materializes the reply by pointing a
RedisSock at a **memory stream over `reply_frame`** and running the ordinary
atomic `resp_cb` (which reads via `redis_sock->stream` and writes
`return_value`). This reuses phpredis's full reply parser/serializers unchanged;
the pump only needs a lightweight RESP frame-boundary scanner.

So reads happen in the C callback (no dedicated reader coroutine, no blocking);
only the cheap byte-copy materialization runs per coroutine, where the result
must land anyway.

### 5.5 Backpressure & degrade to checkout

Backpressure parks the producer: a bounded in-flight FIFO (caps pipeline depth)
and an out-buffer high-water mark (caps unsent bytes) both suspend the sending
coroutine until the lane drains. A non-multiplexable command, or a lane marked
broken, transparently falls back to `redis_pool_acquire_conn` (Stage 1);
stateful sequences (MULTI/SUB/BLPOP/SELECT) always run on a private connection.

### 5.6 Walkthrough — a `$redis->get('x')` over mux

The concrete end-to-end flow (implemented and working; see Status):

1. **Dispatch** (`redis_process_cmd`): build the command bytes
   (`*2\r\n$3\r\nGET\r\n$1\r\nx\r\n`) using the template socket's serializer.
   Gate on `redis_pool_should_mux` (pool + mux, in a coroutine, no pinned conn)
   and `redis_cmd_is_multiplexable` (GET → yes).
2. **Pick a lane** (`redis_mux_pick`): `argmin(in_flight)` across the lanes,
   lazily opening the socket (and its READABLE poll event → pump) on first use.
3. **Register a waiter**: a `zend_future_t` plus a FIFO node pushed at the lane's
   tail; `in_flight++`. The FIFO order is the wire order.
4. **Write** (`redis_mux_flush`): append the bytes to `out_buf` and non-blocking
   `send(MSG_DONTWAIT)` as much as the socket takes; any remainder stays buffered
   for the WRITABLE drain. Concurrent senders just append → one batched write =
   implicit pipelining.
5. **Arm the poll** (`redis_mux_update_poll`): set READABLE (reply pending) and
   WRITABLE (if bytes are still unsent) on the one combined handle so the reactor
   invokes the pump.
6. **Await** (`redis_mux_await`): the coroutine suspends on its Future; control
   returns to the scheduler and other coroutines pile their commands onto the
   same lane.
7. **Reply pump** (`redis_mux_pump`, a reactor C callback firing on socket
   readability, between coroutines): non-blocking `recv` into `in_buf`; for each
   complete RESP frame (`redis_resp_frame_len`) pop the FIFO head waiter (in
   order) and `ZEND_FUTURE_COMPLETE(future, frame)` → resolves the Future →
   resumes that coroutine.
8. **Materialize** (back in the resumed coroutine): wrap the frame in a read-only
   memory stream and run the ordinary atomic `resp_cb` → `return_value`.

Reply matching needs no correlation IDs: steps 3–6 fix the order, step 7 hands
replies out FIFO, and Redis guarantees reply order within a connection.

```
coroutines A,B,C  →  GET on one lane
   write:  [cmdA][cmdB][cmdC]   ── one batched write (pipeline)
   wire ←  replyA  replyB  replyC       (in order)
   pump:   replyA → FIFO.pop = A → wake A
           replyB → FIFO.pop = B → wake B
           replyC → FIFO.pop = C → wake C
```

### 5.7 Race-free lazy lane creation

Opening a lane *suspends*: `redis_pool_connect` performs the connect (and AUTH/
handshake) through the async stream, so the creating coroutine yields mid-open.
A naive "check `lanes[idx]`, connect, then store" therefore races: every
coroutine cold-starting at once sees the slot empty, each opens its own socket,
and all but the last assignment are orphaned — leaking `2·(N-1)` lanes for `N`
coroutines and blowing the "live connections == mux" invariant (a thundering
herd of connects).

The fix is to **reserve the slot synchronously before the suspending connect**
(`redis_mux_lane_get`): allocate the lane struct and store `lanes[idx]` with no
yield in between, *then* connect. Concurrent coroutines that pick the same idx
get the still-connecting lane and batch their commands onto it; `redis_mux_flush`
and `redis_mux_update_poll` are guarded (no socket / no poll event yet) so the
batch simply queues. When the connector finishes, its own dispatch flushes the
whole accumulated out-buffer and arms the poll — replies then drain FIFO as
usual. Exactly `mux` connections are opened, regardless of `N`.

If the connect fails, `redis_mux_lane_fail` detaches the slot (so a later
dispatch rebuilds it) and wakes any batched senders with a "not delivered"
result; each fails its command, and the last one to wake frees the dead lane
(refcount == its in-flight count) — no leak, no use-after-free.

---

## 6. PHP-level API

Transparent pool (like PDO: one object shared across coroutines), configured via
constructor options (phpredis 6 already accepts an assoc array):

```php
$redis = new Redis([
    'host' => '127.0.0.1',
    'port' => 6379,
    'auth' => ['user', 'pass'],
    'pool' => [
        'enabled' => true,
        'min'     => 0,     // prewarm
        'max'     => 16,    // total physical connections
        'mux'     => 2,     // Stage 2: reserve for multiplexing (0 = off)
    ],
]);

// Then it is ordinary phpredis — transparent to the user:
Async\spawn(fn() => $redis->get('a'));   // mux fast path
Async\spawn(function () use ($redis) {    // checkout: the transaction pins a conn
    $redis->multi();
    $redis->set('x', 1);
    $redis->exec();
});
```

`$redis->getPool()` → the `Async\Pool` wrapper for introspection
(`count()` / `idleCount()` / `activeCount()`), like `PDO::getPool()`; returns
`null` when pooling is disabled. The wrapper is created lazily and cached on the
pool, released on destroy.

---

## 7. Work checklist

### Stage 1 — checkout pool — DONE
- [x] `redis_pool.{c,h}`: structures, create/destroy.
- [x] Parse the `pool` option in the constructor; object → template mode.
- [x] factory / destructor / healthcheck / before_acquire / before_release on
      `ZEND_ASYNC_NEW_POOL`.
- [x] `redis_pool_acquire_conn` / `redis_pool_maybe_release` / on-finish callback.
- [x] `redis_sock_get` pool-aware; `redis_process_cmd`/`_kw_cmd` tail release.
- [x] `redis_conn_is_pinned` (MULTI/PIPELINE/WATCH/SUB/SELECT).
- [x] `getPool()` wrapper (`Async\Pool` introspection; null without a pool).
- [x] Tests `tests/async/` (construct, single, concurrent, backpressure,
      transaction pin, MULTI isolation, getPool introspection) — all green.

### Stage 2 — multiplex queue
- [x] `redis_cmd_is_multiplexable` classifier (command name parsed from RESP bytes).
- [x] RESP frame-boundary scanner (`redis_resp_frame_len`; RESP2/RESP3, nesting,
      attributes, partial-frame safe; unit-tested across 21 cases).
- [x] `redis_mux_t` lane struct + waiter (a `zend_future_t`) + lane array on the
      pool + lifecycle (lazy slots, teardown). Leak-clean construct/destroy.
- [x] Race-free lazy lane open (reserve slot before the suspending connect, §5.7)
      + `argmin(in_flight)` selection.
- [x] Reply pump: recv + frame + FIFO pop + `ZEND_FUTURE_COMPLETE`.
- [x] Coroutine-side materialization via memory-stream + atomic `resp_cb`.
- [x] Dispatch in `redis_process_cmd`/`_kw_cmd`; degrade to checkout.
- [x] Write path: non-blocking `send(MSG_DONTWAIT)` + WRITABLE-drain on one
      combined poll handle + out-buffer batching (implicit pipelining).
- [x] Connect-failure teardown: fail batched waiters, free dead lane (§5.7).
- [x] Tests `tests/async/` 101–107: basic, high-concurrency, interleaved
      ordering, stateful fallback, checkout coexist, cancellation, large frames.
- [ ] Backpressure: bounded FIFO + out-buffer high-water park the producer (§9b).
- [ ] Mid-flight broken-lane recovery + TLS on lanes (§9b).

---

## 8. Tests (`tests/async/`)

`.phpt` in the `php-src/ext/async/tests` style (coroutines via `Async\spawn` /
`await`), not the synchronous `TestRedis.php`. Helper
`inc/async_redis_pool_test.inc` after `ext/async/tests/pdo_mysql/inc/...`:
`skipIfNoAsync()`, `skipIfNoRedis()`, `skipIfNoServer()`, `poolFactory(max, mux)`.

Chaos-style invariants (true under any interleaving — count attempts/successes,
not exact values).

Current: 001–007 all pass — construct introspection, single, concurrent,
backpressure, transaction pin, concurrent-MULTI isolation, getPool introspection.

Planned Stage 2: mux many GET over one socket, reply ordering under interleaving,
fallback for MULTI/SUB/BLPOP, broken mux socket, implicit pipelining.

---

## 9. Open questions / risks

- **Where to call `maybe_release`.** v1: command-dispatch tail (between commands).
  Alternative — release on coroutine suspend (via TrueAsync switch handlers): the
  conn is "yours" while the coroutine actively bursts, returned when it parks.
  Better reuse; deferred until measured.
- **Stage 2 reply pump** — the response-callback machinery (`fold_item`,
  `reply_callback`) is per-`RedisSock` and assumes the caller reads; in mux mode
  the pump runs it on behalf of the originator.
- **RESP3 push** (client tracking / invalidation) — on a mux socket push frames
  arrive outside the reply FIFO; route them via a separate channel (like redis-rs
  `push_sender`). v1 mux: no RESP3 push / no client-side caching.
- **Cluster / Sentinel** (`redis_cluster.c`) — out of scope for v1; pooling first
  for the standalone `Redis`.

---

## 9a. Technical debt — multiplex reply framing

The RESP frame scanner (`redis_resp_frame_len`) is correct for well-formed
RESP2/RESP3 (unit-tested, 21 cases) and efficient on the common path — small
replies, and large bulk *strings* whose bodies are skipped arithmetically
(`total = header + len + 2`, no body walk). A single scan costs O(structure)
(element count + short headers), not O(payload). Known debt, to clear before the
multiplex path ships or as profiling dictates:

1. **O(elements²) re-scan of large aggregates under fragmentation.** The scanner
   is stateless and restarts at byte 0 on every `recv`, so a large multibulk
   (big `MGET`/`LRANGE`/`HGETALL`) delivered in chunks re-walks the
   already-arrived elements each time. Large bulk strings are unaffected.
   - Cheap mitigation (in the pump): keep a `frame_start` offset and never
     re-scan an already-completed reply — removes the O(N²) *between* replies in
     a multi-reply read.
   - Full fix: a resumable parser (saved position + a stack of remaining element
     counts), like hiredis, so a single large aggregate is never re-walked. Do
     this only if profiling shows large fragmented multibulks matter.
2. **No recursion-depth cap (stack-overflow DoS).** `resp_scan` recurses per
   nesting level; a pathological/hostile deeply nested reply overflows the C
   stack. Add a depth cap (e.g. 128 → protocol error). Low likelihood (trusted
   server) but defense-in-depth.
3. **No length bound (lane wedge).** `resp_int_line` parses the length field
   with no overflow/sanity check; a malformed huge length makes the scanner
   keep returning 0, wedging the lane on bytes that never arrive. Reject `n`
   beyond a sane maximum (>= 512 MiB) → protocol error.
4. **Micro:** `resp_line_end` scans for `\r\n` byte-by-byte; `memchr` is faster,
   but the lines here are short headers, so the gain is marginal. Low priority.

### 9b. Technical debt — multiplex v0

5. **Plain TCP only.** The pump reads via raw `recv(MSG_DONTWAIT)`, bypassing the
   stream filters — TLS on a mux lane is not supported in v0.
6. **Broken-lane recovery mid-flight.** A lane that drops *after* it is connected
   (peer reset with replies still pending) is not yet recovered: only the
   connect-time failure path fails its batched waiters (§5.7). A live-lane EOF
   should fail the in-flight futures and fall back to checkout (§5.5).

---

## 10. References

- PDO pool (the reference): `php-src/ext/pdo/pdo_pool.{c,h}`, `pdo_dbh.c`.
- Async pool API: `php-src/Zend/zend_async_API.h` (`zend_async_pool_t`,
  `ZEND_ASYNC_NEW_POOL`, `ZEND_ASYNC_POOL_ACQUIRE/RELEASE/CLOSE`).
- Event-driven socket I/O pattern: `ext/curl/curl_async.c` (`curl_poll_callback`,
  `ZEND_ASYNC_NEW_SOCKET_EVENT`), `ext/pgsql/pgsql.c`.
- Channel API: `ZEND_ASYNC_NEW_CHANNEL` in `php-src/Zend/zend_async_API.h`.
- Redis pooling vs multiplexing: <https://redis.io/docs/latest/develop/clients/pools-and-muxing/>

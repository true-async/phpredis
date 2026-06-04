# TrueAsync connection pool for phpredis — design & algorithm

A transparent connection pool for phpredis under TrueAsync, mirroring the proven
PDO pool (`php-src/ext/pdo/pdo_pool.{c,h}`) and adding multiplexing on top — the
best of both worlds.

**Status**

- **Stage 1 (checkout pool): implemented & tested.** `redis_pool.{c,h}`, wired
  into the command path; concurrent coroutines, transaction pinning and
  concurrent-MULTI isolation pass under ASAN.
- **Stage 2 (multiplexing): designed, not yet implemented.** See §5.

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

Per multiplexed socket (`redis_mux_t`):

- `RedisSock *sock` — the shared physical connection.
- **A waiter ring buffer** — the FIFO of in-flight requests. Use the existing
  `zend_async_channel_t` (`ZEND_ASYNC_NEW_CHANNEL`): it *is* a ring buffer, and
  pushing into it gives a ready-made trigger to pump. Each waiter holds
  `{ awaitable event, resp_cb, ctx }`.
- `zend_async_poll_event_t *read_ev` — a READABLE poll event on the socket FD
  (`ZEND_ASYNC_NEW_SOCKET_EVENT(fd, ASYNC_READABLE)`), started lazily while the
  FIFO is non-empty, stopped when it drains.
- A parse buffer for partial RESP frames split across reads.
- A write baton / out-buffer to serialize concurrent writes (and optionally
  batch frames before flush → pipelining).

### 5.2 Dispatch (mux extension of `redis_sock_get`)

```
redis_sock_get(obj):
    if !pool: return obj->sock
    binding = bindings[coro_key]
    if binding && binding->conn: return binding->conn        # already pinned (checkout)
    if mux && redis_cmd_is_multiplexable(state, cmd): MUX path (§5.3)
    return redis_pool_acquire_conn(obj)                      # checkout
```

`redis_cmd_is_multiplexable` excludes `SUBSCRIBE/PSUBSCRIBE/SSUBSCRIBE, MULTI,
WATCH, BLPOP/BRPOP/BLMOVE/BLMPOP/BZPOP*, WAIT/WAITAOF, SELECT/SWAPDB, MONITOR`
and any non-atomic / watching / subscribed state.

### 5.3 Command flow (mux mode)

A coroutine issuing a multiplexable command:

1. Serialize its RESP frame.
2. Write it to the shared socket under the write baton (or append to the
   out-buffer for batched flush — implicit pipelining).
3. Push a waiter `{ awaitable, resp_cb, ctx }` into the channel ring buffer.
4. Ensure `read_ev` is started.
5. Park on its awaitable (suspend).

### 5.4 Reply pump (C callback — the heart of Stage 2)

Fires on **socket-readable** (`read_ev`) and on **channel push** (drain
immediately in case a reply is already buffered). Runs between coroutines:

```
redis_mux_pump(mux):
    n = non_blocking_read(sock, parse_buf)        # poll/recv, NEVER parks
    if n == EOF or error: fail_all_waiters(mux); teardown(mux); return
    while parse_buf holds a complete RESP reply:
        waiter = channel_pop_front(mux->channel)  # FIFO order == reply order
        run waiter.resp_cb on the reply -> waiter result
        ZEND_ASYNC_CALLBACKS_NOTIFY(waiter.awaitable, result, NULL)  # resume coroutine
    if channel is empty: read_ev.stop()
```

The originating coroutine wakes with its own reply. Because Redis preserves
reply order on a connection, FIFO pop matches replies to requests with no
correlation IDs.

### 5.5 Degrade to checkout

A non-multiplexable command, or a mux socket marked broken, transparently falls
back to `redis_pool_acquire_conn` (Stage 1). Stateful scenarios
(MULTI/SUB/BLPOP) always run on a personal connection.

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

Optional `$redis->getPool()` → a PHP wrapper for introspection
(count/idle/active), like `PDO::getPool()`. (Not yet implemented.)

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
- [x] Tests `tests/async/` (single, concurrent, transaction pin, MULTI isolation).
- [ ] `getPool()` wrapper (introspection; tests 001/004 wait on it).

### Stage 2 — multiplex queue
- [ ] `redis_cmd_is_multiplexable` classifier.
- [ ] `redis_mux_t`: write baton, channel ring buffer, READABLE poll event.
- [ ] Reply pump: non-blocking drain, FIFO match, `ZEND_ASYNC_CALLBACKS_NOTIFY`.
- [ ] Write batching (implicit pipelining) + flush strategy.
- [ ] Degrade to checkout for non-multiplexable / broken sockets.
- [ ] Tests: reply ordering under interleaving, fallback, broken socket, batching.

---

## 8. Tests (`tests/async/`)

`.phpt` in the `php-src/ext/async/tests` style (coroutines via `Async\spawn` /
`await`), not the synchronous `TestRedis.php`. Helper
`inc/async_redis_pool_test.inc` after `ext/async/tests/pdo_mysql/inc/...`:
`skipIfNoAsync()`, `skipIfNoRedis()`, `skipIfNoServer()`, `poolFactory(max, mux)`.

Chaos-style invariants (true under any interleaving — count attempts/successes,
not exact values).

Current: 002 (single), 003 (concurrent), 005 (transaction pin), 006 (concurrent
MULTI isolation) pass. 001 (construct introspection) and 004 (backpressure
observability) need `getPool()`.

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

## 10. References

- PDO pool (the reference): `php-src/ext/pdo/pdo_pool.{c,h}`, `pdo_dbh.c`.
- Async pool API: `php-src/Zend/zend_async_API.h` (`zend_async_pool_t`,
  `ZEND_ASYNC_NEW_POOL`, `ZEND_ASYNC_POOL_ACQUIRE/RELEASE/CLOSE`).
- Event-driven socket I/O pattern: `ext/curl/curl_async.c` (`curl_poll_callback`,
  `ZEND_ASYNC_NEW_SOCKET_EVENT`), `ext/pgsql/pgsql.c`.
- Channel API: `ZEND_ASYNC_NEW_CHANNEL` in `php-src/Zend/zend_async_API.h`.
- Redis pooling vs multiplexing: <https://redis.io/docs/latest/develop/clients/pools-and-muxing/>

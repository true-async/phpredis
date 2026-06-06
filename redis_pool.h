/*
  TrueAsync connection pool for phpredis.
  See TRUE_ASYNC_POOL.md for the design and algorithm.
*/

#ifndef REDIS_POOL_H
#define REDIS_POOL_H

#include "common.h"

/* redis_pool is forward-declared in common.h (attached to redis_object). */

/*
 * Create a pool from constructor options.
 * If `opts` carries an enabled 'pool' key, builds the async pool, copies the
 * template connection config and attaches it to redis->pool. If no pool is
 * requested, leaves redis->pool == NULL. Returns FAILURE (with an exception)
 * on bad config or when the async runtime is unavailable.
 */
int redis_pool_create(redis_object *redis, HashTable *opts);

/* Tear down the pool: release active conns, detach bindings, close async pool. */
void redis_pool_destroy(redis_object *redis);

/* Lazily create and cache the PHP pool wrapper object (Async\Pool) for
 * Redis::getPool(). Returns NULL when the object has no pool. The returned
 * object is owned by the pool (released on destroy); callers that hand it to
 * userland must GC_ADDREF it. */
zend_object *redis_pool_get_wrapper(redis_object *redis);

/* True when a command (by its verb — the dispatch kw or method token) may ride
 * the shared multiplexed socket; false for stateful/blocking/connection-rebinding
 * commands that must take a private checkout connection. Case-insensitive. */
bool redis_cmd_is_multiplexable(const char *name);

/* Length of one complete RESP reply at buf[0..len), or 0 when more bytes are
 * needed. Used by the multiplex reply pump for frame boundaries. */
size_t redis_resp_frame_len(const char *buf, size_t len);

/* True when the current command should ride a multiplex lane: mux is enabled,
 * we are in a coroutine, and it holds no pinned checkout connection. */
bool redis_pool_should_mux(redis_object *redis);

/* Run a built command over a shared multiplex lane: enqueue, write, await the
 * reply and materialize it into return_value. Takes ownership of `cmd` (frees
 * it). Must be called from a coroutine on a multiplexable command. */
void redis_mux_dispatch(redis_object *redis, char *cmd, int cmd_len,
	FailableResultCallback resp_cb, void *ctx, INTERNAL_FUNCTION_PARAMETERS);

/*
 * Per-coroutine checkout. Returns a READY RedisSock or NULL on failure
 * (throws unless no_throw). Reuses the coroutine's pinned connection when one
 * is already bound; otherwise acquires from the pool, parking the coroutine if
 * the pool is exhausted.
 */
RedisSock *redis_pool_acquire_conn(redis_object *redis, int no_throw);

/*
 * Return the coroutine's connection to the pool unless it is pinned
 * (mid MULTI/PIPELINE, WATCH active, subscribed, or on a non-default DB).
 * Called at the tail of the command dispatchers. No-op when `id` is not a
 * Redis object or the object has no pool.
 */
void redis_pool_maybe_release(zval *id);

#endif /* REDIS_POOL_H */

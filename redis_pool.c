/*
  TrueAsync connection pool for phpredis.
  See TRUE_ASYNC_POOL.md for the design and algorithm.

  The user-facing Redis object becomes a template: its own RedisSock is never
  opened, physical connections live in a zend_async_pool_t. Each coroutine
  checks out a connection for the duration of a command burst (or a pinned
  stateful sequence) and returns it to the pool afterwards.
*/

#include "php_redis.h"

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "common.h"
#include "library.h"
#include "redis_pool.h"
#include "Zend/zend_async_API.h"
#include "zend_exceptions.h"

extern zend_class_entry *redis_exception_ce;

/*
 * Long-lived association between a pool and a coroutine. Created once on first
 * acquire, reused for subsequent acquire/release cycles within the same
 * coroutine, freed when the coroutine finalizes.
 */
typedef struct {
	zend_async_event_callback_t event;  /* must be first for the callback cast */
	redis_async_pool *pool;             /* owning pool, NULL once destroyed */
	RedisSock   *conn;                  /* checked-out conn, NULL if released */
	zend_ulong   coro_key;              /* key in pool->bindings */
	bool         has_coro_callback;     /* registered with the coroutine event */
} redis_pool_binding_t;

struct _redis_async_pool {
	zend_async_pool_t *async_pool;      /* physical RedisSock resources */
	HashTable         *bindings;        /* coro_key -> redis_pool_binding_t* */
	HashTable         *opts;            /* dup'd ctor options; factory re-applies */
	zend_object       *wrapper;         /* cached getPool() wrapper, released on destroy */
	long               db_default;      /* configured DB; drift pins the conn */
	uint32_t           mux_reserve;     /* connections reserved for multiplexing */
};

/* Stable hash key for the current coroutine (zend_object handle when available,
 * pointer otherwise). Mirrors the PDO pool key derivation. */
static zend_always_inline zend_ulong redis_pool_coro_key(zend_coroutine_t *coro)
{
	if (ZEND_ASYNC_EVENT_IS_ZEND_OBJ(&coro->event)) {
		return (zend_ulong)ZEND_ASYNC_EVENT_TO_OBJECT(&coro->event)->handle;
	}

	return ((uintptr_t)coro) >> ZEND_MM_ALIGNMENT_LOG2;
}

/* True if the connection currently carries pub/sub subscriptions. */
static bool redis_pool_sock_subscribed(const RedisSock *sock)
{
	for (int i = 0; i < REDIS_SUBS_BUCKETS; i++) {
		if (sock->subs[i] != NULL) {
			return true;
		}
	}

	return false;
}

/* A connection must stay pinned to its coroutine while stateful: mid
 * MULTI/PIPELINE, WATCH active, subscribed, or moved off the default DB. */
static bool redis_conn_is_pinned(const redis_async_pool *rp, RedisSock *sock)
{
	return !IS_ATOMIC(sock)
		|| sock->watching
		|| redis_pool_sock_subscribed(sock)
		|| sock->dbNumber != rp->db_default;
}

/* Close and free a pooled connection. */
static void redis_pool_free_conn(RedisSock *sock)
{
	if (sock == NULL) {
		return;
	}

	redis_sock_disconnect(sock, 0, 1);
	redis_free_socket(sock);
}

/*
 * Pool resource handlers
 */

/* Factory: build a fresh connection by replaying the constructor options. */
static bool redis_pool_factory(zend_async_pool_t *pool, zval *result)
{
	redis_async_pool *rp = (redis_async_pool *)pool->user_data;

	if (UNEXPECTED(rp == NULL)) {
		return false;
	}

	RedisSock *sock = redis_sock_create(ZEND_STRL("127.0.0.1"), 6379, 0, 0, 0, NULL, 0);
	if (UNEXPECTED(sock == NULL)) {
		return false;
	}

	if (rp->opts != NULL && redis_sock_configure(sock, rp->opts) != SUCCESS) {
		redis_pool_free_conn(sock);
		return false;
	}

	/* The pool owns connection lifetime; never route through the persistent
	 * connection registry. */
	sock->persistent = 0;

	if (redis_sock_server_open(sock) != SUCCESS) {
		redis_pool_free_conn(sock);
		return false;
	}

	ZVAL_PTR(result, sock);
	return true;
}

/* Destructor: close a connection removed from the pool. */
static void redis_pool_destructor(zend_async_pool_t *pool, zval *resource)
{
	redis_pool_free_conn(Z_PTR_P(resource));
}

/* Healthcheck: cheap liveness probe. */
static bool redis_pool_healthcheck(zend_async_pool_t *pool, zval *resource)
{
	RedisSock *sock = Z_PTR_P(resource);

	if (UNEXPECTED(sock == NULL || sock->stream == NULL)) {
		return false;
	}

	if (sock->status == REDIS_SOCK_STATUS_FAILED) {
		return false;
	}

	return php_stream_eof(sock->stream) == 0;
}

/* Before acquire: refuse a connection that has gone bad. */
static bool redis_pool_before_acquire(zend_async_pool_t *pool, zval *resource)
{
	RedisSock *sock = Z_PTR_P(resource);

	return sock != NULL && sock->stream != NULL
		&& sock->status != REDIS_SOCK_STATUS_FAILED;
}

/* Before release: recycle only clean, atomic connections on the default DB.
 * Anything still stateful is dropped rather than leak state to the next
 * borrower. The pin predicate normally prevents reaching here dirty — this is
 * the force-release safety net (coroutine finalized mid-sequence). */
static bool redis_pool_before_release(zend_async_pool_t *pool, zval *resource)
{
	redis_async_pool *rp = (redis_async_pool *)pool->user_data;
	RedisSock  *sock = Z_PTR_P(resource);

	if (sock == NULL || sock->stream == NULL) {
		return false;
	}

	if (sock->status == REDIS_SOCK_STATUS_FAILED) {
		return false;
	}

	if (!IS_ATOMIC(sock) || sock->watching || redis_pool_sock_subscribed(sock)) {
		return false;
	}

	if (rp != NULL && sock->dbNumber != rp->db_default) {
		return false;
	}

	return true;
}

/*
 * Binding callbacks — registered once per coroutine per pool.
 */

/* Dispose: free the binding once the coroutine event cleans up callbacks. */
static void redis_pool_binding_dispose(
	zend_async_event_callback_t *callback,
	zend_async_event_t *event)
{
	efree(callback);
}

/* On coroutine finalize: return a still-held connection to the pool. */
static void redis_pool_binding_on_coroutine_finish(
	zend_async_event_t *event,
	zend_async_event_callback_t *callback,
	void *result,
	zend_object *exception)
{
	redis_pool_binding_t *binding = (redis_pool_binding_t *)callback;

	/* Pool already destroyed — nothing to do. */
	if (binding->pool == NULL) {
		return;
	}

	if (binding->conn != NULL) {
		zval conn_zval;
		ZVAL_PTR(&conn_zval, binding->conn);
		ZEND_ASYNC_POOL_RELEASE(binding->pool->async_pool, &conn_zval);
		binding->conn = NULL;
	}

	if (binding->pool->bindings != NULL) {
		zend_hash_index_del(binding->pool->bindings, binding->coro_key);
	}
}

/*
 * Public API
 */

int redis_pool_create(redis_object *redis, HashTable *opts)
{
	if (opts == NULL) {
		return SUCCESS;
	}

	zval *zpool = zend_hash_str_find(opts, ZEND_STRL("pool"));
	if (zpool == NULL) {
		return SUCCESS;
	}

	ZVAL_DEREF(zpool);
	if (Z_TYPE_P(zpool) != IS_ARRAY) {
		REDIS_THROW_EXCEPTION("Invalid 'pool' option (expected array)", 0);
		return FAILURE;
	}

	HashTable *ph = Z_ARRVAL_P(zpool);
	zval *zv;

	zv = zend_hash_str_find(ph, ZEND_STRL("enabled"));
	if (zv == NULL || !zend_is_true(zv)) {
		return SUCCESS;
	}

	if (zend_async_new_pool_fn == NULL) {
		REDIS_THROW_EXCEPTION("Redis pool requires the async runtime (ext/async)", 0);
		return FAILURE;
	}

	zend_long min_size = 0, max_size = 10, mux = 0;

	if ((zv = zend_hash_str_find(ph, ZEND_STRL("min"))) != NULL) {
		min_size = zval_get_long(zv);
	}

	if ((zv = zend_hash_str_find(ph, ZEND_STRL("max"))) != NULL) {
		max_size = zval_get_long(zv);
	}

	if ((zv = zend_hash_str_find(ph, ZEND_STRL("mux"))) != NULL) {
		mux = zval_get_long(zv);
	}

	if (min_size < 0) min_size = 0;
	if (max_size < 1) max_size = 1;
	if (max_size < min_size) max_size = min_size;
	if (mux < 0) mux = 0;

	redis_async_pool *rp = ecalloc(1, sizeof(*rp));

	rp->async_pool = ZEND_ASYNC_NEW_POOL(
		redis_pool_factory,
		redis_pool_destructor,
		redis_pool_healthcheck,
		redis_pool_before_acquire,
		redis_pool_before_release,
		(uint32_t)min_size,
		(uint32_t)max_size,
		0);

	if (UNEXPECTED(rp->async_pool == NULL)) {
		efree(rp);
		REDIS_THROW_EXCEPTION("Failed to create Redis connection pool", 0);
		return FAILURE;
	}

	rp->async_pool->user_data = rp;

	rp->bindings = emalloc(sizeof(HashTable));
	zend_hash_init(rp->bindings, 8, NULL, NULL, 0);

	rp->opts = zend_array_dup(opts);
	rp->db_default = redis->sock ? redis->sock->dbNumber : 0;
	rp->mux_reserve = (uint32_t)mux;

	redis->pool = rp;
	return SUCCESS;
}

void redis_pool_destroy(redis_object *redis)
{
	redis_async_pool *rp = redis->pool;

	if (rp == NULL) {
		return;
	}

	/* Detach all bindings: release active connections, invalidate the rest. */
	if (rp->bindings != NULL) {
		redis_pool_binding_t *binding;
		ZEND_HASH_FOREACH_PTR(rp->bindings, binding) {
			if (binding->conn != NULL) {
				zval conn_zval;
				ZVAL_PTR(&conn_zval, binding->conn);
				ZEND_ASYNC_POOL_RELEASE(rp->async_pool, &conn_zval);
				binding->conn = NULL;
			}

			if (!binding->has_coro_callback) {
				efree(binding);
			} else {
				/* Coroutine callback will no-op; dispose frees the binding. */
				binding->pool = NULL;
			}
		} ZEND_HASH_FOREACH_END();

		zend_hash_destroy(rp->bindings);
		efree(rp->bindings);
		rp->bindings = NULL;
	}

	if (rp->wrapper != NULL) {
		OBJ_RELEASE(rp->wrapper);
		rp->wrapper = NULL;
	}

	if (rp->async_pool != NULL) {
		ZEND_ASYNC_POOL_CLOSE(rp->async_pool);
		ZEND_ASYNC_EVENT_RELEASE(&rp->async_pool->event);
		rp->async_pool = NULL;
	}

	if (rp->opts != NULL) {
		zend_array_release(rp->opts);
		rp->opts = NULL;
	}

	efree(rp);
	redis->pool = NULL;
}

RedisSock *redis_pool_acquire_conn(redis_object *redis, int no_throw)
{
	redis_async_pool *rp = redis->pool;
	zend_coroutine_t *coro = ZEND_ASYNC_CURRENT_COROUTINE;
	const zend_ulong coro_key = coro ? redis_pool_coro_key(coro) : 0;

	redis_pool_binding_t *binding = zend_hash_index_find_ptr(rp->bindings, coro_key);

	if (binding != NULL) {
		/* Reuse the connection already checked out for this coroutine. */
		if (EXPECTED(binding->conn != NULL)) {
			return binding->conn;
		}

		zval resource;
		if (UNEXPECTED(!ZEND_ASYNC_POOL_ACQUIRE(rp->async_pool, &resource, 0))) {
			if (!no_throw) {
				REDIS_THROW_EXCEPTION("Redis pool: failed to acquire connection", 0);
			}
			return NULL;
		}

		binding->conn = Z_PTR(resource);
		return binding->conn;
	}

	/* First acquire for this coroutine — create the binding. */
	zval resource;
	if (UNEXPECTED(!ZEND_ASYNC_POOL_ACQUIRE(rp->async_pool, &resource, 0))) {
		if (!no_throw) {
			REDIS_THROW_EXCEPTION("Redis pool: failed to acquire connection", 0);
		}
		return NULL;
	}

	binding = ecalloc(1, sizeof(*binding));
	binding->event.callback = redis_pool_binding_on_coroutine_finish;
	binding->event.dispose = redis_pool_binding_dispose;
	binding->event.ref_count = 1;
	binding->pool = rp;
	binding->conn = Z_PTR(resource);
	binding->coro_key = coro_key;

	zend_hash_index_add_new_ptr(rp->bindings, coro_key, binding);

	/* Register once on the coroutine — released back to the pool on finalize. */
	if (coro != NULL) {
		coro->event.add_callback(&coro->event, &binding->event);
		binding->has_coro_callback = true;
	}

	return binding->conn;
}

void redis_pool_maybe_release(zval *id)
{
	if (Z_TYPE_P(id) != IS_OBJECT) {
		return;
	}

	redis_object *redis = PHPREDIS_ZVAL_GET_OBJECT(redis_object, id);
	redis_async_pool *rp = redis->pool;

	if (rp == NULL || rp->bindings == NULL) {
		return;
	}

	zend_coroutine_t *coro = ZEND_ASYNC_CURRENT_COROUTINE;
	const zend_ulong coro_key = coro ? redis_pool_coro_key(coro) : 0;

	redis_pool_binding_t *binding = zend_hash_index_find_ptr(rp->bindings, coro_key);
	if (binding == NULL || binding->conn == NULL) {
		return;
	}

	if (redis_conn_is_pinned(rp, binding->conn)) {
		return;
	}

	zval conn_zval;
	ZVAL_PTR(&conn_zval, binding->conn);
	ZEND_ASYNC_POOL_RELEASE(rp->async_pool, &conn_zval);
	binding->conn = NULL;
}

zend_object *redis_pool_get_wrapper(redis_object *redis)
{
	redis_async_pool *rp = redis->pool;

	if (rp == NULL) {
		return NULL;
	}

	if (rp->wrapper == NULL) {
		rp->wrapper = ZEND_ASYNC_NEW_POOL_OBJ(rp->async_pool);
	}

	return rp->wrapper;
}

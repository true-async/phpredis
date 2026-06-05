/*
  TrueAsync connection pool for phpredis.
  See TRUE_ASYNC_POOL.md for the design and algorithm.

  The user-facing Redis object becomes a template: its own RedisSock is never
  opened. Physical connections live in a zend_async_pool_t. Two paths share
  them:
    - checkout: a coroutine borrows a private connection for a command burst or
      a pinned stateful sequence (MULTI/WATCH/SUBSCRIBE/SELECT) and returns it;
    - multiplex: stateless commands ride shared lanes (one socket, many
      coroutines) via an in-order reply pump. See the redis_mux_* section.
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
#include "php_streams.h"
#include "php_network.h"
#include "ext/standard/php_smart_string.h"
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif

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

/*
 * One in-flight multiplexed command: a Future the sending coroutine awaits,
 * resolved by the reply pump with the framed reply. Linked in the lane's FIFO
 * so reply order (the Redis wire guarantee) maps to waiter order.
 */
typedef struct _redis_mux_waiter {
	zend_future_t            *future;   /* awaitable (holds a ref); resolved with the framed reply */
	struct _redis_mux_waiter *next;
	bool                      abandoned;/* sender gone (cancelled): pump discards the reply */
	bool                      lane_failed;/* lane connection dropped: sender wakes and throws */
} redis_mux_waiter_t;

/*
 * One multiplex lane: a shared physical connection carrying many coroutines'
 * stateless commands with an in-order pending-reply FIFO. Opened eagerly at pool
 * construction; sock == NULL marks a lane whose connection has dropped (dead).
 */
typedef struct _redis_mux {
	redis_async_pool *pool;             /* owning pool (lane context for the pump) */
	RedisSock        *sock;             /* shared physical connection */
	php_socket_t      fd;               /* cached socket fd (for non-blocking recv) */
	redis_mux_waiter_t *head, *tail;    /* in-flight FIFO; order == reply order */
	uint32_t          in_flight;        /* queue depth: lane selection + backpressure */
	zend_async_poll_event_t *poll_ev;   /* one combined watch: READABLE while in-flight,
	                                       WRITABLE while out_buf has unsent bytes. A single
	                                       handle per fd (two would conflict in libuv). */
	async_poll_event  armed;            /* mask currently armed on poll_ev */
	smart_string      out_buf;          /* pending outbound bytes (batched) */
	smart_string      in_buf;           /* inbound bytes awaiting framing */
} redis_mux_t;

struct _redis_async_pool {
	zend_async_pool_t *async_pool;      /* physical RedisSock resources */
	HashTable         *bindings;        /* coro_key -> redis_pool_binding_t* */
	HashTable         *opts;            /* dup'd ctor options; factory re-applies */
	zend_object       *wrapper;         /* cached getPool() wrapper, released on destroy */
	long               db_default;      /* configured DB; drift pins the conn */
	uint32_t           mux_reserve;     /* connections reserved for multiplexing */
	redis_mux_t      **lanes;           /* mux lanes (opened at construction) */
	uint32_t           lane_count;      /* number of lanes (== configured mux) */
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

/* Binding for the current coroutine, or NULL if none (or no pool bindings). */
static redis_pool_binding_t *redis_pool_current_binding(const redis_async_pool *rp)
{
	if (rp->bindings == NULL) {
		return NULL;
	}

	zend_coroutine_t *coro = ZEND_ASYNC_CURRENT_COROUTINE;
	return zend_hash_index_find_ptr(rp->bindings, coro ? redis_pool_coro_key(coro) : 0);
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
static bool redis_conn_is_pinned(const redis_async_pool *rp, const RedisSock *sock)
{
	return !IS_ATOMIC(sock)
		|| sock->watching
		|| redis_pool_sock_subscribed(sock)
		|| sock->dbNumber != rp->db_default;
}

/* Extract the command name (first RESP bulk argument) from built command bytes.
 * The wire form is "*<argc>\r\n$<len>\r\n<NAME>\r\n...". Writes up to dst_sz-1
 * upper-cased bytes plus a NUL; returns the name length, or 0 when it cannot be
 * parsed (caller then treats the command as non-multiplexable). */
static size_t redis_cmd_name(const char *cmd, int cmd_len, char *dst, size_t dst_sz)
{
	const char *p = cmd, *end = cmd + cmd_len;

	if (p >= end || *p != '*') {
		return 0;
	}

	while (p < end && *p != '\n') {
		p++;
	}

	if (++p >= end || *p != '$') {
		return 0;
	}

	p++;
	long len = 0;
	while (p < end && *p >= '0' && *p <= '9') {
		len = len * 10 + (*p - '0');
		p++;
	}

	if (len <= 0 || (size_t)len >= dst_sz || p + 2 + len > end
		|| p[0] != '\r' || p[1] != '\n') {
		return 0;
	}

	p += 2;
	for (long i = 0; i < len; i++) {
		const char c = p[i];
		dst[i] = (c >= 'a' && c <= 'z') ? (char)(c - 32) : c;
	}

	dst[len] = '\0';
	return (size_t)len;
}

/* True when a built command may ride the shared multiplexed socket. Commands
 * that open a stateful sequence, block, or rebind the connection are excluded
 * and must take a private checkout connection instead. */
bool redis_cmd_is_multiplexable(const char *cmd, int cmd_len)
{
	static const char *const blocklist[] = {
		"MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
		"SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE",
		"SSUBSCRIBE", "SUNSUBSCRIBE",
		"BLPOP", "BRPOP", "BLMOVE", "BRPOPLPUSH", "BLMPOP",
		"BZPOPMIN", "BZPOPMAX", "BZMPOP",
		"WAIT", "WAITAOF", "SELECT", "SWAPDB", "MONITOR", "RESET",
		NULL
	};

	char name[24];
	if (redis_cmd_name(cmd, cmd_len, name, sizeof(name)) == 0) {
		return false;
	}

	for (size_t i = 0; blocklist[i] != NULL; i++) {
		if (strcmp(name, blocklist[i]) == 0) {
			return false;
		}
	}

	return true;
}

/*
 * RESP frame-boundary scanner (non-blocking, partial-frame safe).
 *
 * Used by the multiplex reply pump to detect a complete reply in a buffer
 * without materializing it: the pump only needs the byte boundary, the resumed
 * coroutine then runs phpredis's normal parser over the framed bytes.
 */

/* Offset just past the first \r\n at or after `from`, or 0 if no complete line. */
static size_t resp_line_end(const char *buf, size_t len, size_t from)
{
	for (size_t i = from; i + 1 < len; i++) {
		if (buf[i] == '\r' && buf[i + 1] == '\n') {
			return i + 2;
		}
	}

	return 0;
}

/* Parse the (possibly negative) integer header between `from` and \r\n. On
 * success sets *out and returns the offset past \r\n; 0 if the line is partial. */
static size_t resp_int_line(const char *buf, size_t len, size_t from, long *out)
{
	size_t end = resp_line_end(buf, len, from);
	if (end == 0) {
		return 0;
	}

	long sign = 1, n = 0;
	size_t i = from;

	if (buf[i] == '-') {
		sign = -1;
		i++;
	} else if (buf[i] == '+') {
		i++;
	}

	for (; i + 2 <= end; i++) {
		const char c = buf[i];
		if (c < '0' || c > '9') {
			break;
		}
		n = n * 10 + (c - '0');
	}

	*out = sign * n;
	return end;
}

/* Length of one complete RESP value at buf[0..len), or 0 if more bytes are
 * needed. Handles RESP2 and RESP3 (maps/sets/push/attributes); an attribute is
 * transparent — it is consumed together with the reply it prefixes. */
static size_t resp_scan(const char *buf, size_t len)
{
	if (len < 1) {
		return 0;
	}

	switch (buf[0]) {
		case '+': case '-': case ':': case '_': case ',': case '#': case '(':
			return resp_line_end(buf, len, 1);

		case '$': case '=': {
			long n;
			size_t hdr = resp_int_line(buf, len, 1, &n);
			if (hdr == 0) {
				return 0;
			}
			if (n < 0) {
				return hdr;
			}
			size_t total = hdr + (size_t)n + 2;
			return total <= len ? total : 0;
		}

		case '*': case '~': case '>': case '%': case '|': {
			long n;
			size_t off = resp_int_line(buf, len, 1, &n);
			if (off == 0) {
				return 0;
			}
			if (n < 0) {
				return off;
			}

			size_t elems = (buf[0] == '%' || buf[0] == '|') ? (size_t)n * 2 : (size_t)n;
			for (size_t e = 0; e < elems; e++) {
				size_t c = resp_scan(buf + off, len - off);
				if (c == 0) {
					return 0;
				}
				off += c;
			}

			if (buf[0] == '|') {
				size_t c = resp_scan(buf + off, len - off);
				if (c == 0) {
					return 0;
				}
				off += c;
			}

			return off;
		}

		default:
			return resp_line_end(buf, len, 1);
	}
}

size_t redis_resp_frame_len(const char *buf, size_t len)
{
	return resp_scan(buf, len);
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

/* Build a fresh, opened connection by replaying the constructor options.
 * Returns a READY RedisSock, or NULL on failure. The pool owns its lifetime, so
 * persistence is forced off. Shared by the checkout factory and the mux lanes. */
static RedisSock *redis_pool_connect(const redis_async_pool *rp)
{
	RedisSock *sock = redis_sock_create(ZEND_STRL("127.0.0.1"), 6379, 0, 0, 0, NULL, 0);
	if (sock == NULL) {
		return NULL;
	}

	if (rp->opts != NULL && redis_sock_configure(sock, rp->opts) != SUCCESS) {
		redis_pool_free_conn(sock);
		return NULL;
	}

	sock->persistent = 0;

	if (redis_sock_server_open(sock) != SUCCESS) {
		redis_pool_free_conn(sock);
		return NULL;
	}

	return sock;
}

/* Tear down a multiplex lane: stop poll events, drain any leftover waiters,
 * drop the connection, free buffers. */
static void redis_mux_lane_free(redis_mux_t *lane)
{
	if (lane == NULL) {
		return;
	}

	if (lane->poll_ev != NULL) {
		if (lane->armed != 0) {
			lane->poll_ev->base.stop(&lane->poll_ev->base);
		}
		lane->poll_ev->base.dispose(&lane->poll_ev->base);
	}

	/* Drain any leftover waiters (e.g. abandoned ones whose reply never arrived)
	 * so their futures are released and nodes freed. */
	redis_mux_waiter_t *w = lane->head;
	while (w != NULL) {
		redis_mux_waiter_t *next = w->next;
		ZEND_ASYNC_EVENT_RELEASE(&w->future->event);
		efree(w);
		w = next;
	}

	smart_string_free(&lane->out_buf);
	smart_string_free(&lane->in_buf);
	redis_pool_free_conn(lane->sock);
	efree(lane);
}

/*
 * Multiplex command path (v0: N lanes, plain TCP — see TRUE_ASYNC_POOL.md §9b).
 */

/* Underlying socket fd of a lane (for non-blocking recv in the pump). */
static php_socket_t redis_mux_fd(RedisSock *sock)
{
	php_socket_t fd = -1;

	if (sock->stream == NULL) {
		return -1;
	}

	php_stream_cast(sock->stream, PHP_STREAM_AS_FD_FOR_SELECT | PHP_STREAM_CAST_INTERNAL,
		(void *)&fd, 0);
	return fd;
}

/* Re-arm the lane's single poll handle to what it currently needs: READABLE
 * while replies are pending, WRITABLE while bytes await sending. One handle per
 * fd — two (a separate read and write watch) would conflict in libuv. */
static void redis_mux_update_poll(redis_mux_t *lane)
{
	async_poll_event want = 0;
	if (lane->in_flight > 0) {
		want |= ASYNC_READABLE;
	}
	if (lane->out_buf.len > 0) {
		want |= ASYNC_WRITABLE;
	}

	if (want == lane->armed) {
		return;
	}

	if (lane->armed != 0) {
		lane->poll_ev->base.stop(&lane->poll_ev->base);
	}
	if (want != 0) {
		lane->poll_ev->events = want;
		lane->poll_ev->base.start(&lane->poll_ev->base);
	}
	lane->armed = want;
}

/* Send as much of out_buf as the socket accepts right now (non-blocking, never
 * parks). Any leftover stays buffered for the WRITABLE drain. */
static void redis_mux_flush(redis_mux_t *lane)
{
	while (lane->out_buf.len > 0) {
		const ssize_t n = send(lane->fd, lane->out_buf.c, lane->out_buf.len, MSG_DONTWAIT);
		if (n <= 0) {
			break;   /* EAGAIN/error: retry when the socket is writable again */
		}
		if ((size_t)n < lane->out_buf.len) {
			memmove(lane->out_buf.c, lane->out_buf.c + (size_t)n, lane->out_buf.len - (size_t)n);
		}
		lane->out_buf.len -= (size_t)n;
	}
}

/* A lane's connection dropped (peer close or hard error). Recovery is out of
 * scope for v0 (fail-fast): stop the poll, fail every still-pending sender so it
 * wakes and throws (the waiters stay in the FIFO and are freed at pool destroy —
 * the lane outlives them), and drop the dead connection. The lane struct itself
 * is kept (sock == NULL marks it dead) until the pool is destroyed. */
static void redis_mux_lane_kill(redis_mux_t *lane)
{
	if (lane->armed != 0) {
		lane->poll_ev->base.stop(&lane->poll_ev->base);   /* safe inside its own callback */
		lane->armed = 0;
	}

	for (redis_mux_waiter_t *w = lane->head; w != NULL; w = w->next) {
		if (w->abandoned) {
			continue;   /* sender already gone; freed at destroy */
		}

		zval nul;
		ZVAL_NULL(&nul);
		w->lane_failed = true;
		ZEND_FUTURE_COMPLETE(w->future, &nul);   /* wake sender; it throws */
	}

	redis_pool_free_conn(lane->sock);
	lane->sock = NULL;
	lane->fd = -1;
}

/* Drain complete replies (non-blocking) and resolve waiters' futures FIFO.
 * Returns false if the connection dropped (the lane was killed). */
static bool redis_mux_drain(redis_mux_t *lane)
{
	char tmp[16384];
	bool died = false;

	for (;;) {
		const ssize_t n = recv(lane->fd, tmp, sizeof(tmp), MSG_DONTWAIT);
		if (n > 0) {
			smart_string_appendl(&lane->in_buf, tmp, (size_t)n);
			if ((size_t)n < sizeof(tmp)) {
				break;
			}
			continue;
		}
		if (n == 0) {
			died = true;   /* peer closed the connection */
		} else if (errno != EAGAIN && errno != EWOULDBLOCK) {
			died = true;   /* hard error */
		}
		break;
	}

	size_t off = 0;
	while (lane->head != NULL) {
		const size_t flen = redis_resp_frame_len(lane->in_buf.c + off, lane->in_buf.len - off);
		if (flen == 0) {
			break;
		}

		redis_mux_waiter_t *w = lane->head;
		lane->head = w->next;
		if (lane->head == NULL) {
			lane->tail = NULL;
		}
		lane->in_flight--;

		/* Deliver the reply unless the sender was cancelled; either way the
		 * bytes are consumed in order so the stream stays in sync. */
		if (!w->abandoned) {
			zval frame;
			ZVAL_STR(&frame, zend_string_init(lane->in_buf.c + off, flen, 0));
			ZEND_FUTURE_COMPLETE(w->future, &frame);
			zval_ptr_dtor(&frame);
		}

		ZEND_ASYNC_EVENT_RELEASE(&w->future->event);   /* the waiter's future ref */
		efree(w);

		off += flen;
	}

	if (off > 0) {
		if (off < lane->in_buf.len) {
			memmove(lane->in_buf.c, lane->in_buf.c + off, lane->in_buf.len - off);
		}
		lane->in_buf.len -= off;
	}

	if (died) {
		redis_mux_lane_kill(lane);   /* fail the rest; lane is now dead */
		return false;
	}

	return true;
}

/* Combined reactor I/O callback: send what we can, drain what arrived, then
 * re-arm the poll for whatever is still outstanding. Runs between coroutines. */
static void redis_mux_io(zend_async_event_t *event, zend_async_event_callback_t *callback,
	void *result, zend_object *exception)
{
	redis_mux_t *lane = *(redis_mux_t **)((char *)event + event->extra_offset);
	const async_poll_event triggered = ((zend_async_poll_event_t *)event)->triggered_events;

	if (triggered & ASYNC_WRITABLE) {
		redis_mux_flush(lane);
	}
	if (triggered & ASYNC_READABLE) {
		if (!redis_mux_drain(lane)) {
			return;   /* lane died and was killed; do not re-arm */
		}
	}

	redis_mux_update_poll(lane);
}

/* Open one multiplex lane: a fresh connection (replaying the ctor options) plus a
 * combined poll event wired to the I/O callback (armed on demand). Called eagerly
 * at pool construction — the only place a connect may block/suspend — so the
 * command path never opens a lane (and never suspends to connect). Returns NULL
 * on failure; the constructor then fails fast. */
static redis_mux_t *redis_mux_lane_open(redis_async_pool *rp)
{
	RedisSock *sock = redis_pool_connect(rp);
	if (sock == NULL) {
		return NULL;
	}

	redis_mux_t *lane = ecalloc(1, sizeof(*lane));
	lane->pool = rp;
	lane->sock = sock;
	lane->fd = redis_mux_fd(sock);

	zend_async_poll_event_t *ev =
		ZEND_ASYNC_NEW_SOCKET_EVENT_EX(lane->fd, ASYNC_READABLE, sizeof(redis_mux_t *));
	if (ev == NULL) {
		redis_mux_lane_free(lane);
		return NULL;
	}

	*(redis_mux_t **)((char *)&ev->base + ev->base.extra_offset) = lane;
	ev->base.add_callback(&ev->base, ZEND_ASYNC_EVENT_CALLBACK(redis_mux_io));
	lane->poll_ev = ev;   /* armed lazily via redis_mux_update_poll */

	return lane;
}

/* Pick the live lane with the fewest in-flight replies. Dead lanes (a dropped
 * connection, sock == NULL) are skipped; NULL means every lane is dead. */
static redis_mux_t *redis_mux_pick(redis_async_pool *rp)
{
	redis_mux_t *best = NULL;

	for (uint32_t i = 0; i < rp->lane_count; i++) {
		redis_mux_t *l = rp->lanes[i];
		if (l == NULL || l->sock == NULL) {
			continue;
		}
		if (best == NULL || l->in_flight < best->in_flight) {
			best = l;
		}
	}

	return best;
}

static void redis_mux_materialize(redis_mux_t *lane, zend_string *frame,
	FailableResultCallback resp_cb, void *ctx, INTERNAL_FUNCTION_PARAMETERS);

/* Suspend the current coroutine until the pump resolves `future`, then
 * materialize the reply. Ownership: the future is resolved (not freed) by the
 * wake callback, which delivers the reply into the coroutine's waker
 * (waker->result) — we read it there, not from future->result. resume_when
 * borrows the future; waker_clean releases that borrow and the result. The
 * future's own ref is owned by the waiter and released by the pump.
 * Returns true on a delivered reply, false if it was not delivered — the sender
 * was cancelled (see w->abandoned) or the lane died (see w->lane_failed). */
static bool redis_mux_await(redis_mux_t *lane, zend_future_t *future,
	FailableResultCallback resp_cb, void *ctx, INTERNAL_FUNCTION_PARAMETERS)
{
	zend_coroutine_t *coro = ZEND_ASYNC_CURRENT_COROUTINE;

	ZEND_FUTURE_SET_USED(future);
	ZEND_FUTURE_SET_EXCEPTION_CAUGHT(future);

	ZEND_ASYNC_WAKER_NEW(coro);
	zend_async_resume_when(coro, &future->event, false,
		zend_async_waker_callback_resolve, NULL);

	ZEND_ASYNC_SUSPEND();

	const bool delivered = EG(exception) == NULL
		&& coro->waker != NULL
		&& Z_TYPE(coro->waker->result) == IS_STRING;

	if (delivered) {
		redis_mux_materialize(lane, Z_STR(coro->waker->result), resp_cb, ctx,
			INTERNAL_FUNCTION_PARAM_PASSTHRU);
	}

	zend_async_waker_clean(coro);
	return delivered;
}

/* Materialize a framed reply into return_value by feeding it to the ordinary
 * (atomic) phpredis parser through a read-only memory stream. */
static void redis_mux_materialize(redis_mux_t *lane, zend_string *frame,
	FailableResultCallback resp_cb, void *ctx, INTERNAL_FUNCTION_PARAMETERS)
{
	php_stream *real = lane->sock->stream;
	php_stream *mem = php_stream_memory_open(TEMP_STREAM_READONLY, frame);

	lane->sock->stream = mem;
	resp_cb(INTERNAL_FUNCTION_PARAM_PASSTHRU, lane->sock, NULL, ctx);
	lane->sock->stream = real;

	php_stream_close(mem);
}

bool redis_pool_should_mux(redis_object *redis)
{
	const redis_async_pool *rp = redis->pool;
	if (rp == NULL || rp->lane_count == 0) {
		return false;
	}

	if (ZEND_ASYNC_CURRENT_COROUTINE == NULL) {
		return false;   /* mux needs a coroutine to suspend on the reply */
	}

	/* Skip mux while the coroutine holds a pinned checkout connection
	 * (mid MULTI/WATCH/SUBSCRIBE) — that sequence stays on its private conn. */
	const redis_pool_binding_t *binding = redis_pool_current_binding(rp);
	return binding == NULL || binding->conn == NULL;
}

void redis_mux_dispatch(redis_object *redis, char *cmd, int cmd_len,
	FailableResultCallback resp_cb, void *ctx, INTERNAL_FUNCTION_PARAMETERS)
{
	redis_async_pool *rp = redis->pool;
	redis_mux_t *lane = redis_mux_pick(rp);
	if (UNEXPECTED(lane == NULL)) {
		REDIS_THROW_EXCEPTION("Redis pool: multiplex lane unavailable", 0);
		efree(cmd);
		RETURN_FALSE;
	}

	/* The future's single ref belongs to the waiter; the pump releases it after
	 * delivering or discarding the reply (or pool destroy does, for a dead lane).
	 * resume_when borrows it for the await. */
	zend_future_t *future = ZEND_ASYNC_NEW_FUTURE(false);

	redis_mux_waiter_t *w = ecalloc(1, sizeof(*w));
	w->future = future;

	/* Queue the command and register the waiter in one synchronous step — no yield
	 * between, so FIFO order == wire order. The sender does no socket I/O: it only
	 * arms the poll; the reactor callback owns all send/recv. */
	if (lane->tail != NULL) {
		lane->tail->next = w;
	} else {
		lane->head = w;
	}
	lane->tail = w;
	lane->in_flight++;

	smart_string_appendl(&lane->out_buf, cmd, cmd_len);
	efree(cmd);
	redis_mux_update_poll(lane);   /* arm READABLE (reply pending) + WRITABLE (bytes to send) */

	if (!redis_mux_await(lane, future, resp_cb, ctx, INTERNAL_FUNCTION_PARAM_PASSTHRU)) {
		if (w->lane_failed) {
			/* The lane's connection dropped while we were parked (the pump resolved
			 * the future and killed the lane). The waiter stays queued and is freed
			 * at pool destroy; surface the failure. */
			REDIS_THROW_EXCEPTION("Redis pool: multiplex lane connection lost", 0);
			RETURN_FALSE;
		}

		/* Sender cancelled before the reply arrived: the waiter is still queued.
		 * Abandon it so the pump discards the reply (in order) and frees it.
		 * EG(exception) (the cancellation) propagates. */
		w->abandoned = true;
	}
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

	RedisSock *sock = redis_pool_connect(rp);
	if (UNEXPECTED(sock == NULL)) {
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
	const redis_async_pool *rp = (const redis_async_pool *)pool->user_data;
	RedisSock *sock = Z_PTR_P(resource);

	if (sock == NULL || sock->stream == NULL || sock->status == REDIS_SOCK_STATUS_FAILED) {
		return false;
	}

	/* Recycle only clean connections; drop anything still stateful. */
	return rp == NULL || !redis_conn_is_pinned(rp, sock);
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

	/* Pre-open all multiplex lanes here, in the constructor — the one place a
	 * connect may block/suspend. The command path then never opens (or waits on)
	 * a lane. Fail fast: any failure tears down the whole pool. Recovery of a lane
	 * that drops later is out of scope for v0 (see TRUE_ASYNC_POOL.md §9b). */
	if (mux > 0) {
		rp->lane_count = (uint32_t)mux;
		rp->lanes = ecalloc(rp->lane_count, sizeof(redis_mux_t *));

		for (uint32_t i = 0; i < rp->lane_count; i++) {
			rp->lanes[i] = redis_mux_lane_open(rp);

			if (UNEXPECTED(rp->lanes[i] == NULL)) {
				redis_pool_destroy(redis);
				REDIS_THROW_EXCEPTION("Redis pool: failed to open multiplex lane", 0);
				return FAILURE;
			}
		}
	}

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

	if (rp->lanes != NULL) {
		for (uint32_t i = 0; i < rp->lane_count; i++) {
			redis_mux_lane_free(rp->lanes[i]);
		}
		efree(rp->lanes);
		rp->lanes = NULL;
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

	if (rp == NULL) {
		return;
	}

	redis_pool_binding_t *binding = redis_pool_current_binding(rp);
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

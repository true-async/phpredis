# TrueAsync pool tests

Tests for the phpredis connection pool under TrueAsync. Format: `.phpt` (like
`php-src/ext/async/tests`), coroutines via `Async\spawn` / `Async\await`. NOT the
synchronous `tests/TestRedis.php` framework.

Status: **executable specification**. Tests that depend on already-implemented
behaviour pass; those depending on not-yet-implemented parts (e.g. `getPool()`)
fail until those land.

## Running

Needs a php-src build with `--enable-zts` and the TrueAsync reactor, phpredis
built against it, and a live Redis (default `127.0.0.1:6379`, overridable via
`REDIS_TEST_HOST` / `REDIS_TEST_PORT`).

```sh
REDIS_TEST_HOST=127.0.0.1 REDIS_TEST_PORT=6379 \
  php run-tests.php -p $(which php) tests/async
```

The helper `inc/async_redis_pool_test.inc` provides `skipIf*` and `poolFactory()`.

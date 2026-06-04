# TrueAsync pool tests

Тесты пула соединений phpredis под TrueAsync. Формат — `.phpt` (как
`php-src/ext/async/tests`), корутины через `Async\spawn` / `Async\await`.
НЕ синхронный `tests/TestRedis.php`.

Статус: **исполняемая спецификация**. Фича из `TRUE_ASYNC_POOL.md` ещё не
реализована — пока тесты падают/скипаются. По мере реализации Stage 1/2 должны
зеленеть.

## Запуск

Нужен build php-src с `--enable-zts` и подключённым TrueAsync-реактором +
собранный phpredis против него, и живой Redis (по умолчанию `127.0.0.1:6379`,
переопределяется `REDIS_TEST_HOST` / `REDIS_TEST_PORT`).

```sh
REDIS_TEST_HOST=127.0.0.1 REDIS_TEST_PORT=6379 \
  php run-tests.php -p $(which php) tests/async
```

Хелпер `inc/async_redis_pool_test.inc` даёт `skipIf*` и `poolFactory()`.

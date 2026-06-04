# TrueAsync Connection Pool для phpredis — план разработки и алгоритм

Статус: **дизайн-документ**. Код ещё не написан. Тесты в `tests/async/` —
исполняемая спецификация (пока не проходят).

Автор: Edmond. Цель: дать phpredis прозрачный пул соединений под TrueAsync,
повторяя проверенную модель PDO-пула (`php-src/ext/pdo/pdo_pool.{c,h}`) и
добавляя поверх неё мультиплекс — «лучшее из двух миров».

---

## 0. Контекст и предпосылки

- Ветка `true-async` форка phpredis сейчас почти пустая (только PHP 8.6-шимы в
  `common.h`). Весь I/O идёт через `php_stream`.
- Под TrueAsync блокирующее чтение `php_stream` **внутри корутины паркуется
  автоматически** через реактор. Значит существующий синхронный код phpredis
  («write команду → blocking read ответа») **корректно работает в модели
  "одно соединение на корутину"** без хирургии в command-path.
- Опасность только одна: один `RedisSock` нельзя использовать из двух корутин
  одновременно — общие `stream`, `reply_callback`, `pipeline_cmd`, `mode`
  перемешают команды/ответы. Нужна координация.

### Почему пул для Redis — отдельный вопрос

В отличие от SQL, Redis отвечает на одном соединении **строго в порядке команд**
(in-order RESP). Поэтому для Redis технически возможны ДВЕ модели, и индустрия
раскололась пополам:

| Модель | Кто | Суть |
|---|---|---|
| Мультиплекс (1 общий сокет) | Lettuce, StackExchange.Redis, redis-rs `MultiplexedConnection`, ioredis | Все корутины пишут в один сокет, отдельный reader разбирает ответы FIFO. Автопайплайнинг даром. |
| Пул соединений (checkout) | Jedis, go-redis, redis-py, deadpool | N соединений, корутина берёт одно эксклюзивно. Как наш PDO-пул. |

Часть команд **нельзя** мультиплексировать (захватывают соединение целиком):
`MULTI/EXEC`+`WATCH`, `SUBSCRIBE/PSUBSCRIBE`, блокирующие `BLPOP/BRPOP/WAIT`,
`pipeline()`, `SELECT`. Поэтому все «мультиплекс»-клиенты на деле **гибридны**:
мультиплекс для stateless request/reply + выделенное соединение для остального.

**Почему мультиплекс важен под TrueAsync.** Redis-сервер однопоточный. При пуле
из 10 соединений и 1000 корутин 990 паркуются в ожидании. При мультиплексе все
1000 команд льются в один сокет, пайплайнятся, сервер молотит их подряд без
простоя на RTT. Для кэш-нагрузки (куча мелких GET/SET) — разница в разы.

---

## 1. Целевая архитектура (гибрид)

Пул из `max` физических соединений делится на две группы:

```
            ┌──────────────────────── zend_async_pool_t ───────────────────────┐
            │                                                                   │
   корутина │   ┌─ mux-резерв (mux=N) ─┐      ┌─── checkout-пул (max - N) ───┐  │
   ─────────┼──>│  shared socket #1     │      │  conn  conn  conn  conn ...  │  │
   stateless│   │  shared socket #2     │      │  (персонально, эксклюзивно)  │  │
            │   │  (общая FIFO-очередь) │      └──────────────────────────────┘  │
            │   └───────────────────────┘                ▲                       │
   корутина │            ▲                                │                       │
   ─────────┼────────────┘                       stateful: MULTI/WATCH/SUB/      │
   stateful │     stateless: GET/SET/...          BLPOP/SELECT/pipeline          │
            └───────────────────────────────────────────────────────────────────┘
```

- **mux-резерв** (`mux=2..3`): эти соединения **никому не выдаются персонально**.
  Их обслуживает общая очередь запросов — Stage 2. Stateless-команды идут сюда.
- **checkout-пул** (остальные): выдаются корутине эксклюзивно на время stateful
  работы — Stage 1. Это в точности «логика транзакций» из PDO-пула.

Решение «куда пойдёт команда» принимается в одной точке (`redis_sock_get`),
по типу команды и текущему состоянию соединения корутины.

### Две стадии разработки

- **Stage 1 — Checkout-пул** («шарить сокет по очереди во времени»). Прямой порт
  `pdo_pool`. Корутина берёт физический `RedisSock` эксклюзивно, держит во время
  burst-а команд / транзакции, возвращает в пул. Корректно для ВСЕХ типов команд
  сразу. Низкий риск. Самодостаточно и отгружаемо.
- **Stage 2 — Multiplex-очередь** («делать очередь»). Поверх Stage 1: reader-
  корутина + FIFO reply-router на mux-резерве. Stateless-команды уходят сюда,
  stateful — остаются на checkout. Это и есть полный гибрид Lettuce.

Stage 1 — фундамент: выделенные соединения для blocking/pubsub/txn нужны в
гибриде в любом случае.

---

## 2. Структуры данных

Зеркалят `pdo_pool_binding_t` / поля `pdo_dbh_t`.

```c
/* redis_pool.h */

/* Per-coroutine привязка. Аналог pdo_pool_binding_t. */
typedef struct _redis_pool_binding {
    zend_async_event_callback_t event;   /* коллбэк на финал корутины */
    RedisSock  *conn;                    /* выданный физический conn, или NULL */
    zend_ulong  coro_key;
    bool        has_coro_callback;
} redis_pool_binding_t;

/* Состояние пула, висит на объекте Redis (template). */
typedef struct _redis_pool {
    zend_async_pool_t *async_pool;       /* физические RedisSock внутри */
    HashTable         *bindings;         /* coro_key -> redis_pool_binding_t */
    zend_object       *wrapper;          /* PHP-обёртка пула (getPool()) */

    /* конфиг фабрики: копия connect-параметров шаблона */
    zend_string *host; int port;
    zend_string *user; zend_string *pass;
    double timeout, read_timeout;
    long   db_default;                   /* SELECT по умолчанию для каждого conn */
    /* ... serializer/compression/prefix — клиентские, одинаковы для всех ... */

    uint32_t mux_reserve;                /* Stage 2: соединений под мультиплекс */
    /* Stage 2: redis_mux_t *mux; (см. §5) */
} redis_pool;
```

Объект `Redis` в pool-режиме — **шаблон**: его собственный `RedisSock *sock`
не подключён (или NULL), а реальные соединения живут в `async_pool`. Точно как
`pdo_dbh_t.driver_data == NULL` в PDO-пуле.

Физическое соединение = полноценный `RedisSock` от фабрики (host/port/auth/db
идентичны для всех), со своим `php_stream`.

---

## 3. Точки интеграции (минимальная хирургия)

phpredis уже даёт два чокпойнта — через них проходит почти каждая команда:

- `redis_sock_get(zval *id, int nothrow)` — `library.c`, возвращает `RedisSock*`.
- `redis_process_cmd()` / `redis_process_kw_cmd()` — `redis.c:674/708`. Обе:
  1. `redis_sock = redis_sock_get(getThis(), 0);`
  2. строят команду, пишут, читают ответ;
  3. `if (IS_ATOMIC(redis_sock)) resp_cb(...); else буферизуют (MULTI/PIPELINE)`.

**Интеграция Stage 1 — ровно две правки:**

1. `redis_sock_get()` делаем pool-aware:
   ```c
   RedisSock *redis_sock_get(zval *id, int nothrow) {
       redis_object *obj = ...;
       if (obj->pool == NULL) return obj->sock;        /* как сегодня */
       return redis_pool_acquire_conn(obj);            /* per-coro checkout */
   }
   ```
2. В хвост `redis_process_cmd` / `redis_process_kw_cmd` (после обработки ответа)
   добавить `redis_pool_maybe_release(getThis())`.

Команды, минующие эти функции (subscribe, multi/exec, raw, pipeline) — это
**ровно stateful-команды, которые пиннят соединение**, поэтому они естественно
держат conn до завершения. Отдельной обработки для Stage 1 не требуют.

---

## 4. Алгоритм Stage 1 — Checkout-пул

### 4.1. Acquire (порт `pdo_pool_acquire_conn`)

```
redis_pool_acquire_conn(obj):
    coro_key = current_coroutine_key()        # 0 если вне корутины
    binding  = obj->pool->bindings[coro_key]

    if binding && binding->conn:
        if binding->conn не сломан: return binding->conn   # reuse в этой корутине
        else: detach (release если refcount==0), binding->conn = NULL

    resource = ZEND_ASYNC_POOL_ACQUIRE(async_pool, timeout)   # ПАРКУЕТ если пусто
    if !resource: return NULL                                  # ошибка/таймаут

    if !binding:
        binding = ecalloc(...)
        binding->event.callback = redis_pool_on_coroutine_finish
        binding->event.dispose  = redis_pool_binding_dispose
        binding->coro_key = coro_key
        bindings[coro_key] = binding
        coro->event.add_callback(coro, &binding->event)   # release на финале корутины

    binding->conn = resource
    return binding->conn
```

### 4.2. Pin-предикат (аналог PDO `in_txn`)

Соединение НЕЛЬЗЯ вернуть в пул, пока оно «грязное» (stateful):

```c
static bool redis_conn_is_pinned(RedisSock *s) {
    return !IS_ATOMIC(s)                 /* идёт буферизация MULTI или PIPELINE */
        || s->watching                   /* WATCH — оптимистическая блокировка */
        || redis_sock_is_subscribed(s)   /* pub/sub режим */
        || s->dbNumber != pool->db_default; /* SELECT увёл с дефолтной БД */
    /* блокирующие (BLPOP) флага не требуют: корутина запаркована ВНУТРИ вызова,
       держа conn, release между командами для них не вызывается. */
}
```

### 4.3. Maybe-release (порт `pdo_pool_maybe_release`)

```
redis_pool_maybe_release(obj):
    binding = obj->pool->bindings[current_coro_key]
    if !binding || !binding->conn: return
    if redis_conn_is_pinned(binding->conn): return        # держим за корутиной
    ZEND_ASYNC_POOL_RELEASE(async_pool, binding->conn)
    binding->conn = NULL
```

Вызывается в хвосте `redis_process_cmd`/`_kw_cmd`. Итог: между обычными
командами соединение возвращается в пул (как PDO между statement-ами), а во
время MULTI/WATCH/SUBSCRIBE/чужой-БД — пиннится за корутиной.

### 4.4. Финал корутины (порт `pdo_pool_binding_on_coroutine_finish`)

Когда корутина завершается, её зарегистрированный коллбэк:
- если `binding->conn != NULL` — сбрасывает состояние и `RELEASE` обратно в пул
  (страховка от утечки соединения, если корутина умерла в транзакции/подписке);
- освобождает binding.

### 4.5. Коллбэки пула (`ZEND_ASYNC_NEW_POOL`)

Зеркалят PDO-фабрику:

- `factory`     → `redis_sock_create` + `redis_sock_connect` + AUTH + `SELECT
  db_default` + readonly/HELLO. Один conn = одна реальная TCP/UDS-сессия.
- `destructor`  → disconnect + free `RedisSock`.
- `healthcheck` → liveness (`redis_stream_liveness_check` /
  `redis_stream_detect_dirty`), опц. PING.
- `before_acquire` → опц. проверка живости перед выдачей.
- `before_release` → **очистка состояния** (страховка): если в MULTI — DISCARD;
  watching — UNWATCH; subscribed — UNSUBSCRIBE/RESET; `dbNumber != default` —
  SELECT обратно; очистить `pipeline_cmd`. Непрочитанные байты в сокете → conn
  битый (не возвращать в пул).

### 4.6. Корректность connection-scoped состояния

Только серверное session-состояние течёт между заёмщиками: `SELECT`, `WATCH`,
`MULTI`, `SUBSCRIBE`, `CLIENT SETNAME/TRACKING`. Клиентское (serializer, prefix,
compression) одинаково на всех conn (ставится фабрикой) — не течёт.

Правило v1: фабрика выставляет `db_default`; runtime-`SELECT` на другую БД
делает conn pinned (через предикат §4.2) — он не вернётся в общий пул, пока БД
не дефолтная. Это просто и корректно. (Альтернатива — сброс на release; выбрано
pinning как менее «болтливое».)

---

## 5. Алгоритм Stage 2 — Multiplex-очередь

Включается, когда `mux_reserve > 0`. Цель — stateless-команды многих корутин
гонять по 1–3 общим сокетам с автопайплайнингом.

### 5.1. Классификация команды

```c
static bool redis_cmd_is_multiplexable(RedisSock *s /*текущее состояние*/, cmd) {
    if (!IS_ATOMIC(s) || s->watching || redis_sock_is_subscribed(s)) return false;
    if (cmd ∈ {SUBSCRIBE,PSUBSCRIBE,SSUBSCRIBE,MULTI,WATCH,
               BLPOP,BRPOP,BLMOVE,BRPOPLPUSH,BLMPOP,BZPOPMIN,BZPOPMAX,
               WAIT,WAITAOF,SELECT,SWAPDB,MONITOR,...}) return false;
    return true;
}
```

### 5.2. Диспетчер (расширение `redis_sock_get` в гибриде)

```
redis_sock_get(obj):
    if !pool: return obj->sock
    binding = bindings[coro_key]
    if binding && binding->conn: return binding->conn        # уже пиннут (checkout)
    if mux && redis_cmd_is_multiplexable(...): return MUX_SENTINEL  # см. §5.3
    return redis_pool_acquire_conn(obj)                      # checkout
```

### 5.3. Reply-router (сердце Stage 2)

Один общий сокет обслуживается так:

- **Запись.** Корутина сериализует свою команду (полный RESP-фрейм) и пишет в
  общий сокет под write-«батоном» (по границам команд писать безопасно). Можно
  батчить несколько фреймов перед flush — это и есть автопайплайнинг.
- **Учёт.** Команда кладёт «ожидающего» в FIFO сокета: `{coro, resp_cb, ctx}`.
- **Чтение.** На сокет назначена единственная **reader-корутина**. Она парсит
  ответы строго по порядку; на каждый ответ снимает голову FIFO, выполняет её
  `resp_cb` от имени originator-а и **резюмит** запаркованную корутину с готовым
  zval.
- Корутина-отправитель после записи паркуется на своём awaitable и просыпается,
  когда reader отдал её ответ.

То есть в mux-режиме `redis_read_reply` у вызывающей корутины заменяется на
«enqueue waiter + suspend», а фактическое чтение делает reader. `fold_item`/
`reply_callback` на `RedisSock` сейчас per-socket и предполагают чтение в
контексте вызывающего — в mux их исполняет reader от имени originator-а. Это
основной объём работы Stage 2.

### 5.4. Деградация в checkout

Если команда не мультиплексируема (§5.1) или mux-сокет помечен битым — корутина
прозрачно уходит на `redis_pool_acquire_conn` (Stage 1). Stateful-сценарий
(MULTI/SUB/BLPOP) всегда на персональном соединении.

---

## 6. PHP-level API

Прозрачный пул (как PDO: один объект, шарится между корутинами), конфиг через
опции конструктора `Redis` (phpredis 6 уже принимает ассоц-массив):

```php
$redis = new Redis([
    'host' => '127.0.0.1',
    'port' => 6379,
    'auth' => ['user', 'pass'],
    'pool' => [
        'enabled' => true,
        'min'     => 0,     // прогрев
        'max'     => 16,    // всего физических соединений
        'mux'     => 2,     // Stage 2: резерв под мультиплекс (0 = выкл)
    ],
]);

// Дальше — обычный phpredis. Прозрачно для пользователя:
Async\spawn(fn() => $redis->get('a'));   // mux fast-path
Async\spawn(fn() => {                     // checkout: транзакция пиннит conn
    $redis->multi();
    $redis->set('x', 1);
    $redis->exec();
});
```

Опционально `$redis->getPool()` → PHP-обёртка для интроспекции (count/idle/active),
как `PDO::getPool()` (см. `pdo_pool_get_wrapper`).

---

## 7. План работ (чеклист)

### Stage 1 — Checkout-пул
- [ ] `redis_pool.{c,h}`: структуры, init/shutdown, create/destroy.
- [ ] Парсинг опции `pool` в конструкторе/`connect`; объект → template-режим.
- [ ] Фабрика/destructor/healthcheck/before_acquire/before_release на
      `ZEND_ASYNC_NEW_POOL`.
- [ ] `redis_pool_acquire_conn` / `redis_pool_maybe_release` / on-finish callback.
- [ ] `redis_sock_get` → pool-aware; хвост `redis_process_cmd`/`_kw_cmd` → release.
- [ ] `redis_conn_is_pinned` + проверка по MULTI/PIPELINE/WATCH/SUB/SELECT.
- [ ] Корректное поведение `subscribe`/`multi`/`pipeline`/blocking (пиннинг).
- [ ] `getPool()` обёртка (опц.).
- [ ] Тесты `tests/async/` (см. §8).

### Stage 2 — Multiplex-очередь
- [ ] `redis_cmd_is_multiplexable` классификатор.
- [ ] Структура mux-сокета: write-батон, FIFO waiters, reader-корутина.
- [ ] Reply-router: парс по порядку, resp_cb от имени originator, resume.
- [ ] Замена read-пути в mux-режиме (suspend/resume вместо blocking read).
- [ ] Деградация в checkout для не-мультиплексируемых/битых.
- [ ] Батчинг записи (автопайплайнинг) + flush-стратегия.
- [ ] Тесты mux: порядок ответов, конкуренция, fallback на stateful, битый сокет.

---

## 8. Тесты (`tests/async/`)

Формат — `.phpt` в стиле `php-src/ext/async/tests` (корутины `Async\spawn` /
`await`), НЕ синхронный `TestRedis.php`. Хелпер `inc/async_redis_pool_test.inc`
по образцу `ext/async/tests/pdo_mysql/inc/async_pdo_mysql_test.inc`:
`skipIfNoAsync()`, `skipIfNoRedis()`, `poolFactory(max, mux)`.

Инварианты chaos-стиля (истинны при любой интерливинге — считаем attempts/
success, а не точные значения).

### Stage 1
1. `001` — pool construct: объект в template-режиме, idle/active/count.
2. `002` — одна корутина: GET/SET сквозь пул, тот же conn переиспользуется.
3. `003` — N корутин конкурентно: каждая видит изолированный результат.
4. `004` — backpressure: `max=1`, вторая корутина паркуется до release
   (порт `pool/029-pool_acquire_blocks_until_release.phpt`).
5. `005` — транзакция пиннит conn: MULTI…EXEC на одном физическом соединении.
6. `006` — изоляция: конкурентные MULTI в двух корутинах не перемешиваются.
7. `007` — WATCH/UNWATCH удерживает conn между вызовами.
8. `008` — pub/sub: SUBSCRIBE пиннит, обычные команды других корутин не страдают.
9. `009` — блокирующий BLPOP держит свой conn; пул не «съеден» для остальных.
10. `010` — финал корутины в открытой транзакции → conn вычищен и возвращён.
11. `011` — cancellation корутины во время команды → conn не утёк/не битый.
12. `012` — SELECT на не-дефолтную БД пиннит conn (не течёт к следующему).

### Stage 2
13. `101` — mux: много GET из N корутин по 1 сокету, все ответы корректны.
14. `102` — порядок: ответы матчатся отправителям при интерливинге.
15. `103` — fallback: MULTI/SUBSCRIBE/BLPOP в mux-режиме уходят на checkout.
16. `104` — битый mux-сокет → деградация в checkout, без потери ответов.
17. `105` — автопайплайнинг: K команд в один тик → один flush (наблюдаемо по RTT).

---

## 9. Открытые вопросы / риски

- **Где звать `maybe_release`.** v1: хвост `redis_process_*` (между командами).
  Альтернатива — release на suspend корутины (через switch-handlers TrueAsync):
  conn «твой», пока корутина активно бёрстит, и возвращается при парковке. Даёт
  лучшее переиспользование; отложено до замеров.
- **Stage 2 reply-router** — основной объём/риск: `fold_item`/`reply_callback`
  per-socket нужно исполнять в reader от имени originator-а.
- **RESP3 push** (client tracking, invalidation) — на mux-сокете push-сообщения
  идут вне FIFO ответов; роутить отдельным каналом (как redis_rs push_sender).
  Для v1 mux — без RESP3 push / без client-side caching.
- **Cluster/Sentinel** (`redis_cluster.c`) — вне рамок первой версии; пул сначала
  для одиночного `Redis`.

---

## 10. Ссылки

- PDO-пул (эталон): `php-src/ext/pdo/pdo_pool.{c,h}`, `pdo_dbh.c`.
- Async pool API: `php-src/Zend/zend_async_API.h` (`zend_async_pool_t`,
  `ZEND_ASYNC_NEW_POOL`, `ZEND_ASYNC_POOL_ACQUIRE/RELEASE/CLOSE`).
- Тест-образцы: `php-src/ext/async/tests/pool/`, `.../pdo_mysql/`.
- Redis pooling vs multiplexing: <https://redis.io/docs/latest/develop/clients/pools-and-muxing/>

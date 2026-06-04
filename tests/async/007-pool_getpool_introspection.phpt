--TEST--
Async pool: getPool() — null without a pool, settled counters after work
--SKIPIF--
<?php
require_once __DIR__ . '/inc/async_redis_pool_test.inc';
AsyncRedisPoolTest::skip();
?>
--FILE--
<?php
require_once __DIR__ . '/inc/async_redis_pool_test.inc';

use function Async\spawn;
use function Async\await;

/* A non-pooled instance has no pool object. */
$plain = new Redis(['host' => AsyncRedisPoolTest::host(), 'port' => AsyncRedisPoolTest::port()]);
echo "no-pool getPool: " . var_export($plain->getPool(), true) . "\n";

$redis = AsyncRedisPoolTest::poolFactory(max: 4);
$pool  = $redis->getPool();
echo "pool class: " . get_class($pool) . "\n";

/* Run concurrent work, then observe the settled pool. Invariants hold under any
 * interleaving: once every coroutine has finished, nothing is checked out. */
$coros = [];
for ($i = 0; $i < 6; $i++) {
    $coros[$i] = spawn(function() use ($redis, $i) {
        $k = AsyncRedisPoolTest::key("gp:$i");
        $redis->set($k, $i);
        $redis->del($k);
    });
}
foreach ($coros as $c) {
    await($c);
}

echo "settled active: " . $pool->activeCount() . "\n";
echo "idle == count: " . ($pool->idleCount() === $pool->count() ? "yes" : "no") . "\n";
echo "count within [1, 4]: " . ($pool->count() >= 1 && $pool->count() <= 4 ? "yes" : "no") . "\n";
echo "Done\n";
?>
--EXPECT--
no-pool getPool: NULL
pool class: Async\Pool
settled active: 0
idle == count: yes
count within [1, 4]: yes
Done

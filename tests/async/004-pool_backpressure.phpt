--TEST--
Async pool: max=1 backpressure — second coroutine waits for release
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

/* Port of ext/async/tests/pool/029: with max=1 the second consumer must park
 * while the first holds the connection (pinned by the transaction). */
$redis = AsyncRedisPoolTest::poolFactory(max: 1);
$k = AsyncRedisPoolTest::key('bp');

$c2Blocked = false;

$c1 = spawn(function() use ($redis, $k) {
    $redis->multi();              // pins the only connection
    $redis->set($k, '1');
    \Async\suspend();
    \Async\suspend();
    $redis->exec();               // releases the connection back to the pool
});

$c2 = spawn(function() use ($redis, $k, &$c2Blocked) {
    $c2Blocked = ($redis->getPool()->idleCount() === 0);
    $v = $redis->get($k);         // waits until c1 frees the conn
    $redis->del($k);
    return $v;
});

await($c1);
$got = await($c2);

echo "C2 had to wait: " . ($c2Blocked ? "yes" : "no") . "\n";
echo "C2 got: $got\n";
echo "Total connections: " . $redis->getPool()->count() . "\n";
echo "Done\n";
?>
--EXPECT--
C2 had to wait: yes
C2 got: 1
Total connections: 1
Done

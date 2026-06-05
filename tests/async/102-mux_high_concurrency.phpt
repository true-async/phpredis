--TEST--
Async mux: 64 concurrent coroutines, no cross-talk (data integrity)
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

/* Invariant (chaos-style): under any interleaving of 64 coroutines sharing 2
 * lanes, each coroutine reads exactly its own value back. The reply pump must
 * route every reply to its own sender — one mismatch fails the count. */
$redis = AsyncRedisPoolTest::poolFactory(max: 4, mux: 2);

$N = 64;
$coros = [];
for ($i = 0; $i < $N; $i++) {
    $coros[$i] = spawn(function() use ($redis, $i) {
        $k = AsyncRedisPoolTest::key("hc:$i");
        $val = "value-of-coroutine-$i";
        $redis->set($k, $val);
        $got = $redis->get($k);
        $redis->del($k);
        return $got === $val;
    });
}

$ok = 0;
foreach ($coros as $c) {
    if (await($c) === true) $ok++;
}

echo "correct: $ok / $N\n";
echo "Done\n";
?>
--EXPECT--
correct: 64 / 64
Done

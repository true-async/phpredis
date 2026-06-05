--TEST--
Async mux: multiplexed and checkout (transaction) coroutines coexist on one $redis
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

/* The same $redis serves two coroutine populations concurrently:
 *  - stateless workers ride the mux lanes;
 *  - transaction workers pin private checkout connections (MULTI/EXEC).
 * Both must stay correct and isolated under interleaving. */
$redis = AsyncRedisPoolTest::poolFactory(max: 6, mux: 2);

$M = 16;
$mux = [];
$txn = [];
for ($i = 0; $i < $M; $i++) {
    $mux[$i] = spawn(function() use ($redis, $i) {
        $k = AsyncRedisPoolTest::key("co:mux:$i");
        $redis->set($k, "m$i");
        \Async\suspend();
        $ok = $redis->get($k) === "m$i";
        $redis->del($k);
        return $ok;
    });
    $txn[$i] = spawn(function() use ($redis, $i) {
        $k = AsyncRedisPoolTest::key("co:txn:$i");
        $redis->multi();
        $redis->set($k, "t$i");
        \Async\suspend();
        $redis->append($k, "!");
        $redis->exec();
        $ok = $redis->get($k) === "t$i!";
        $redis->del($k);
        return $ok;
    });
}

$mok = 0; foreach ($mux as $c) if (await($c) === true) $mok++;
$tok = 0; foreach ($txn as $c) if (await($c) === true) $tok++;

echo "mux ok: $mok / $M\n";
echo "txn ok: $tok / $M\n";
echo "Done\n";
?>
--EXPECT--
mux ok: 16 / 16
txn ok: 16 / 16
Done

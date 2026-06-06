--TEST--
Async mux: command routing — multiplexable rides a lane, stateful takes checkout
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

/* Multiplexability is decided from the command verb known at dispatch (no
 * re-parse of the wire bytes). A multiplexable command rides a shared lane and
 * acquires NO checkout connection; a blocklisted one (here SELECT, classified
 * from the lowercase method token) must take — and pin — a private checkout
 * connection. The pool's checkout count is the observable. */
$redis = AsyncRedisPoolTest::poolFactory(max: 4, mux: 1);

/* A plain GET is multiplexable -> lane, so the checkout pool stays empty. */
$muxCount = await(spawn(function() use ($redis) {
    $redis->get(AsyncRedisPoolTest::key('route'));
    return $redis->getPool()->count();
}));

/* SELECT rebinds the connection -> not multiplexable -> checkout. */
$selDelta = await(spawn(function() use ($redis) {
    $before = $redis->getPool()->count();
    $redis->select(1);
    return $redis->getPool()->count() - $before;
}));

echo "mux command took checkout: " . ($muxCount === 0 ? "no" : "yes") . "\n";
echo "SELECT acquired a checkout: " . ($selDelta >= 1 ? "yes" : "no") . "\n";
echo "Done\n";
?>
--EXPECT--
mux command took checkout: no
SELECT acquired a checkout: yes
Done

--TEST--
Async mux: cancelling a coroutine mid-command leaves the lane usable
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

/* A coroutine is cancelled while parked waiting for its mux reply: the command
 * was already written, so the reply still arrives and the pump must discard it
 * (without use-after-free) while the lane keeps serving other coroutines. */
$main = spawn(function() {
    $redis = AsyncRedisPoolTest::poolFactory(max: 4, mux: 1);
    $k = AsyncRedisPoolTest::key('cancel');
    $redis->set($k, 'persists');

    $cancelled = 0;
    for ($n = 0; $n < 8; $n++) {
        $victim = spawn(function() use ($redis, $k) { return $redis->get($k); });
        \Async\suspend();          // let the victim send GET and park
        $victim->cancel();
        try { await($victim); } catch (\Throwable $e) { $cancelled++; }
        \Async\suspend();          // let the pump discard the abandoned reply
    }

    // The lane must still work after repeated cancellations.
    $alive = $redis->get($k);
    $redis->del($k);
    return [$cancelled, $alive];
});

[$cancelled, $alive] = await($main);
echo "cancellations observed: " . ($cancelled > 0 ? "yes" : "no") . "\n";
echo "lane alive after: $alive\n";
echo "Done\n";
?>
--EXPECT--
cancellations observed: yes
lane alive after: persists
Done

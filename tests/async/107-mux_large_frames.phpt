--TEST--
Async mux: large replies framed correctly across multiple reads, mixed with small
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

/* Values far larger than the pump's recv buffer (16 KiB) force a single reply to
 * span several reads, exercising the partial-frame scanner. Large and small
 * replies interleave on the shared lanes; every value must round-trip intact. */
$redis = AsyncRedisPoolTest::poolFactory(max: 4, mux: 2);

$N = 24;
$coros = [];
for ($i = 0; $i < $N; $i++) {
    $coros[$i] = spawn(function() use ($redis, $i) {
        $k = AsyncRedisPoolTest::key("big:$i");
        // sizes from tiny to ~250 KiB, content keyed to $i so cross-talk is caught
        $len = ($i % 2 === 0) ? (($i + 1) * 10000) : (5 + $i);
        $val = str_repeat(chr(65 + ($i % 26)), $len);
        $redis->set($k, $val);
        \Async\suspend();
        $got = $redis->get($k);
        $redis->del($k);
        return ($got === $val) ? 1 : 0;
    });
}

$ok = 0;
foreach ($coros as $c) {
    $ok += await($c);
}

echo "intact: $ok / $N\n";
echo "Done\n";
?>
--EXPECT--
intact: 24 / 24
Done

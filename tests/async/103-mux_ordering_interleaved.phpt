--TEST--
Async mux: reply ordering holds when coroutines interleave mid-command
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

/* Each coroutine yields between SET and GET, so commands from many coroutines
 * are forcibly interleaved on the shared lanes. FIFO reply matching must still
 * give each coroutine its own reply. Single lane stresses the FIFO hardest. */
$redis = AsyncRedisPoolTest::poolFactory(max: 4, mux: 1);

$N = 40;
$coros = [];
for ($i = 0; $i < $N; $i++) {
    $coros[$i] = spawn(function() use ($redis, $i) {
        $k = AsyncRedisPoolTest::key("ord:$i");
        $redis->set($k, "V-$i");
        \Async\suspend();
        \Async\suspend();
        $got = $redis->get($k);
        \Async\suspend();
        $redis->del($k);
        return $got === "V-$i";
    });
}

$ok = 0;
foreach ($coros as $c) {
    if (await($c) === true) $ok++;
}

echo "ordered: $ok / $N\n";
echo "Done\n";
?>
--EXPECT--
ordered: 40 / 40
Done

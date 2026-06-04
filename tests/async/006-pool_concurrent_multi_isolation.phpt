--TEST--
Async pool: concurrent MULTI in two coroutines do not interleave
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

/* Two coroutines run transactions in parallel. Each pins its OWN physical
 * connection (needs max>=2); commands do not interleave between transactions. */
$redis = AsyncRedisPoolTest::poolFactory(max: 2);

$mk = function(string $tag) use ($redis) {
    return function() use ($redis, $tag) {
        $k = AsyncRedisPoolTest::key("iso:$tag");
        $redis->multi();
        $redis->set($k, $tag);
        \Async\suspend();              // yield to the other coroutine
        $redis->append($k, $tag);
        $out = $redis->exec();
        $v = $redis->get($k);
        $redis->del($k);
        return $v;
    };
};

$a = spawn($mk('A'));
$b = spawn($mk('B'));

$va = await($a);
$vb = await($b);

echo "A: $va\n";   // must be "AA", not mixed with B
echo "B: $vb\n";   // must be "BB"
echo "Done\n";
?>
--EXPECT--
A: AA
B: BB
Done

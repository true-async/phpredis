--TEST--
Async mux: stateful commands fall back to a private checkout connection
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

/* In a mux-enabled pool, MULTI/EXEC and SELECT are NOT multiplexable: they must
 * transparently run on a private checkout connection, then mux resumes for the
 * following stateless command — all within one coroutine. */
$redis = AsyncRedisPoolTest::poolFactory(max: 4, mux: 2);

$co = spawn(function() use ($redis) {
    $k = AsyncRedisPoolTest::key('fb');

    // transaction -> checkout
    $redis->multi();
    $redis->set($k, 'a');
    $redis->append($k, 'b');
    $redis->get($k);
    $res = $redis->exec();

    // stateless again -> back on a mux lane
    $after = $redis->get($k);
    $redis->del($k);

    return [count($res), $res[2], $after];
});

[$n, $txval, $after] = await($co);
echo "exec count: $n\n";
echo "tx value: $txval\n";
echo "mux after txn: $after\n";
echo "Done\n";
?>
--EXPECT--
exec count: 3
tx value: ab
mux after txn: ab
Done

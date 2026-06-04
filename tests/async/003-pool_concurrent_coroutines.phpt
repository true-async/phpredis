--TEST--
Async pool: N concurrent coroutines, each sees isolated result over shared $redis
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

$redis = AsyncRedisPoolTest::poolFactory(max: 4);

/* Инвариант (chaos-стиль): при любой интерливинге каждая корутина читает
 * именно своё значение — общий $redis не перемешивает ответы. */
$N = 8;
$coros = [];
for ($i = 0; $i < $N; $i++) {
    $coros[$i] = spawn(function() use ($redis, $i) {
        $k = AsyncRedisPoolTest::key("conc:$i");
        $redis->set($k, "v$i");
        $v = $redis->get($k);
        $redis->del($k);
        return $v === "v$i";
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
correct: 8 / 8
Done

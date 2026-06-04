--TEST--
Async pool: single coroutine SET/GET, connection reused within coroutine
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
$k = AsyncRedisPoolTest::key('single');

$co = spawn(function() use ($redis, $k) {
    $redis->set($k, 'hello');
    $v = $redis->get($k);
    $redis->del($k);
    return $v;
});

echo "got: " . await($co) . "\n";
echo "Done\n";
?>
--EXPECT--
got: hello
Done

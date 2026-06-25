--TEST--
Async pool: host/port supplied via connect() (not the constructor) is used by the pool factory
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

/* Pool enabled in the constructor WITHOUT a host; the connection target is
 * supplied by a later connect(). The pool factory must seed each physical
 * connection from that template instead of dialing the loopback default —
 * otherwise acquire fails wherever Redis is not on 127.0.0.1 (e.g. Docker). */
$redis = new Redis([
    'pool' => [
        'enabled' => true,
        'min'     => 0,
        'max'     => 4,
        'mux'     => 0,
    ],
]);
$redis->connect(AsyncRedisPoolTest::host(), AsyncRedisPoolTest::port(), 1.0);

$single = spawn(function() use ($redis) {
    $k = AsyncRedisPoolTest::key('connhost:single');
    $redis->set($k, 'hello');
    $v = $redis->get($k);
    $redis->del($k);
    return $v;
});
echo "single: " . await($single) . "\n";

/* Several coroutines, each acquiring its own connection from the same pool. */
$coros = [];
for ($i = 0; $i < 6; $i++) {
    $coros[$i] = spawn(function() use ($redis, $i) {
        $k = AsyncRedisPoolTest::key("connhost:$i");
        $redis->set($k, $i);
        $v = $redis->get($k);
        $redis->del($k);
        return $v;
    });
}
$out = [];
foreach ($coros as $i => $c) {
    $out[$i] = await($c);
}
echo "concurrent: " . implode(',', $out) . "\n";
echo "Done\n";
?>
--EXPECT--
single: hello
concurrent: 0,1,2,3,4,5
Done

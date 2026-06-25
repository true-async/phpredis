--TEST--
Async pool: connect() is rejected in pool mode; host must come from the constructor
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

/* In pool mode the Redis object is a template, not a single live connection.
 * connect()/pconnect() must be rejected so the host can only come from the
 * constructor options — the one place the pool factory (and eager mux lanes)
 * can read it. */
$redis = new Redis([
    'pool' => ['enabled' => true, 'min' => 0, 'max' => 4, 'mux' => 0],
]);

try {
    $redis->connect(AsyncRedisPoolTest::host(), AsyncRedisPoolTest::port(), 1.0);
    echo "connect: NO EXCEPTION\n";
} catch (RedisException $e) {
    echo "connect throws: " . $e->getMessage() . "\n";
}

/* The supported way: host/port in the constructor options. */
$ok = AsyncRedisPoolTest::poolFactory(max: 4);
$co = spawn(function() use ($ok) {
    $k = AsyncRedisPoolTest::key('ctorhost');
    $ok->set($k, 'hello');
    $v = $ok->get($k);
    $ok->del($k);
    return $v;
});
echo "ctor-host get: " . await($co) . "\n";
echo "Done\n";
?>
--EXPECT--
connect throws: Redis::connect() is not supported in pool mode; set 'host' and 'port' in the constructor options
ctor-host get: hello
Done

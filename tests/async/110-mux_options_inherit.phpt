--TEST--
Async pool: multiplex lanes inherit setOption() state (serializer, prefix)
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

/* Commands are built with the template's options and replies are parsed with the
 * lane's — both sides must agree. */
$redis = AsyncRedisPoolTest::poolFactory(max: 2, mux: 1);
$redis->setOption(Redis::OPT_SERIALIZER, Redis::SERIALIZER_PHP);
$redis->setOption(Redis::OPT_PREFIX, 'muxtest:');

$plain = new Redis();
$plain->connect(AsyncRedisPoolTest::host(), AsyncRedisPoolTest::port(), 1.0);

$keys = [];
$coros = [];

for ($i = 0; $i < 4; $i++) {
    $key = 'mux_options_' . $i;
    $keys[] = $key;

    $coros[] = spawn(function () use ($redis, $key, $i) {
        $redis->set($key, ['n' => $i]);
        return $redis->get($key);
    });
}

foreach ($coros as $i => $co) {
    echo "co{$i}: ", var_export(await($co), true), "\n";
}

foreach ($keys as $key) {
    echo "prefixed {$key}: ", var_export((bool)$plain->exists('muxtest:' . $key), true), "\n";
    $plain->del('muxtest:' . $key);
}

$plain->close();

echo "Done\n";
?>
--EXPECT--
co0: array (
  'n' => 0,
)
co1: array (
  'n' => 1,
)
co2: array (
  'n' => 2,
)
co3: array (
  'n' => 3,
)
prefixed mux_options_0: true
prefixed mux_options_1: true
prefixed mux_options_2: true
prefixed mux_options_3: true
Done

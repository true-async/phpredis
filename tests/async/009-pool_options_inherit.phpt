--TEST--
Async pool: setOption() state (serializer, prefix) is inherited by pooled connections
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

/* In pool mode the object's own socket is a template that is never opened, so
 * options set on it must reach the physical connections the pool hands out. */
$redis = AsyncRedisPoolTest::poolFactory(max: 2);
$redis->setOption(Redis::OPT_SERIALIZER, Redis::SERIALIZER_PHP);
$redis->setOption(Redis::OPT_PREFIX, 'pooltest:');

/* Plain connection used to inspect what actually landed on the server. */
$plain = new Redis();
$plain->connect(AsyncRedisPoolTest::host(), AsyncRedisPoolTest::port(), 1.0);

$key = 'options_inherit';

$value = await(spawn(function () use ($redis, $key) {
    $redis->set($key, ['a' => 1]);
    return $redis->get($key);
}));

echo "round-trip: ", var_export($value, true), "\n";
echo "prefixed key on server: ", var_export((bool)$plain->exists('pooltest:' . $key), true), "\n";
echo "stored serialized: ", var_export(str_starts_with((string)$plain->get('pooltest:' . $key), 'a:1:'), true), "\n";

/* Options changed after the pool already has a live connection: the next
 * checkout must pick the new prefix up. */
$redis->setOption(Redis::OPT_PREFIX, 'pooltest2:');

await(spawn(function () use ($redis, $key) {
    $redis->set($key, ['b' => 2]);
}));

echo "new prefix on server: ", var_export((bool)$plain->exists('pooltest2:' . $key), true), "\n";

$plain->del('pooltest:' . $key, 'pooltest2:' . $key);
$plain->close();

echo "Done\n";
?>
--EXPECT--
round-trip: array (
  'a' => 1,
)
prefixed key on server: true
stored serialized: true
new prefix on server: true
Done

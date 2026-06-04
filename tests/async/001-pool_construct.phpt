--TEST--
Async pool: construct in template mode, introspection
--SKIPIF--
<?php
require_once __DIR__ . '/inc/async_redis_pool_test.inc';
AsyncRedisPoolTest::skip();
?>
--FILE--
<?php
require_once __DIR__ . '/inc/async_redis_pool_test.inc';

$redis = AsyncRedisPoolTest::poolFactory(max: 4);
$pool  = $redis->getPool();

echo "Has pool: " . ($pool !== null ? "yes" : "no") . "\n";
echo "Count: "  . $pool->count() . "\n";      // ещё ничего не выдано
echo "Idle: "   . $pool->idleCount() . "\n";
echo "Active: " . $pool->activeCount() . "\n";
echo "Done\n";
?>
--EXPECT--
Has pool: yes
Count: 0
Idle: 0
Active: 0
Done

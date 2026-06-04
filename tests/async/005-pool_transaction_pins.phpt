--TEST--
Async pool: MULTI...EXEC runs on a single pinned connection
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
$k = AsyncRedisPoolTest::key('txn');

$co = spawn(function() use ($redis, $k) {
    $redis->multi();
    $redis->set($k, 'a');
    $redis->append($k, 'b');
    $redis->get($k);
    $res = $redis->exec();        // транзакция атомарна на одном соединении
    $redis->del($k);
    return $res;
});

$res = await($co);
echo "exec count: " . count($res) . "\n";
echo "final value: " . $res[2] . "\n";   // SET, APPEND(len), GET
echo "Done\n";
?>
--EXPECT--
exec count: 3
final value: ab
Done

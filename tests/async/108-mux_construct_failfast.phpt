--TEST--
Async mux: an unreachable server fails construction fast (no half-built pool)
--SKIPIF--
<?php
require_once __DIR__ . '/inc/async_redis_pool_test.inc';
AsyncRedisPoolTest::skip();
// The fail-fast check needs a port where nothing listens; skip if one happens to.
$dead = @fsockopen(AsyncRedisPoolTest::host(), 6390, $e, $s, 0.5);
if ($dead) { fclose($dead); die("skip something is listening on port 6390\n"); }
?>
--FILE--
<?php
require_once __DIR__ . '/inc/async_redis_pool_test.inc';

/* With mux enabled the pool opens its lanes eagerly in the constructor. If the
 * server is unreachable, construction must throw immediately rather than hand
 * back a pool with half-open lanes (recovery is out of scope for v0). */
try {
    new Redis([
        'host' => AsyncRedisPoolTest::host(),
        'port' => 6390,
        'pool' => ['enabled' => true, 'min' => 0, 'max' => 4, 'mux' => 2],
    ]);
    echo "constructed (unexpected)\n";
} catch (\RedisException $e) {
    echo "threw RedisException\n";
}

/* A mux-less pool is template-only: it must NOT connect at construction, so the
 * same dead port constructs fine (connections are made lazily on checkout). */
$lazy = new Redis([
    'host' => AsyncRedisPoolTest::host(),
    'port' => 6390,
    'pool' => ['enabled' => true, 'min' => 0, 'max' => 4, 'mux' => 0],
]);
echo "mux-less constructed: " . ($lazy instanceof Redis ? "yes" : "no") . "\n";
echo "Done\n";
?>
--EXPECT--
threw RedisException
mux-less constructed: yes
Done

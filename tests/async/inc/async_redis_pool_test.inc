<?php
/* Helper for phpredis async pool tests. Modeled on
 * php-src/ext/async/tests/pdo_mysql/inc/async_pdo_mysql_test.inc */

use function Async\spawn;
use function Async\await;

class AsyncRedisPoolTest
{
    static function host(): string { return getenv('REDIS_TEST_HOST') ?: '127.0.0.1'; }
    static function port(): int    { return (int)(getenv('REDIS_TEST_PORT') ?: 6379); }

    /* Skip if the TrueAsync runtime is unavailable. */
    static function skipIfNoAsync(): void {
        if (!function_exists('Async\\spawn')) {
            die("skip TrueAsync runtime not available\n");
        }
    }

    /* Skip if the redis extension is not loaded. */
    static function skipIfNoRedis(): void {
        if (!extension_loaded('redis')) {
            die("skip redis extension not loaded\n");
        }
    }

    /* Skip if no live Redis server is reachable. */
    static function skipIfNoServer(): void {
        $r = new Redis();
        if (!@$r->connect(self::host(), self::port(), 1.0)) {
            die("skip no Redis server at " . self::host() . ":" . self::port() . "\n");
        }
        $r->close();
    }

    static function skip(): void {
        self::skipIfNoRedis();
        self::skipIfNoAsync();
        self::skipIfNoServer();
    }

    /* Pool-enabled Redis (template mode). Pool config — see TRUE_ASYNC_POOL.md §6. */
    static function poolFactory(int $max = 4, int $mux = 0, int $min = 0): Redis {
        return new Redis([
            'host' => self::host(),
            'port' => self::port(),
            'pool' => [
                'enabled' => true,
                'min'     => $min,
                'max'     => $max,
                'mux'     => $mux,
            ],
        ]);
    }

    /* Unique key prefix so tests do not clobber each other. */
    static function key(string $name): string {
        return 'phpredis:async:test:' . $name;
    }
}

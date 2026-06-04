<?php
/* Хелпер для async pool тестов phpredis. По образцу
 * php-src/ext/async/tests/pdo_mysql/inc/async_pdo_mysql_test.inc */

use function Async\spawn;
use function Async\await;

class AsyncRedisPoolTest
{
    static function host(): string { return getenv('REDIS_TEST_HOST') ?: '127.0.0.1'; }
    static function port(): int    { return (int)(getenv('REDIS_TEST_PORT') ?: 6379); }

    /* Скип, если TrueAsync-рантайм недоступен. */
    static function skipIfNoAsync(): void {
        if (!function_exists('Async\\spawn')) {
            die("skip TrueAsync runtime not available\n");
        }
    }

    /* Скип, если расширение redis не загружено. */
    static function skipIfNoRedis(): void {
        if (!extension_loaded('redis')) {
            die("skip redis extension not loaded\n");
        }
    }

    /* Скип, если живой Redis недоступен. */
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

    /* Pool-enabled Redis (template-режим). Конфиг пула — см. TRUE_ASYNC_POOL.md §6. */
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

    /* Уникальный префикс ключей, чтобы тесты не топтали друг друга. */
    static function key(string $name): string {
        return 'phpredis:async:test:' . $name;
    }
}

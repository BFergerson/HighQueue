package io.vertx.blueprint.kue.util;

import io.vertx.blueprint.kue.queue.JobState;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.ResponseType;

/**
 * Helper class for operating Redis.
 *
 * @author Eric Zhao
 */
public final class RedisHelper {

    private static final String VERTX_KUE_REDIS_PREFIX = "vertx_kue";

    private RedisHelper() {
    }

    /**
     * Factory method for creating a Redis client in Vert.x context.
     *
     * @param vertx  Vertx instance
     * @param config configuration
     * @return the new Redis client instance
     */
    public static Redis client(Vertx vertx, JsonObject config) {
        return Redis.createClient(vertx, options(config));
    }

    /**
     * Factory method for creating a default local Redis client configuration.
     *
     * @param config configuration from Vert.x context
     * @return the new configuration instance
     */
    public static RedisOptions options(JsonObject config) {
        String redisAuth = config.getString("redis.auth", "");
        if (!redisAuth.isEmpty()) {
            redisAuth += "@";
        }
        return new RedisOptions()
                .addConnectionString(String.format("redis://%s%s:%d",
                        redisAuth,
                        config.getString("redis.host", "127.0.0.1"),
                        config.getInteger("redis.port", 6379)));
    }

    /**
     * Wrap the key with prefix of Vert.x Kue namespace.
     *
     * @param key the key to wrap
     * @return the wrapped key
     */
    public static String getKey(String key) {
        return VERTX_KUE_REDIS_PREFIX + ":" + key;
    }

    /**
     * Generate the key of a certain task state with prefix of Vert.x Kue namespace.
     *
     * @param state task state
     * @return the generated key
     */
    public static String getStateKey(JobState state) {
        return VERTX_KUE_REDIS_PREFIX + ":jobs:" + state.name();
    }

    /**
     * Create an id for the zset to preserve FIFO order.
     *
     * @param id id
     */
    public static String createFIFO(long id) {
        String idLen = "" + ("" + id).length();
        int len = 2 - idLen.length();
        while (len-- > 0)
            idLen = "0" + idLen;
        return idLen + "|" + id;
    }

    /**
     * Parse out original ID from zid.
     *
     * @param zid zid
     */
    public static String stripFIFO(String zid) {
        return zid.substring(zid.indexOf('|') + 1);
    }

    /**
     * Parse out original ID from zid.
     *
     * @param zid zid
     */
    public static long numStripFIFO(String zid) {
        return Long.parseLong(zid.substring(zid.indexOf('|') + 1));
    }

    public static JsonArray toJsonArray(Response r) {
        JsonArray result = new JsonArray();
        r.forEach(it -> {
            if (it.type() == ResponseType.MULTI) {
                JsonArray innerArray = new JsonArray();
                it.forEach(it2 -> {
                    innerArray.add(it2.toString());
                });
                result.add(innerArray);
            } else {
                result.add(it.toString());
            }
        });
        return result;
    }
}

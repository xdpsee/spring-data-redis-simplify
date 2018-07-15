package com.zhenhui.library.redis.cache.support;

import com.zhenhui.library.redis.serializer.Serializer;
import redis.clients.jedis.Jedis;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

public class KeySupport<K> extends SerializeSupport<K> {

    public static final Charset UTF8 = Charset.forName("UTF-8");
    private static final String PREFIX_SPLITTER_IN_RAW_KEY = "|";

    private final String prefix;
    private final int expires;
    private final TimeUnit timeUnit;

    public KeySupport(String prefix, Serializer<K> serializer, int expires, TimeUnit timeUnit) {
        super(serializer);
        this.prefix = prefix;
        this.expires = expires;
        this.timeUnit = timeUnit;
    }

    public byte[] encodeKey(K key) {
        if (null == key) {
            throw new IllegalArgumentException("key == null");
        }

        return String.format("%s%s%s"
                , prefix
                , PREFIX_SPLITTER_IN_RAW_KEY
                , serializeAsString(key)
        ).getBytes(UTF8);
    }

    public K decodeKey(byte[] rawKey) {
        if (null == rawKey) {
            throw new IllegalArgumentException("rawKey == null");
        }

        String rawKeyStr = new String(rawKey, UTF8);
        int index = rawKeyStr.indexOf(PREFIX_SPLITTER_IN_RAW_KEY);
        if (index < 0) {
            throw new IllegalArgumentException("invalid raw key");
        }

        return deserialize(rawKeyStr.substring(index + 1));
    }

    public int defaultExpireSeconds() {
        return (int) timeUnit.toSeconds(expires);
    }

    public void expireKey(Jedis jedis, byte[] rawKey, int seconds) {
        jedis.expire(rawKey, seconds);
    }

}

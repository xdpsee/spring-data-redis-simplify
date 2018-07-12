package com.zhenhui.library.redis.cache.support;


import org.springframework.beans.factory.annotation.Autowired;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

public abstract class CacheSupport<K, V> {

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final String PREFIX_SPLITTER_IN_RAW_KEY = "|";
    private final String prefix;
    private final SerializerProvider<K, V> serializerProvider;
    private final long expires;
    private final TimeUnit timeUnit;

    @Autowired
    protected JedisPool commandSupport;

    public CacheSupport(String prefix, SerializerProvider<K, V> serializerProvider, long expires, TimeUnit timeUnit) {
        this.prefix = prefix;
        this.serializerProvider = serializerProvider;
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
                , serializerProvider.keySerializer().serialize(key)
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

        return serializerProvider.keySerializer().deserialize(rawKeyStr.substring(index + 1));
    }

    public byte[] encodeValue(V value) {
        if (null == value) {
            throw new IllegalArgumentException("value == null");
        }

        return serializerProvider.valueSerializer().serialize(value).getBytes(UTF8);
    }

    protected V decodeValue(byte[] bytes) {
        if (bytes != null) {
            return serializerProvider.valueSerializer().deserialize(new String(bytes, UTF8));
        }

        return null;
    }

    protected int defaultExpireSeconds() {
        return (int) timeUnit.toSeconds(expires);
    }

    protected void expireKey(Jedis jedis, byte[] rawKey, int seconds) {
        jedis.expire(rawKey, seconds);
    }

}


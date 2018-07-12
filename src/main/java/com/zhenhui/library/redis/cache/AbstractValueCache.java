package com.zhenhui.library.redis.cache;

import com.zhenhui.library.redis.cache.support.CacheSupport;
import com.zhenhui.library.redis.cache.support.SerializerProvider;
import org.springframework.util.CollectionUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AbstractValueCache<K, V> extends CacheSupport<K, V> {

    public AbstractValueCache(String prefix, SerializerProvider<K, V> serializer, long expires, TimeUnit timeUnit) {
        super(prefix, serializer, expires, timeUnit);
    }

    public V get(K key) {
        Jedis jedis = commandSupport.getResource();
        try {
            byte[] bytes = jedis.get(encodeKey(key));
            return decodeValue(bytes);
        } finally {
            jedis.close();
        }
    }

    public void put(K key, V value) {
        put(key, value, defaultExpireSeconds());
    }

    public void put(K key, V value, int expireInSeconds) {

        Jedis jedis = commandSupport.getResource();
        try {
            byte[] rawKey = encodeKey(key);
            jedis.set(rawKey, encodeValue(value));
            expireKey(jedis, rawKey, expireInSeconds);
        } finally {
            jedis.close();
        }

    }

    public Map<K, V> multiGet(Collection<K> keys) {
        final Map<K, V> result = new HashMap<>();
        if (keys.isEmpty()) {
            return result;
        }

        final byte[][] rawKeys = new byte[keys.size()][];
        keys.stream().map(this::encodeKey).collect(Collectors.toList()).toArray(rawKeys);

        Jedis jedis = commandSupport.getResource();

        try {
            final List<byte[]> rawValues = jedis.mget(rawKeys);
            assert rawKeys.length == rawValues.size() : "rawKeys,rawValues size mismatch";

            for (int i = 0; i < rawKeys.length; ++i) {
                byte[] rawKey = rawKeys[i];
                byte[] rawValue = rawValues.get(i);
                if (rawValue != null) {
                    result.put(decodeKey(rawKey), decodeValue(rawValue));
                }
            }

            return result;
        } finally {
            jedis.close();
        }
    }

    public void multiPut(Map<K, V> kvMap) {
        multiPut(kvMap, defaultExpireSeconds());
    }

    public void multiPut(Map<K, V> kvMap, int expireInSeconds) {
        if (!CollectionUtils.isEmpty(kvMap)) {

            final byte[][] keyValues = new byte[kvMap.size() * 2][];

            kvMap.entrySet().stream()
                    .map(e -> {
                        byte[][] elements = new byte[2][];
                        elements[0] = encodeKey(e.getKey());
                        elements[1] = encodeValue(e.getValue());
                        return elements;
                    }).flatMap(Stream::of)
                    .collect(Collectors.toList())
                    .toArray(keyValues);

            Jedis jedis = commandSupport.getResource();
            try {
                Pipeline pipeline = jedis.pipelined();
                pipeline.mset(keyValues);
                for (int i = 0; i < keyValues.length; i += 2) {
                    pipeline.pexpire(keyValues[i], (long)expireInSeconds * 1000);
                }
                pipeline.sync();
            } finally {
                jedis.close();
            }
        }
    }
}

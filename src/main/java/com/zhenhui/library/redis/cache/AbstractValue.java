package com.zhenhui.library.redis.cache;

import com.zhenhui.library.redis.cache.support.CommandSupport;
import com.zhenhui.library.redis.cache.support.KeySupport;
import com.zhenhui.library.redis.cache.support.SerializeSupport;
import com.zhenhui.library.redis.serializer.Serializer;
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

public class AbstractValue<K, V> extends CommandSupport {

    private final KeySupport<K> keySupport;
    private final SerializeSupport<V> valueSupport;

    public AbstractValue(String prefix, Serializer<K> keySerializer, Serializer<V> valueSerializer, int expires, TimeUnit timeUnit) {
        keySupport = new KeySupport<>(prefix, keySerializer, expires, timeUnit);
        valueSupport = new SerializeSupport<>(valueSerializer);
    }

    public V get(K key) {
        Jedis jedis = jedisPool.getResource();
        try {
            byte[] bytes = jedis.get(keySupport.encodeKey(key));
            return valueSupport.deserialize(bytes);
        } finally {
            jedis.close();
        }
    }

    public void put(K key, V value) {
        put(key, value, keySupport.defaultExpireSeconds());
    }

    public void put(K key, V value, int expireInSeconds) {

        Jedis jedis = jedisPool.getResource();
        try {
            byte[] rawKey = keySupport.encodeKey(key);
            jedis.set(rawKey, valueSupport.serialize(value));
            keySupport.expireKey(jedis, rawKey, expireInSeconds);
        } finally {
            jedis.close();
        }

    }

    public Map<K, V> get(Collection<K> keys) {
        final Map<K, V> result = new HashMap<>();
        if (keys.isEmpty()) {
            return result;
        }

        final byte[][] rawKeys = new byte[keys.size()][];
        keys.stream().map(keySupport::encodeKey).collect(Collectors.toList()).toArray(rawKeys);

        Jedis jedis = jedisPool.getResource();

        try {
            final List<byte[]> rawValues = jedis.mget(rawKeys);
            assert rawKeys.length == rawValues.size() : "rawKeys,rawValues size mismatch";

            for (int i = 0; i < rawKeys.length; ++i) {
                byte[] rawKey = rawKeys[i];
                byte[] rawValue = rawValues.get(i);
                if (rawValue != null) {
                    result.put(keySupport.decodeKey(rawKey), valueSupport.deserialize(rawValue));
                }
            }

            return result;
        } finally {
            jedis.close();
        }
    }

    public void put(Map<K, V> kvMap) {
        put(kvMap, keySupport.defaultExpireSeconds());
    }

    public void put(Map<K, V> kvMap, int expireInSeconds) {
        if (!CollectionUtils.isEmpty(kvMap)) {

            final byte[][] keyValues = new byte[kvMap.size() * 2][];

            kvMap.entrySet().stream()
                    .map(e -> {
                        byte[][] elements = new byte[2][];
                        elements[0] = keySupport.encodeKey(e.getKey());
                        elements[1] = valueSupport.serialize(e.getValue());
                        return elements;
                    }).flatMap(Stream::of)
                    .collect(Collectors.toList())
                    .toArray(keyValues);

            Jedis jedis = jedisPool.getResource();
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

    public boolean exists(K key) {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.exists(keySupport.encodeKey(key));
        } finally {
            jedis.close();
        }
    }

    public void evict(K key) {
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.del(keySupport.encodeKey(key));
        } finally {
            jedis.close();
        }
    }

}


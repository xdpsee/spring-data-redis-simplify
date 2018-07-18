package com.zhenhui.library.redis.cache;

import com.zhenhui.library.redis.cache.support.CommandSupport;
import com.zhenhui.library.redis.cache.support.KeySupport;
import com.zhenhui.library.redis.cache.support.SerializeSupport;
import com.zhenhui.library.redis.serializer.Serializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class AbstractZSet<K, M> extends CommandSupport {

    @Data
    @AllArgsConstructor
    public static class Member<M> {

        private final M value;
        private final Double score;

    }

    private final KeySupport<K> keySupport;
    private final SerializeSupport<M> memberSupport;


    public AbstractZSet(String prefix, Serializer<K> keySerializer, Serializer<M> memberSerializer, int expires, TimeUnit timeUnit) {
        keySupport = new KeySupport<>(prefix, keySerializer, expires, timeUnit);
        memberSupport = new SerializeSupport<>(memberSerializer);
    }

    public Double score(K key, M member) {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.zscore(keySupport.encodeKeyAsString(key), memberSupport.serializeAsString(member));
        } finally {
            jedis.close();
        }
    }

    public void add(K key, M member, double score) {
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.zadd(keySupport.encodeKeyAsString(key), score, memberSupport.serializeAsString(member));
        } finally {
            jedis.close();
        }
    }

    public void add(K key, Member<M> member) {
        add(key, member.value, member.score);
    }

    public void add(K key, Map<M, Double> memberScores) {
        Jedis jedis = jedisPool.getResource();
        try {
            Map<String, Double> tuples = new HashMap<>();
            memberScores.forEach((k, v) -> tuples.put(memberSupport.serializeAsString(k), v));
            jedis.zadd(keySupport.encodeKeyAsString(key), tuples);
        } finally {
            jedis.close();
        }
    }

    public void add(K key, Collection<Member<M>> members) {
        add(key, members.stream()
                .collect(Collectors.toMap(Member::getValue, Member::getScore)));
    }

    public void remove(K key, M member) {
        Jedis jedis = jedisPool.getResource();
        try {
            Long ret = jedis.zrem(keySupport.encodeKey(key), memberSupport.serialize(member));
            System.out.print(ret);
        } finally {
            jedis.close();
        }
    }

    public int count(K key) {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.zcard(keySupport.encodeKey(key)).intValue();
        } finally {
            jedis.close();
        }
    }

    public int count(K key, double min, double max) {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.zcount(keySupport.encodeKey(key), min, max).intValue();
        } finally {
            jedis.close();
        }
    }

    public List<M> top(K key, int limit, boolean ascending) {
        return top(key, null, limit, ascending);
    }

    public List<M> top(K key, Double offsetScore, int limit, boolean ascending) {

        final List<M> result = new ArrayList<>(limit);

        Jedis jedis = jedisPool.getResource();
        try {
            Double min = Double.MIN_VALUE;
            Double max = Double.MAX_VALUE;

            Set<String> members;
            if (ascending) {
                if (offsetScore != null) {
                    min = offsetScore;
                }

                members = jedis.zrangeByScore(keySupport.encodeKeyAsString(key), min, max, 0, limit);
            } else {
                if (offsetScore != null) {
                    max = offsetScore;
                }

                members = jedis.zrevrangeByScore(keySupport.encodeKeyAsString(key), max, min, 0, limit);
            }

            result.addAll(members.stream().map(memberSupport::deserialize).collect(Collectors.toList()));

        } finally {
            jedis.close();
        }

        return result;
    }

    public List<Member<M>> topWithScore(K key, int limit, boolean ascending) {
        return topWithScore(key, null, limit, ascending);
    }

    public List<Member<M>> topWithScore(K key, Double offsetScore, int limit, boolean ascending) {

        final List<Member<M>> result = new ArrayList<>(limit);

        Jedis jedis = jedisPool.getResource();
        try {
            Double min = Double.MIN_VALUE;
            Double max = Double.MAX_VALUE;

            Set<Tuple> members;
            if (ascending) {
                if (offsetScore != null) {
                    min = offsetScore;
                }

                members = jedis.zrangeByScoreWithScores(keySupport.encodeKeyAsString(key), min, max, 0, limit);
            } else {
                if (offsetScore != null) {
                    max = offsetScore;
                }

                members = jedis.zrevrangeByScoreWithScores(keySupport.encodeKeyAsString(key), max, min, 0, limit);
            }

            result.addAll(members.stream()
                    .map(e -> new Member<>(memberSupport.deserialize(e.getBinaryElement()), e.getScore()))
                    .collect(Collectors.toList())
            );

        } finally {
            jedis.close();
        }

        return result;
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

package com.zhenhui.library.redis.serializer;


@SuppressWarnings("unused")
public class LongSerializer implements Serializer<Long> {

    @Override
    public String serialize(Long v) {
        return String.valueOf(v);
    }

    @Override
    public Long deserialize(String v) {
        return Long.valueOf(v);
    }
}
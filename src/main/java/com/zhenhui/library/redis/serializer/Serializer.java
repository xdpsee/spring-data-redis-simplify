package com.zhenhui.library.redis.serializer;


@SuppressWarnings("unused")
public interface Serializer<T> {

    String serialize(T v);

    T deserialize(String v);

}

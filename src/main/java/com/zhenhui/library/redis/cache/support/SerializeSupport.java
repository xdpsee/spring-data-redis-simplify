package com.zhenhui.library.redis.cache.support;

import com.zhenhui.library.redis.serializer.Serializer;

import java.nio.charset.Charset;

public class SerializeSupport<T> {

    public static final Charset UTF8 = Charset.forName("UTF-8");

    private final Serializer<T> serializer;

    public SerializeSupport(Serializer<T> serializer) {
        this.serializer = serializer;
    }

    public byte[] serialize(T value) {
        if (null == value) {
            throw new IllegalArgumentException("value == null");
        }

        return serializer.serialize(value).getBytes(UTF8);
    }

    public String serializeAsString(T value) {
        if (null == value) {
            throw new IllegalArgumentException("value == null");
        }

        return serializer.serialize(value);
    }

    public T deserialize(String v) {
        if (v != null) {
            return serializer.deserialize(v);
        }

        return null;
    }

    public T deserialize(byte[] bytes) {
        if (bytes != null) {
            return serializer.deserialize(new String(bytes, UTF8));
        }

        return null;
    }

}

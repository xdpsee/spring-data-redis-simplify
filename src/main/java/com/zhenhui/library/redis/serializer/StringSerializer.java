package com.zhenhui.library.redis.serializer;

@SuppressWarnings("unused")
public class StringSerializer implements Serializer<String> {

    public String deserialize(String v) {
        return v;
    }

    public String serialize(String v) {
        return v;
    }
}



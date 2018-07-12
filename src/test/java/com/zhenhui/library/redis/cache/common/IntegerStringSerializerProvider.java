package com.zhenhui.library.redis.cache.common;

import com.zhenhui.library.redis.cache.support.SerializerProvider;
import com.zhenhui.library.redis.serializer.Serializer;
import com.zhenhui.library.redis.serializer.StringSerializer;
import org.springframework.stereotype.Component;

@Component
public class IntegerStringSerializerProvider implements SerializerProvider<Integer, String> {

    @Override
    public Serializer<Integer> keySerializer() {
        return new Serializer<Integer>() {
            @Override
            public String serialize(Integer v) {
                return String.valueOf(v);
            }

            @Override
            public Integer deserialize(String v) {
                return Integer.valueOf(v);
            }
        };
    }

    @Override
    public Serializer<String> valueSerializer() {
        return new StringSerializer();
    }

}

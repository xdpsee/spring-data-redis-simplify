package com.zhenhui.library.redis.cache.support;

import com.zhenhui.library.redis.serializer.Serializer;

public interface SerializerProvider<K, V> {

    Serializer<K> keySerializer();

    Serializer<V> valueSerializer();

}



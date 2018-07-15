package com.zhenhui.library.redis.cache.support;


import org.springframework.beans.factory.annotation.Autowired;
import redis.clients.jedis.JedisPool;

public abstract class CommandSupport{

    @Autowired
    protected JedisPool jedisPool;

}





package com.zhenhui.library.redis.cache;

import com.zhenhui.library.redis.serializer.Serializer;
import com.zhenhui.library.redis.serializer.StringSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:test-context.xml")
public class KVCacheTests extends AbstractJUnit4SpringContextTests {


    @Component
    public static class KeyValueTest extends AbstractValue<Integer, String> {

        public KeyValueTest() {
            super("KeyValueTest", new Serializer<Integer>() {
                @Override
                public String serialize(Integer v) {
                    return String.valueOf(v);
                }

                @Override
                public Integer deserialize(String v) {
                    return Integer.valueOf(v);
                }
            }, new StringSerializer(), 3, TimeUnit.SECONDS);
        }

    }

    @Autowired
    private KeyValueTest keyValueTestCache;

    @Test
    public void testNormal() {

        keyValueTestCache.put(1024, "hello");

        String value = keyValueTestCache.get(1024);

        assertEquals("hello", value);
        }

    @Test
    public void testMulti() {

        Map<Integer, String> result = keyValueTestCache.get(Arrays.asList(1,2,3,4,5));
        assertEquals(0, result.size());

        Map<Integer, String> tuples = new HashMap<>();
        tuples.put(1, "1");
        tuples.put(2, "2");
        tuples.put(3, "3");

        keyValueTestCache.put(tuples);

        result = keyValueTestCache.get(Arrays.asList(1,2,3,4,5));
        assertEquals(3, result.size());

    }

    @Test
    public void testExpire() {

        keyValueTestCache.put(1024, "hello");

        String value = keyValueTestCache.get(1024);

        assertEquals("hello", value);

        try {
            Thread.sleep(3000);
            value = keyValueTestCache.get(1024);
            assertNull(value);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testEvict() {
        keyValueTestCache.put(1024, "hello");

        String value = keyValueTestCache.get(1024);

        assertEquals("hello", value);

        keyValueTestCache.evict(1024);
        assertNull(keyValueTestCache.get(1024));
    }

}


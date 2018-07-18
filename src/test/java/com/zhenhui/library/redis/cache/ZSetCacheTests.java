package com.zhenhui.library.redis.cache;

import com.zhenhui.library.redis.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:test-context.xml")
public class ZSetCacheTests extends AbstractJUnit4SpringContextTests {

    @Component
    public static class ZSetCache extends AbstractZSet<String, String> {
        public ZSetCache() {
            super("ZSetCache", new StringSerializer(), new StringSerializer(), 6, TimeUnit.SECONDS);
        }
    }

    @Autowired
    private ZSetCache zSetCache;

    @Before
    public void setup() {
        zSetCache.add("top", Arrays.asList(
                new AbstractZSet.Member<>("tom", 97.0),
                new AbstractZSet.Member<>("json", 67.0),
                new AbstractZSet.Member<>("lucy", 97.0),
                new AbstractZSet.Member<>("john", 89.0),
                new AbstractZSet.Member<>("bob", 91.0),
                new AbstractZSet.Member<>("tms", 98.0),
                new AbstractZSet.Member<>("bee", 89.0)
        ));
    }

    @After
    public void tearDown() {
        zSetCache.evict("top");
    }

    @Test
    public void testAddCount() {

        zSetCache.add("test", "john", 98.0);

        zSetCache.add("test", new AbstractZSet.Member<>("jack", 99.0));

        Map<String, Double> members = new HashMap<>();
        members.put("lucy", 61.0);
        zSetCache.add("test", members);

        zSetCache.add("test", Arrays.asList(
                new AbstractZSet.Member<>("json", 97.0),
                new AbstractZSet.Member<>("tom", 89.0))
        );

        assertEquals(5, zSetCache.count("test"));
        assertEquals(5, zSetCache.count("test", 50.0, 99.0));

    }

    @Test
    public void testTop() {

        List<String> members = zSetCache.top("top", 10, true);
        assertEquals("json", members.get(0));

        members = zSetCache.top("top", 10, false);
        assertEquals("tms", members.get(0));

        members = zSetCache.top("top", 70.0, 10, true);
        assertEquals(6, members.size());

        members = zSetCache.top("top", 70.0, 10, false);
        assertEquals(1, members.size());
    }

    @Test
    public void testTopWithScore() {
        List<AbstractZSet.Member<String>> members = zSetCache.topWithScore("top", 10, true);

        assertEquals(7, members.size());

        assertEquals("json", members.get(0).getValue());
        assertEquals((Double) 67.0, members.get(0).getScore());

        assertEquals("bee", members.get(1).getValue());
        assertEquals((Double) 89.0, members.get(1).getScore());

        assertEquals("john", members.get(2).getValue());
        assertEquals((Double) 89.0, members.get(2).getScore());

        assertEquals("bob", members.get(3).getValue());
        assertEquals((Double) 91.0, members.get(3).getScore());


    }

    @Test
    public void testRemove() {

        zSetCache.add("remove", "jerry", 100.0);

        List<String> members = zSetCache.top("remove", 10, true);
        assertEquals(1, members.size());

        zSetCache.remove("remove", "jerry");

        assertEquals(0, zSetCache.count("remove"));
    }

    @Test
    public void testScore() {

        assertEquals((Double) 97.0, zSetCache.score("top", "tom"));

    }
}

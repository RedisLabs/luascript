package com.redislabs.luascript;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by dvirsky on 02/03/16.
 */
public class LuaScriptTest {

    @org.junit.Before
    public void setUp() throws Exception {

    }


    @org.junit.Test
    public void testSimple() throws Exception {

        LuaScript s = LuaScript.fromResource("lua/simple.lua", "redis://localhost:6379");
        assertNotNull(s.code);
        assertNotNull(s.sha);
        assertNotNull(s.pool);

        Object o = s.execute();
        assertNotNull(o);
        assertEquals("BAR", o);

    }

    @org.junit.Test
    public void testFromSource() throws Exception {

        LuaScript s = LuaScript.fromSource("return 'WAT'", "redis://localhost:6379");
        assertNotNull(s.code);
        assertNotNull(s.sha);
        assertNotNull(s.pool);

        Object o = s.execute();
        assertNotNull(o);
        assertEquals("WAT", o);

    }

    @org.junit.Test
    public void testArgs() throws Exception {

        LuaScript s = LuaScript.fromResource("lua/with_arguments.lua", "redis://localhost:6379");
        assertNotNull(s.code);
        assertNotNull(s.sha);
        assertNotNull(s.pool);

        String key = "fooargs", val = "waaat";
        Object o = s.execute(1, key, val);
        assertNotNull(o);
        assertEquals(o, val);

        Jedis conn = s.pool.getResource();
        assertEquals(o, conn.get(key));
        conn.close();

        List<String> keys  = new ArrayList<String>(1);
        keys.add(key);
        List<String> args  = new ArrayList<String>(1);
        args.add(val);
        o = s.execute(keys, args);
        assertNotNull(o);
        assertEquals(o, val);
    }

    @org.junit.Test
    public void testWithExternalConnection() throws Exception {
        LuaScript s = LuaScript.fromResource("lua/simple.lua", "redis://localhost:6379");

        Jedis conn = new Jedis( "redis://localhost:6379");
        Object o = s.execute(conn);
        assertNotNull(o);
        assertEquals("BAR", o);


        List<String> keys  = new ArrayList<String>(0);
        List<String> args  = new ArrayList<String>(0);
        o = s.execute(conn, keys, args);
        assertNotNull(o);
        assertEquals("BAR", o);

        conn.close();
    }

    @org.junit.Test
    public void testWithPipeline() throws Exception {
        LuaScript s = LuaScript.fromResource("lua/simple.lua", "redis://localhost:6379");

        Jedis conn = new Jedis( "redis://localhost:6379");
        Pipeline pipe = conn.pipelined();

        Response<String> res = s.execute(pipe);
        assertNotNull(res);

        List<Object> objs = pipe.syncAndReturnAll();
        assertEquals(1, objs.size());
        assertEquals("BAR", objs.get(0));

        List<String> keys  = new ArrayList<String>(0);
        List<String> args  = new ArrayList<String>(0);
        res = s.execute(pipe, keys, args);
        assertNotNull(res);
        objs = pipe.syncAndReturnAll();
        assertEquals(1, objs.size());
        assertEquals("BAR", objs.get(0));

        conn.close();
    }

}
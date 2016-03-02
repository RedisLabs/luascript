package com.redislabs.luascript;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.*;
import java.net.URL;
import java.util.List;

/**
 * LuaScript is a convenience wrapper around Lua scripts in redis.
 * The idea is thatyou turn the script into an object which can be called,
 * and the library takes care of loading the script, bookkeeping of SHAs, etc
 */
public class LuaScript {

    protected JedisPool pool;

    // the actual source code
    protected String code;

    // the SHA hash of the loaded code from redis
    protected String sha;


    /**
     * Create a new LuaScript object from a Lua file resource, loading it to a given redis server
     * @param resource resource name to load, e.g. "lua/myscript.lua"
     * @param redisURI URI of the server to connect to, e.g. redis://localhost:6379
     * @return the newly created script
     * @throws IOException
     */
    public  static LuaScript fromResource(String resource, String redisURI) throws IOException {

        // find the resource
        URL u = LuaScript.class.getClassLoader().getResource(resource);
        if (u == null) {
            throw new FileNotFoundException("resource not found");
        }

        // read its contents
        File file = new File(u.getFile());
        FileReader r = new FileReader(file);
        char[] data = new char[(int) file.length()];
        r.read(data);
        r.close();

        return new LuaScript(new String(data), redisURI);
    }

    /**
     * Create a new LuaScript from raw lua source code
     * @param source the lua source code to load
     * @param redisURI URI of the server to connect to, e.g. redis://localhost:6379
     * @return the newly cretaed script object
     * @throws IOException
     */
    public  static LuaScript fromSource(String source, String redisURI) throws IOException {
        return new LuaScript(source, redisURI);
    }

    /**
     * Internal constructor
     */
    private LuaScript(String sourceCode, String redisURI) throws IOException {
        code = sourceCode;
        pool = new JedisPool(redisURI);
        loadScript();

    }

    /**
     * Load the script and keep the SHA value
     */
    private void loadScript() {
        Jedis conn = pool.getResource();

        sha = conn.scriptLoad(code);

        conn.close();
    }


    /**
     * Execute the script with no arguments
     * @return
     */
    public Object execute() {
        return execute(0);
    }

    /**
     * Execute the script on a Jedis connection instance
     * @param conn a Jedis connection instance
     * @return script output
     */
    public Object execute(Jedis conn) {
        return execute(conn, 0);
    }

    /**
     * Execute the script with a list of keys and list of arguments
     * @param keys the KEYS to be sent to the script
     * @param args the ARGV to be sent to the script
     * @return script output
     */
    public Object execute(List<String> keys, List<String> args) {

        Jedis conn = pool.getResource();
        Object ret = execute(conn, keys, args);
        conn.close();
        return ret;
    }


    /**
     * Execute the script with a list of keys and list of arguments, on a given Jedis connection
     * @param conn Jedis connection overriding the default one
     * @param keys the KEYS to be sent to the script
     * @param args the ARGV to be sent to the script
     * @return script output
     */
    public Object execute(Jedis conn, List<String> keys, List<String> args) {
        int numKeys = keys.size();

        String[] combined = new String[keys.size() + args.size()];
        for (int i = 0; i < keys.size(); i++) {
            combined[i] = keys.get(i);
        }
        for (int i = 0; i < args.size(); i++) {
            combined[keys.size()+i] = args.get(i);
        }

        return execute(conn, numKeys, combined);
    }

    /**
     * Execute the script with a list of keys and list of arguments, on a given Jedis pipeline.
     * This DOES NOT actuall executes the pipeline, just sends the EVALSHA command to redis.
     * Notice that if the script is not loaded, we load it using the default redis URI
     * @param pipe the pipeline to execute the script
     * @param keys
     * @param args
     * @return
     */
    public Response<String> execute(Pipeline pipe, List<String> keys, List<String> args) {
        int numKeys = keys.size();

        String[] combined = new String[keys.size() + args.size()];
        for (int i = 0; i < keys.size(); i++) {
            combined[i] = keys.get(i);
        }
        for (int i = 0; i < args.size(); i++) {
            combined[keys.size()+i] = args.get(i);
        }

        return execute(pipe, numKeys, combined);
    }

    /**
     * Execute the script with no arguments on a pipeline
      This DOES NOT actuall executes the pipeline, just sends the EVALSHA command to redis.
     * Notice that if the script is not loaded, we load it using the default redis URI
     * @param pipe the pipeline to execute the script
     * @return pipeline response
     */
    public Response<String> execute(Pipeline pipe) {
        return execute(pipe, 0);
    }

    /**
     * Execute a script with NUMKEYS and KEYS/ARGS combined variadic arguments
     * @param pipe the pipeline to execute the script
     * @param numKeys the number of entries in args which are KEYS
     * @param args combined KEYS + ARGS
     * @return pipeline response
     */
    public Response<String> execute(Pipeline pipe, int numKeys, String ...args) {
        Response<String> res = null;

        try {
            res = pipe.evalsha(sha, numKeys, args);
        } catch (JedisDataException e) {
            // this should pop if the script wasn't loaded or deleted, and re-cache the script
            loadScript();
        }

        return res;
    }

    /**
     * Execute the script  with NUMKEYS and KEYS/ARGS combined variadic arguments
     * @param numKeys the number of entries in args which are KEYS
     * @param args combined KEYS + ARGS
     * @return script output
     */
    public Object execute(int numKeys, String ...args) {

        Jedis conn = pool.getResource();
        Object ret = execute(conn, numKeys, args);
        conn.close();
        return ret;
    }

    /**
     * Execute the script  with NUMKEYS and KEYS/ARGS combined variadic arguments - on  a given Jedis connection
     * @param numKeys the number of entries in args which are KEYS
     * @param args combined KEYS + ARGS
     * @return script output
     */
    public Object execute(Jedis conn, int numKeys, String ...args) {
        Object res = null;

        try {
            res = conn.evalsha(sha, numKeys, args);
        } catch (JedisDataException e) {
            // this should pop if the script wasn't loaded or deleted, and re-cache the script
            loadScript();
        }

        return res;
    }



}

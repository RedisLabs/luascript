# LuaScript - easily run Lua Redis scripts with Jedis

This library provides a nicer interface to handling Lua scripts with Jedis.

The idea is simple - instead of loading the script to redis, managing SHAs etc - just wrap it in a LuaScript object.

Scripts can be raw lua code, or loaded from project resources.

Scripts can then be called with or without arguments.
 
## Usage example

```java
import com.redislabs.luascript.LuaScript;

// loading a script from resource, and caching it on a redis server
LuaScript s = LuaScript.fromResource("lua/simple.lua", "redis://localhost:6379");

// now calling it is done simply with
Object o = s.execute();
```

### Scripts with arguments can be called like so:

```java
// the first argument is the number of keys in the variadic argument list
s.execute(1, key, value1, value2);

// or with lists of strings and keys
s.execute(keys, args);
```

### Scripts can also be used with pipelines or existing Jedis objects:

```java
Jedis conn = new Jedis(...);

s.execute(conn);

Pipeline pipe = conn.pipelined();

// note that executing on pipelines does not sync the pipeline
s.execute(pipe);
```
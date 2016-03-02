
-- ZRANGEBYSCORESTORE <key> <dst> <min> <max>
--
--local tbl = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'WITHSCORES')
--
--for i, _ in ipairs(tbl) do
--    if i % 2 == 1 then do
--        redis.call('ZADD', KEYS[2], tbl[i], tostring(tbl[i+1]))
--        end
--    end
--end

redis.call('SET', 'FOO', 'BAR')
return redis.call('GET', 'FOO')
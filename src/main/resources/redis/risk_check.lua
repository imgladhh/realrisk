local key       = KEYS[1]
local now       = tonumber(ARGV[1])
local window    = tonumber(ARGV[2])
local limit     = tonumber(ARGV[3])
local dedupeKey = ARGV[4]

redis.call('ZADD', key, now, dedupeKey)
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
redis.call('EXPIRE', key, math.ceil(window * 2 / 1000))

local count = redis.call('ZCARD', key)
if count > limit then
  return {1, count}
end
return {0, count}

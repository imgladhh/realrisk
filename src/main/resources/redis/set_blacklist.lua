local key      = KEYS[1]
local reason   = ARGV[1]
local severity = tonumber(ARGV[2])
local ttl      = tonumber(ARGV[3])

local existing = redis.call('GET', key)
if existing then
  local cur_severity = tonumber(string.match(existing, ':(%d+)$'))
  if cur_severity and cur_severity >= severity then
    return 0
  end
end

local value = reason .. ':' .. severity
if ttl == 0 then
  redis.call('SET', key, value)
else
  redis.call('SET', key, value, 'EX', ttl)
end
return 1

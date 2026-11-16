"""Verify the sink's actual Lua in an isolated Redis namespace."""
import json
import pathlib
import re
import uuid

SINK = pathlib.Path(__file__).resolve().parents[1] / (
    "stream-job/src/main/java/io/srimaddireddy/telemetry/stream/RedisWindowSink.java"
)


def sink_lua():
    match = re.search(r'public static final String UPSERT\s*=\s*"""(.*?)""";',
                      SINK.read_text(), re.DOTALL)
    if match is None:
        raise ValueError("Cannot locate RedisWindowSink.UPSERT")
    return match.group(1)


def verify_projection(command):
    prefix = "{telemetry-check-" + uuid.uuid4().hex + "}"
    rows, index = prefix + ":windows", prefix + ":index"

    def redis(*arguments):
        return command(["exec", "-T", "redis", "redis-cli", "--raw", *arguments]).strip()

    script = sink_lua()
    try:
        # Seed 2,000 old windows in one call, then invoke the actual sink upsert.
        seed = """
        for i=1,2000 do
          redis.call('HSET',KEYS[1],tostring(i),'old')
          redis.call('ZADD',KEYS[2],i,tostring(i))
        end
        return 1
        """
        redis("EVAL", seed, "2", rows, index)
        snapshot = json.dumps(dict(impressions=7, spend=0.014))
        for _ in range(2):
            result = redis("EVAL", script, "2", rows, index,
                           "new", snapshot, "2001", "2000")
            assert result == "1", result
        assert redis("HLEN", rows) == "2000"
        assert redis("ZCARD", index) == "2000"
        assert redis("HEXISTS", rows, "1") == "0"
        assert redis("ZSCORE", index, "1") == ""
        assert redis("HGET", rows, "new") == snapshot
        assert redis("ZSCORE", index, "new") == "2001"
        assert redis("HEXISTS", rows, "2") == "1"
        return dict(idempotentProjectionReplay=True, retainedWindows=2000,
                    oldestWindowEvicted=True, productionSinkLua=True)
    finally:
        redis("DEL", rows, index)

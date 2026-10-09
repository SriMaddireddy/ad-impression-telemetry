package io.srimaddireddy.telemetry.stream;
import io.srimaddireddy.telemetry.model.*;
import java.util.List;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.JedisPooled;
public class RedisWindowSink extends RichSinkFunction<WindowReport> {
 private final String host;private final int port;private transient JedisPooled redis;
 public static final String UPSERT="""
 local key=ARGV[1]
 redis.call('HSET',KEYS[1],key,ARGV[2])
 redis.call('ZADD',KEYS[2],ARGV[3],key)
 local excess=redis.call('ZCARD',KEYS[2])-tonumber(ARGV[4])
 if excess>0 then
  local old=redis.call('ZRANGE',KEYS[2],0,excess-1)
  for _,item in ipairs(old) do redis.call('HDEL',KEYS[1],item);redis.call('ZREM',KEYS[2],item) end
 end
 return 1
 """;
 public RedisWindowSink(String host,int port){this.host=host;this.port=port;}
 @Override public void open(Configuration config){redis=new JedisPooled(host,port);redis.ping();}
 @Override public void invoke(WindowReport report,Context context){redis.eval(UPSERT,List.of("{telemetry}:windows","{telemetry}:window-index"),List.of(report.key,Json.write(report),String.valueOf(report.windowEnd),"2000"));}
 @Override public void close(){if(redis!=null)redis.close();}
}

package io.srimaddireddy.telemetry.ingest;
import io.srimaddireddy.telemetry.model.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import jakarta.annotation.PreDestroy;
import redis.clients.jedis.JedisPooled;
import org.springframework.stereotype.Service;
@Service
public class Projection {
 private final JedisPooled redis;private final Map<String,WindowReport> local=new ConcurrentHashMap<>();
 public Projection(Settings settings){redis=settings.kafkaMode()?new JedisPooled(settings.getRedisHost(),settings.getRedisPort()):null;}
 public void put(WindowReport w){local.put(w.key,w);if(local.size()>2000)local.values().stream().min(Comparator.comparingLong(x->x.windowEnd)).ifPresent(old->local.remove(old.key));}
 public List<WindowReport> list(int limit){
  if(redis==null)return local.values().stream().sorted(Comparator.comparingLong((WindowReport x)->x.windowEnd).reversed().thenComparing(x->x.campaignId)).limit(limit).toList();
  var keys=redis.zrevrange("{telemetry}:window-index",0,limit-1);if(keys.isEmpty())return List.of();
  List<WindowReport> rows=new ArrayList<>();for(String value:redis.hmget("{telemetry}:windows",keys.toArray(String[]::new)))if(value!=null)rows.add(Json.read(value,WindowReport.class));return rows;
 }
 @PreDestroy public void close(){if(redis!=null)redis.close();}
}

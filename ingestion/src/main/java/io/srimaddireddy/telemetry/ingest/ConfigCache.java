package io.srimaddireddy.telemetry.ingest;
import io.srimaddireddy.telemetry.model.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import jakarta.annotation.PreDestroy;
import redis.clients.jedis.JedisPooled;
import org.springframework.stereotype.Service;
@Service
public class ConfigCache {
 private final ConfigRepository repo;private final Settings settings;private final JedisPooled redis;
 private record Entry(CampaignConfig value,long expires){}
 private final Map<String,Entry> local=new ConcurrentHashMap<>();private final Map<String,LongAdder> counters=new ConcurrentHashMap<>();
 public ConfigCache(ConfigRepository repo,Settings settings){this.repo=repo;this.settings=settings;redis=settings.kafkaMode()?new JedisPooled(settings.getRedisHost(),settings.getRedisPort()):null;}
 private String key(String id){return "telemetry:config:"+id;}
 private void increment(String name){counters.computeIfAbsent(name,x->new LongAdder()).increment();if(redis!=null)try{redis.hincrBy("telemetry:cache-stats",name,1);}catch(RuntimeException ignored){counters.computeIfAbsent("redisErrors",x->new LongAdder()).increment();}}
 public CampaignConfig get(String id){
  increment("lookups");long now=System.currentTimeMillis();CampaignConfig cached=null;
  if(redis!=null){try{String value=redis.get(key(id));if(value!=null)cached=Json.read(value,CampaignConfig.class);}catch(RuntimeException e){increment("redisErrors");}}
  else{Entry e=local.get(id);if(e!=null&&e.expires()>now)cached=e.value();}
  if(cached!=null){increment("hits");return cached;}increment("misses");increment("databaseReads");CampaignConfig c=repo.get(id);
  if(redis!=null){try{redis.setex(key(id),settings.getConfigTtl(),Json.write(c));}catch(RuntimeException e){increment("redisErrors");}}
  else local.put(id,new Entry(c,now+settings.getConfigTtl()*1000L));return c;
 }
 public void update(CampaignConfig c){repo.update(c);local.remove(c.id());if(redis!=null)try{redis.del(key(c.id()));}catch(RuntimeException e){increment("redisErrors");}}
 public Map<String,Object> stats(){
  Map<String,Long> values=new LinkedHashMap<>();String scope=redis==null?"local-demo":"shared-redis";
  if(redis!=null)try{redis.hgetAll("telemetry:cache-stats").forEach((k,v)->values.put(k,Long.parseLong(v)));}catch(RuntimeException e){scope="node-local-redis-unavailable";}
  if(redis==null||scope.equals("node-local-redis-unavailable"))counters.forEach((k,v)->values.put(k,v.sum()));
  for(String name:List.of("lookups","hits","misses","databaseReads","redisErrors"))values.putIfAbsent(name,0L);
  long lookups=values.get("lookups"),reads=values.get("databaseReads");return Map.of("counters",values,"scope",scope,"avoidedReadsPercent",lookups==0?0:100.0*(1-(double)reads/lookups),"ttlSeconds",settings.getConfigTtl());
 }
 @PreDestroy public void close(){if(redis!=null)redis.close();}
}

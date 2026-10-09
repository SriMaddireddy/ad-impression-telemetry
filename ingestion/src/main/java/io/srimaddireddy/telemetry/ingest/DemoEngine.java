package io.srimaddireddy.telemetry.ingest;
import io.srimaddireddy.telemetry.model.*;
import java.util.*;
import org.springframework.stereotype.Service;
@Service
public class DemoEngine {
 private record Key(String campaign,long start){}
 private final Map<Key,Totals> pending=new HashMap<>();private final ConfigCache configs;private final Projection projection;
 private long watermark=Long.MIN_VALUE,late;
 public DemoEngine(ConfigCache configs,Projection projection){this.configs=configs;this.projection=projection;}
 public synchronized void accept(List<AdEvent> events){
  for(AdEvent e:events){long start=Math.floorDiv(e.eventTimeMs,5000)*5000;if(start+4999<=watermark){late++;continue;}pending.computeIfAbsent(new Key(e.campaignId,start),k->new Totals()).add(e);}
 }
 public synchronized void advance(long eventTime){
  watermark=Math.max(watermark,eventTime-2001);var iterator=pending.entrySet().iterator();
  while(iterator.hasNext()){var e=iterator.next();if(e.getKey().start()+4999<=watermark){var k=e.getKey();projection.put(WindowReport.from(k.campaign(),k.start(),k.start()+5000,e.getValue(),configs.get(k.campaign())));iterator.remove();}}
 }
 public synchronized Map<String,Object> stats(){return Map.of("lateEvents",late,"openWindows",pending.size(),"watermark",watermark);}
}

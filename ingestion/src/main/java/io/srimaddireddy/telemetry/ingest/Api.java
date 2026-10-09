package io.srimaddireddy.telemetry.ingest;
import io.srimaddireddy.telemetry.model.*;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.scheduling.annotation.Scheduled;
@RestController @RequestMapping("/api")
public class Api {
 private final ConfigCache configs;private final ConfigRepository repo;private final Publisher publisher;private final Projection projection;private final DemoEngine demo;private final Settings settings;
 private final String nodeId=UUID.randomUUID().toString();private final LongAdder accepted=new LongAdder();
 public Api(ConfigCache configs,ConfigRepository repo,Publisher publisher,Projection projection,DemoEngine demo,Settings settings){this.configs=configs;this.repo=repo;this.publisher=publisher;this.projection=projection;this.demo=demo;this.settings=settings;}
 @PostMapping("/events") public ResponseEntity<?> ingest(@RequestBody List<AdEvent> events){
  if(events==null||events.isEmpty()||events.size()>2000)throw new IllegalArgumentException("Batch must contain 1..2000 events");
  long now=System.currentTimeMillis();Set<String> ids=new HashSet<>(),campaigns=new HashSet<>();
  for(AdEvent e:events){if(e==null)throw new IllegalArgumentException("Null event");e.validate(false);if(e.eventTimeMs>now+2000||e.eventTimeMs<now-86400000)throw new IllegalArgumentException("Timestamp outside 24h lookback / 2s future skew");if(!ids.add(e.eventId))throw new IllegalArgumentException("Duplicate event ID in one batch");campaigns.add(e.campaignId);}
  // Validate the entire batch and all referenced campaigns before publishing any event.
  for(String id:campaigns)configs.get(id);publisher.publish(events);accepted.add(events.size());return ResponseEntity.accepted().body(Map.of("accepted",events.size(),"mode",settings.getMode(),"nodeId",nodeId));
 }
 @GetMapping("/configs") public List<CampaignConfig> configurations(){return repo.list();}
 @GetMapping("/configs/{id}") public CampaignConfig configuration(@PathVariable String id){return configs.get(id);}
 @PutMapping("/configs/{id}") public CampaignConfig update(@PathVariable String id,@RequestBody CampaignConfig c){c.validate();if(!id.equals(c.id()))throw new IllegalArgumentException("Path and config ID mismatch");configs.update(c);return c;}
 @GetMapping("/windows") public List<WindowReport> windows(@RequestParam(defaultValue="100") int limit){if(limit<1||limit>2000)throw new IllegalArgumentException("Limit must be 1..2000");return projection.list(limit);}
 @GetMapping("/stats") public Map<String,Object> stats(){return Map.of("mode",settings.getMode(),"nodeId",nodeId,"acceptedByNode",accepted.sum(),"cache",configs.stats(),"windowSizeSeconds",5,"outOfOrderSeconds",2,"demo",settings.kafkaMode()?Map.of():demo.stats());}
 @Scheduled(fixedDelay=1000) public void clock(){if(settings.isHeartbeatEnabled())try{publisher.heartbeat();}catch(RuntimeException ignored){}}
}

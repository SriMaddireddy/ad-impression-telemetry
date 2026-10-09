package io.srimaddireddy.telemetry.ingest;
import io.srimaddireddy.telemetry.model.*;
import java.util.*;
import java.util.concurrent.*;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;
@Service
public class Publisher {
 private final DemoEngine demo;private final KafkaProducer<String,String> producer;private final Settings settings;
 public Publisher(DemoEngine demo,Settings settings){
  this.demo=demo;this.settings=settings;if(!settings.kafkaMode()){producer=null;return;}
  Properties p=new Properties();p.put("bootstrap.servers",settings.getKafka());p.put("acks","all");p.put("enable.idempotence","true");p.put("linger.ms","5");p.put("compression.type","lz4");p.put("delivery.timeout.ms","30000");p.put("request.timeout.ms","10000");p.put("max.block.ms","10000");p.put("key.serializer",StringSerializer.class.getName());p.put("value.serializer",StringSerializer.class.getName());
  try(Admin admin=Admin.create(p)){
   var existing=admin.listTopics().names().get(20,TimeUnit.SECONDS);List<NewTopic> topics=new ArrayList<>();
   for(String name:List.of("ad.events","ad.windows","ad.dead-letter"))if(!existing.contains(name))topics.add(new NewTopic(name,6,(short)1));
   if(!topics.isEmpty())try{admin.createTopics(topics).all().get(20,TimeUnit.SECONDS);}catch(ExecutionException e){if(!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException))throw e;}
  }catch(Exception e){throw new IllegalStateException("Cannot initialize Kafka topics",e);}
  producer=new KafkaProducer<>(p);
 }
 public void publish(List<AdEvent> events){
  if(producer==null){demo.accept(events);return;}List<Future<RecordMetadata>> futures=new ArrayList<>();
  for(AdEvent e:events)futures.add(producer.send(new ProducerRecord<>("ad.events",e.campaignId,Json.write(e))));
  try{for(var f:futures)f.get(35,TimeUnit.SECONDS);}catch(Exception e){throw new IllegalStateException("Kafka acknowledgement failed; a partial batch may already be committed",e);}
 }
 public void heartbeat(){
  long now=System.currentTimeMillis();if(producer==null){demo.advance(now);return;}
  for(var partition:producer.partitionsFor("ad.events"))producer.send(new ProducerRecord<>("ad.events",partition.partition(),"_clock",Json.write(new AdEvent(UUID.randomUUID().toString(),"_clock","HEARTBEAT",now,0))));
 }
 @PreDestroy public void close(){if(producer!=null)producer.close();}
}

package io.srimaddireddy.telemetry.stream;
import io.srimaddireddy.telemetry.model.*;
import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.*;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.*;
public class TelemetryJob {
 public static final OutputTag<String> INVALID=new OutputTag<String>("invalid"){};
 public static final OutputTag<AdEvent> LATE=new OutputTag<AdEvent>("late"){};
 public static class Parse extends ProcessFunction<String,AdEvent>{
  public void processElement(String value,Context context,Collector<AdEvent> out){try{AdEvent e=Json.read(value,AdEvent.class);e.validate(true);out.collect(e);}catch(IllegalArgumentException e){context.output(INVALID,Json.write(java.util.Map.of("reason","INVALID_EVENT","raw",value)));}}
 }
 private static String env(String key,String fallback){return System.getenv().getOrDefault(key,fallback);}
 private static KafkaSink<String> sink(String bootstrap,String topic){Properties p=new Properties();p.setProperty("enable.idempotence","true");p.setProperty("acks","all");return KafkaSink.<String>builder().setBootstrapServers(bootstrap).setKafkaProducerConfig(p).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(topic).setValueSerializationSchema(new SimpleStringSchema()).build()).build();}
 public static void main(String[] args) throws Exception {
  String bootstrap=env("KAFKA_BOOTSTRAP_SERVERS","kafka:9092");var environment=StreamExecutionEnvironment.getExecutionEnvironment();
  environment.setParallelism(Integer.parseInt(env("FLINK_PARALLELISM","2")));environment.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE);
  environment.getCheckpointConfig().setCheckpointStorage(env("CHECKPOINT_DIR","file:///opt/flink/checkpoints"));
  environment.getCheckpointConfig().setCheckpointTimeout(60000);environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
  environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,org.apache.flink.api.common.time.Time.seconds(2)));
  var source=KafkaSource.<String>builder().setBootstrapServers(bootstrap).setTopics("ad.events").setGroupId("telemetry-flink-v1").setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema()).build();
  var parsed=environment.fromSource(source,WatermarkStrategy.noWatermarks(),"Kafka raw events").uid("raw-events").process(new Parse()).name("Validate events").uid("validate-events");
  var timed=parsed.assignTimestampsAndWatermarks(WatermarkStrategy.<AdEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner((event,time)->event.eventTimeMs).withIdleness(Duration.ofSeconds(3))).name("Event-time watermarks").uid("watermarks");
  var reports=timed.filter(e->!e.type.equals("HEARTBEAT")).name("Exclude clock records").uid("filter-heartbeats").keyBy(e->e.campaignId)
   .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5))).sideOutputLateData(LATE)
   .aggregate(new WindowAggregate(),new WindowFunction(env("CONFIG_API","http://ingest:8080"))).name("5-second campaign windows").uid("campaign-windows");
  reports.addSink(new RedisWindowSink(env("REDIS_HOST","redis"),Integer.parseInt(env("REDIS_PORT","6379")))).name("Idempotent Redis projection").uid("redis-windows");
  reports.map(Json::write).name("Window JSON").uid("window-json").sinkTo(sink(bootstrap,"ad.windows")).name("Kafka window output").uid("window-output");
  var late=reports.getSideOutput(LATE).map(e->Json.write(java.util.Map.of("reason","LATE_EVENT","event",e))).name("Late event JSON");
  parsed.getSideOutput(INVALID).union(late).sinkTo(sink(bootstrap,"ad.dead-letter")).name("Invalid and late events").uid("dead-letter");
  environment.execute("Ad impression telemetry / five-second event-time windows");
 }
}

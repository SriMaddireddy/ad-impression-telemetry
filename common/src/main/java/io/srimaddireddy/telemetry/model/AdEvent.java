package io.srimaddireddy.telemetry.model;
import java.io.Serializable;
import java.util.Set;
/** Public fields and a no-arg constructor keep Flink on its POJO serializer. */
public class AdEvent implements Serializable {
 public String eventId,campaignId,type;public long eventTimeMs;public double amount;
 public AdEvent(){}
 public AdEvent(String id,String campaign,String type,long time,double amount){this.eventId=id;this.campaignId=campaign;this.type=type;this.eventTimeMs=time;this.amount=amount;}
 public void validate(boolean allowHeartbeat){
  if(eventId==null||eventId.isBlank()||eventId.length()>100||campaignId==null||!campaignId.matches("[A-Za-z0-9_-]{1,80}"))throw new IllegalArgumentException("Invalid event or campaign ID");
  if(type==null||!Set.of("IMPRESSION","CLICK","CONVERSION","HEARTBEAT").contains(type)||(!allowHeartbeat&&type.equals("HEARTBEAT")))throw new IllegalArgumentException("Unsupported event type");
  if(eventTimeMs<=0||!Double.isFinite(amount)||amount<0||amount>1000000)throw new IllegalArgumentException("Invalid timestamp or amount");
  if((type.equals("CLICK")||type.equals("HEARTBEAT"))&&amount!=0)throw new IllegalArgumentException("Clicks and heartbeats must have zero amount");
 }
}

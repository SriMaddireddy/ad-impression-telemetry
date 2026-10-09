package io.srimaddireddy.telemetry.ingest;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
@Component @ConfigurationProperties(prefix="telemetry")
public class Settings {
 private String mode="demo",kafka,redisHost;private int redisPort,configTtl;private boolean heartbeatEnabled=true;
 public String getMode(){return mode;}public void setMode(String x){mode=x;}public String getKafka(){return kafka;}public void setKafka(String x){kafka=x;}
 public String getRedisHost(){return redisHost;}public void setRedisHost(String x){redisHost=x;}public int getRedisPort(){return redisPort;}public void setRedisPort(int x){redisPort=x;}
 public int getConfigTtl(){return configTtl;}public void setConfigTtl(int x){configTtl=x;}public boolean isHeartbeatEnabled(){return heartbeatEnabled;}public void setHeartbeatEnabled(boolean x){heartbeatEnabled=x;}
 public boolean kafkaMode(){if(!mode.equals("demo")&&!mode.equals("kafka"))throw new IllegalStateException("Mode must be demo or kafka");return mode.equals("kafka");}
}

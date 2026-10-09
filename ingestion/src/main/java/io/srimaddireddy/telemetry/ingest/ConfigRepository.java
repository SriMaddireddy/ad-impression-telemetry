package io.srimaddireddy.telemetry.ingest;
import io.srimaddireddy.telemetry.model.CampaignConfig;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
@Repository
public class ConfigRepository {
 private final JdbcTemplate db;private final TransactionTemplate tx;
 public ConfigRepository(JdbcTemplate db,TransactionTemplate tx){this.db=db;this.tx=tx;}
 public CampaignConfig get(String id){return db.query("SELECT * FROM campaign_configs WHERE id=?",(r,n)->new CampaignConfig(r.getString(1),r.getString(2),r.getDouble(3),r.getLong(4)),id).stream().findFirst().orElseThrow(()->new NoSuchElementException("Unknown campaign"));}
 public List<CampaignConfig> list(){return db.query("SELECT * FROM campaign_configs ORDER BY id",(r,n)->new CampaignConfig(r.getString(1),r.getString(2),r.getDouble(3),r.getLong(4)));}
 public void update(CampaignConfig c){c.validate();int count=db.update("UPDATE campaign_configs SET name=?,max_ctr=?,max_impressions=? WHERE id=?",c.name(),c.maxCtr(),c.maxImpressionsPerWindow(),c.id());if(count==0)throw new NoSuchElementException("Unknown campaign");}
 @Bean ApplicationRunner seed(){return args->tx.executeWithoutResult(s->{for(int i=1;i<=25;i++){String id=String.format(Locale.ROOT,"campaign-%03d",i);if(db.queryForObject("SELECT COUNT(*) FROM campaign_configs WHERE id=?",Integer.class,id)==0)db.update("INSERT INTO campaign_configs VALUES (?,?,?,?)",id,"Campaign "+i,.15,i==1?500L:5000L);}});}
}

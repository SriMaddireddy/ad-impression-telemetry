package io.srimaddireddy.telemetry.ingest;
import io.srimaddireddy.telemetry.model.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.http.MediaType;
@SpringBootTest(properties={"spring.datasource.url=jdbc:h2:mem:telemetrytest;DB_CLOSE_DELAY=-1","telemetry.mode=demo","telemetry.heartbeat-enabled=false"}) @AutoConfigureMockMvc
class ApiTest {
 @Autowired MockMvc http;@Autowired ConfigCache configs;@Autowired ConfigRepository repo;
 @Test void acceptsValidatedBatchAndReportsMode() throws Exception {var e=new AdEvent(UUID.randomUUID().toString(),"campaign-002","IMPRESSION",System.currentTimeMillis(),.01);http.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(Json.write(List.of(e)))).andExpect(status().isAccepted()).andExpect(jsonPath("$.accepted").value(1)).andExpect(jsonPath("$.mode").value("demo"));}
 @Test void rejectsDuplicateIdsAndUnknownCampaign() throws Exception {var e=new AdEvent("repeat","campaign-002","CLICK",System.currentTimeMillis(),0);http.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(Json.write(List.of(e,e)))).andExpect(status().isBadRequest());e.campaignId="missing";http.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(Json.write(List.of(e)))).andExpect(status().isNotFound());}
 @Test void rejectsMalformedAndOversizedBatches() throws Exception {http.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content("[]")).andExpect(status().isBadRequest());var e=new AdEvent("a","campaign-002","HEARTBEAT",System.currentTimeMillis(),0);http.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(Json.write(List.of(e)))).andExpect(status().isBadRequest());e.type="CLICK";e.eventTimeMs=System.currentTimeMillis()+10000;http.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(Json.write(List.of(e)))).andExpect(status().isBadRequest());http.perform(get("/api/windows?limit=0")).andExpect(status().isBadRequest());}
 @Test void cacheHitAvoidsAnotherDatabaseReadAndUpdateInvalidates(){var before=(Map<?,?>)configs.stats().get("counters");long reads=((Number)before.get("databaseReads")).longValue();var c=configs.get("campaign-023");configs.get("campaign-023");var after=(Map<?,?>)configs.stats().get("counters");assertEquals(reads+1,((Number)after.get("databaseReads")).longValue());configs.update(new CampaignConfig(c.id(),"Updated",.2,600));assertEquals("Updated",configs.get(c.id()).name());}
 @Test void seedHas25Configs() throws Exception {http.perform(get("/api/configs")).andExpect(jsonPath("$.length()").value(25));}
}

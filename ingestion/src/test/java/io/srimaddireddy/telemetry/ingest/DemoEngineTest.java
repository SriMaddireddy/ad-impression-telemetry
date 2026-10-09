package io.srimaddireddy.telemetry.ingest;
import io.srimaddireddy.telemetry.model.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.*;
import org.junit.jupiter.api.Test;
class DemoEngineTest {
 @Test void windowsCloseAtBoundaryAndRouteLateEvents(){var cache=mock(ConfigCache.class);when(cache.get("c")).thenReturn(new CampaignConfig("c","C",.15,500));var settings=new Settings();settings.setMode("demo");var projection=new Projection(settings);var engine=new DemoEngine(cache,projection);engine.accept(List.of(new AdEvent("1","c","IMPRESSION",5100,.01),new AdEvent("2","c","CLICK",9999,0),new AdEvent("3","c","IMPRESSION",10000,.01)));engine.advance(11999);assertTrue(projection.list(10).isEmpty());engine.advance(12000);var rows=projection.list(10);assertEquals(1,rows.size());assertEquals(5000,rows.get(0).windowStart);assertEquals(1,rows.get(0).impressions);assertEquals(1,rows.get(0).clicks);engine.accept(List.of(new AdEvent("late","c","CLICK",5100,0)));assertEquals(1L,engine.stats().get("lateEvents"));engine.advance(17000);assertEquals(2,projection.list(10).size());}
 @Test void projectionOverwriteIsIdempotent(){var s=new Settings();s.setMode("demo");var p=new Projection(s);var w=WindowReport.from("c",0,5000,new Totals(),new CampaignConfig("c","C",.2,100));p.put(w);p.put(w);assertEquals(1,p.list(10).size());}
}

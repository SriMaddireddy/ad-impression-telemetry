package io.srimaddireddy.telemetry.stream;
import io.srimaddireddy.telemetry.model.*;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.junit.jupiter.api.Test;
class WindowAggregateTest {
 @Test void usesFlinkPojoSerializers(){assertInstanceOf(PojoTypeInfo.class,TypeExtractor.getForClass(AdEvent.class));assertInstanceOf(PojoTypeInfo.class,TypeExtractor.getForClass(Totals.class));assertInstanceOf(PojoTypeInfo.class,TypeExtractor.getForClass(WindowReport.class));}
 @Test void flinkAggregateCountsAndMerges(){var f=new WindowAggregate();var a=f.add(new AdEvent("a","c","IMPRESSION",1,.02),f.createAccumulator());var b=f.add(new AdEvent("b","c","CLICK",1,0),f.createAccumulator());var t=f.getResult(f.merge(a,b));assertEquals(1,t.impressions);assertEquals(1,t.clicks);assertEquals(.02,t.spend,1e-9);}
}

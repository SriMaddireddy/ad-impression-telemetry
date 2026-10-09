package io.srimaddireddy.telemetry.stream;
import io.srimaddireddy.telemetry.model.*;
import org.apache.flink.api.common.functions.AggregateFunction;
public class WindowAggregate implements AggregateFunction<AdEvent,Totals,Totals> {
 public Totals createAccumulator(){return new Totals();}public Totals add(AdEvent e,Totals t){return t.add(e);}public Totals getResult(Totals t){return t;}public Totals merge(Totals a,Totals b){return a.merge(b);}
}

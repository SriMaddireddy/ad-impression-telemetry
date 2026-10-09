package io.srimaddireddy.telemetry.stream;
import io.srimaddireddy.telemetry.model.*;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
/** Query the ingestion API, which performs Redis cache-aside reads from PostgreSQL. */
public class WindowFunction extends ProcessWindowFunction<Totals,WindowReport,String,TimeWindow> {
 private final String configApi;private transient HttpClient client;
 public WindowFunction(String configApi){this.configApi=configApi;}
 public void process(String key,Context context,Iterable<Totals> rows,Collector<WindowReport> out) throws Exception {
  if(client==null)client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
  var response=client.send(HttpRequest.newBuilder(URI.create(configApi+"/api/configs/"+key)).timeout(Duration.ofSeconds(3)).GET().build(),HttpResponse.BodyHandlers.ofString());
  if(response.statusCode()!=200)throw new IllegalStateException("Config lookup failed: "+response.statusCode());
  CampaignConfig config=Json.read(response.body(),CampaignConfig.class);var window=context.window();out.collect(WindowReport.from(key,window.getStart(),window.getEnd(),rows.iterator().next(),config));
 }
}

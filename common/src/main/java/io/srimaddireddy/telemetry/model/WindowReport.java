package io.srimaddireddy.telemetry.model;
import java.io.Serializable;
import java.util.ArrayList;
public class WindowReport implements Serializable {
 public String campaignId,key,anomalyReason;public long windowStart,windowEnd,impressions,clicks,conversions;
 public double spend,revenue,ctr,conversionRate,roas;public boolean anomaly;
 public WindowReport(){}
 public static WindowReport from(String campaign,long start,long end,Totals totals,CampaignConfig config){
  WindowReport w=new WindowReport();w.campaignId=campaign;w.key=campaign+":"+start;w.windowStart=start;w.windowEnd=end;
  w.impressions=totals.impressions;w.clicks=totals.clicks;w.conversions=totals.conversions;w.spend=totals.spend;w.revenue=totals.revenue;
  w.ctr=w.impressions==0?0:(double)w.clicks/w.impressions;w.conversionRate=w.clicks==0?0:(double)w.conversions/w.clicks;w.roas=w.spend==0?0:w.revenue/w.spend;
  var reasons=new ArrayList<String>();if(w.impressions>=100&&w.ctr>config.maxCtr())reasons.add("CTR above "+config.maxCtr());
  if(w.impressions>config.maxImpressionsPerWindow())reasons.add("Impression burst above "+config.maxImpressionsPerWindow());
  w.anomaly=!reasons.isEmpty();w.anomalyReason=String.join("; ",reasons);return w;
 }
}

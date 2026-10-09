package io.srimaddireddy.telemetry.model;
import java.io.Serializable;
public class Totals implements Serializable {
 public long impressions,clicks,conversions;public double spend,revenue;
 public Totals(){}
 public Totals add(AdEvent e){switch(e.type){case "IMPRESSION"-> {impressions++;spend+=e.amount;}case "CLICK"->clicks++;case "CONVERSION"->{conversions++;revenue+=e.amount;}default->throw new IllegalArgumentException("Not a metric event");}return this;}
 public Totals merge(Totals other){impressions+=other.impressions;clicks+=other.clicks;conversions+=other.conversions;spend+=other.spend;revenue+=other.revenue;return this;}
}

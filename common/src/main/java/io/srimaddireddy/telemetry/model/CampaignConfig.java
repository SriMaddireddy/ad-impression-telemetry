package io.srimaddireddy.telemetry.model;
public record CampaignConfig(String id,String name,double maxCtr,long maxImpressionsPerWindow) {
 public void validate(){if(id==null||!id.matches("[A-Za-z0-9_-]{1,80}")||name==null||name.isBlank()||name.length()>100||!Double.isFinite(maxCtr)||maxCtr<=0||maxCtr>1||maxImpressionsPerWindow<1)throw new IllegalArgumentException("Invalid campaign configuration");}
}

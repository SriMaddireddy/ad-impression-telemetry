#!/usr/bin/env python3
import argparse,json,time,uuid,urllib.request
p=argparse.ArgumentParser();p.add_argument('--url',default='http://127.0.0.1:8090');p.add_argument('--expect-mode',default='demo');args=p.parse_args()
def request(path,data=None,method='POST'):
 req=urllib.request.Request(args.url+path,data=None if data is None else json.dumps(data).encode(),headers={'Content-Type':'application/json'},method='GET' if data is None else method)
 with urllib.request.urlopen(req,timeout=60) as response:return json.load(response)
assert request('/api/stats')['mode']==args.expect_mode
# Align to a new window and use an isolated stack for exact count assertions.
time.sleep((5000-int(time.time()*1000)%5000)/1000+.1)
now=int(time.time()*1000);start=now//5000*5000;events=[]
for campaign,impressions,clicks,conversions in [('campaign-001',700,20,5),('campaign-002',200,6,2)]:
 for kind,count,amount in [('IMPRESSION',impressions,.002),('CLICK',clicks,0),('CONVERSION',conversions,5)]:
  events.extend({'eventId':str(uuid.uuid4()),'campaignId':campaign,'type':kind,'eventTimeMs':now,'amount':amount} for _ in range(count))
assert request('/api/events',events)['accepted']==len(events)
deadline=time.monotonic()+90;found={}
while time.monotonic()<deadline:
 rows=request('/api/windows?limit=2000');found={w['campaignId']:w for w in rows if w['windowStart']==start and w['campaignId'] in ('campaign-001','campaign-002')}
 if len(found)==2:break
 time.sleep(.5)
assert len(found)==2,found
for campaign,i,c,v in [('campaign-001',700,20,5),('campaign-002',200,6,2)]:
 w=found[campaign];assert (w['impressions'],w['clicks'],w['conversions'])==(i,c,v),w;assert abs(w['ctr']-c/i)<1e-9;assert abs(w['conversionRate']-v/c)<1e-9
assert found['campaign-001']['anomaly'];assert not found['campaign-002']['anomaly']
# Warm once, then measure actual read avoidance across repeated cache lookups.
request('/api/configs/campaign-025');before=request('/api/stats')['cache']['counters'];latencies=[]
for _ in range(100):
 begin=time.perf_counter();request('/api/configs/campaign-025');latencies.append((time.perf_counter()-begin)*1000)
after=request('/api/stats')['cache']['counters'];lookups=after['lookups']-before['lookups'];reads=after['databaseReads']-before['databaseReads'];assert lookups>=100;assert reads==0,(before,after)
print(json.dumps({'status':'PASS','mode':args.expect_mode,'eventsVerified':len(events),'closedWindows':2,'anomaliesVerified':1,'cacheChecks':100,'databaseReadsDuringChecks':reads,'clientConfigP95Ms':sorted(latencies)[94],'windowStart':start},indent=2))

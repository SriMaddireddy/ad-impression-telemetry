#!/usr/bin/env python3
"""Synthetic load with per-batch timestamps and full window-count verification. Stdlib only."""
import argparse,json,time,uuid,urllib.request,pathlib,platform
from verification import verify_window
p=argparse.ArgumentParser();p.add_argument('--url',default='http://127.0.0.1:8090');p.add_argument('--count',type=int,default=10000);p.add_argument('--rate',type=float,default=0);p.add_argument('--output',default='benchmark.json');args=p.parse_args()
if args.count<1 or args.rate<0:p.error('count must be positive, rate nonnegative')
def request(path,data=None):
 req=urllib.request.Request(args.url+path,data=None if data is None else json.dumps(data).encode(),headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=60) as response:return json.load(response)
time.sleep((5000-int(time.time()*1000)%5000)/1000+.1)
run=str(uuid.uuid4());expected={};sent=0;started=time.monotonic();initial=request('/api/stats')
while sent<args.count:
 batch=[];now=int(time.time()*1000)
 for i in range(sent,min(sent+1000,args.count)):
  campaign=f'campaign-{2+i%24:03d}';kind='IMPRESSION' if i%100<96 else 'CLICK' if i%100<99 else 'CONVERSION'
  batch.append({'eventId':f'{run}-{i}','campaignId':campaign,'type':kind,'eventTimeMs':now,'amount':.002 if kind=='IMPRESSION' else 5 if kind=='CONVERSION' else 0})
  key=(campaign,now//5000*5000);expected.setdefault(key,{'impressions':0,'clicks':0,'conversions':0,'spend':0.0,'revenue':0.0})
  counts=expected[key]
  counts[{'IMPRESSION':'impressions','CLICK':'clicks','CONVERSION':'conversions'}[kind]]+=1
  if kind=='IMPRESSION':counts['spend']+=.002
  if kind=='CONVERSION':counts['revenue']+=5
 assert request('/api/events',batch)['accepted']==len(batch);sent+=len(batch)
 if args.rate:
  pause=sent/args.rate-(time.monotonic()-started)
  if pause>0:time.sleep(pause)
 if sent%100000==0:print(f'Acknowledged {sent:,} events',flush=True)
ingestion_seconds=time.monotonic()-started;deadline=time.monotonic()+180;found={}
while time.monotonic()<deadline:
 found={(w['campaignId'],w['windowStart']):w for w in request('/api/windows?limit=2000')}
 if all(key in found for key in expected):break
 time.sleep(.5)
missing=[key for key in expected if key not in found]
if missing:raise SystemExit(f'Missing {len(missing)} windows; use smaller/shorter runs within the 2,000-window retention cap')
for key,counts in expected.items():
 verify_window(key,found[key],counts)
final=request('/api/stats');report={'measuredAtUtc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'clientPlatform':platform.platform(),'mode':final['mode'],'acknowledgedEvents':sent,'verifiedEvents':sum(x['impressions']+x['clicks']+x['conversions'] for x in expected.values()),'verifiedWindows':len(expected),'verifiedFields':['impressions','clicks','conversions','spend','revenue'],'ingestionSeconds':round(ingestion_seconds,3),'acknowledgedEventsPerSecond':round(sent/ingestion_seconds,2),'endToEndSeconds':round(time.monotonic()-started,3),'initialStats':initial,'finalStats':final,'caveats':['Synthetic isolated workload; not production data.','Ingestion throughput includes HTTP client serialization and broker acknowledgements in Kafka mode.','Five-second windows have two-second out-of-order tolerance.','Demo mode does not exercise Kafka, Flink, or Redis.']}
pathlib.Path(args.output).write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))

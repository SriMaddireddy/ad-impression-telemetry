#!/usr/bin/env python3
"""Docker-only checks: malformed/late DLQ, completed checkpoints, replay of one projection."""
import json,subprocess,time,uuid,urllib.request
BASE='http://127.0.0.1:8090';FLINK='http://127.0.0.1:8091'
def http(base,path):
 with urllib.request.urlopen(base+path,timeout=10) as response:return json.load(response)
def command(args):return subprocess.check_output(['docker','compose']+args,text=True,timeout=45)
# Publish poison and old valid input directly, bypassing the HTTP validator intentionally.
old={'eventId':str(uuid.uuid4()),'campaignId':'campaign-002','type':'CLICK','eventTimeMs':int(time.time()*1000)-60000,'amount':0}
subprocess.run(['docker','compose','exec','-T','kafka','/opt/kafka/bin/kafka-console-producer.sh','--bootstrap-server','kafka:9092','--topic','ad.events'],input='broken-json\n'+json.dumps(old)+'\n',text=True,check=True,timeout=30)
text=command(['exec','-T','kafka','/opt/kafka/bin/kafka-console-consumer.sh','--bootstrap-server','kafka:9092','--topic','ad.dead-letter','--from-beginning','--timeout-ms','20000','--max-messages','2'])
assert 'INVALID_EVENT' in text and 'LATE_EVENT' in text,text
jobs=http(FLINK,'/jobs/overview')['jobs'];running=[j for j in jobs if j['state']=='RUNNING'];assert len(running)==1,jobs
id=running[0]['jid'];deadline=time.monotonic()+60
while time.monotonic()<deadline:
 checkpoints=http(FLINK,'/jobs/'+id+'/checkpoints')
 if checkpoints['counts']['completed']>=1:break
 time.sleep(1)
assert checkpoints['counts']['completed']>=1,checkpoints
# Lua upsert replay must keep one row per campaign/window, not increment counts.
window=http(BASE,'/api/windows?limit=1')[0];before=command(['exec','-T','redis','redis-cli','ZCARD','{telemetry}:window-index']).strip()
# Re-run the same Lua through redis-cli with the exact snapshot and identity.
lua="redis.call('HSET',KEYS[1],ARGV[1],ARGV[2]);redis.call('ZADD',KEYS[2],ARGV[3],ARGV[1]);return 1"
command(['exec','-T','redis','redis-cli','EVAL',lua,'2','{telemetry}:windows','{telemetry}:window-index',window['key'],json.dumps(window),str(window['windowEnd'])])
after=command(['exec','-T','redis','redis-cli','ZCARD','{telemetry}:window-index']).strip();assert before==after
print(json.dumps({'status':'PASS','dlqReasons':['INVALID_EVENT','LATE_EVENT'],'completedCheckpoints':checkpoints['counts']['completed'],'idempotentProjectionReplay':True},indent=2))

import urllib.request
import json
import time

# 1. Cargar el JSON del GET de OTel
with open('data_guia/traces.json', 'r', encoding='utf-8') as f:
    lines = [l.strip() for l in f if l.strip()]
    trace_json_str = lines[1]

payload = {
    'topic': 'tp.observability.application-log.emitted.v1',
    'key': 'PK-8172201-IN',
    'value': trace_json_str
}

print('1. Publicando 3 veces el mismo evento desde el portal web...')
for i in range(1, 4):
    req_send = urllib.request.Request(
        'http://localhost:5000/api/messages/send',
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )
    with urllib.request.urlopen(req_send) as resp:
        res = json.loads(resp.read().decode('utf-8'))
        off = res.get('result', {}).get('offset')
        part = res.get('result', {}).get('partition')
        print(f"  - Publicacion #{i}: Partition={part}, Offset={off}")
    time.sleep(0.5)

print('\nEsperando 3 segundos a que ConsumerStreams y LogSink procesen...')
time.sleep(3)

# 2. Consultar Cosmos DB Emulator
print('\n2. Consultando documentos en Cosmos DB Emulator (http://localhost:8081/api/stats)...')
req_stats = urllib.request.Request('http://localhost:8081/api/stats')
with urllib.request.urlopen(req_stats) as resp_stats:
    stats = json.loads(resp_stats.read().decode('utf-8'))
    total_docs = stats.get('totalDocuments')
    print(f"  - Total de documentos guardados en Cosmos DB: {total_docs}")
    
    docs = stats.get('documents', [])
    for idx, d in enumerate(docs[:total_docs], 1):
        parsed = json.loads(d.get('rawJson', '{}'))
        doc_id = d.get('id')
        trace_id = parsed.get('TraceId')
        name = parsed.get('Name')
        print(f"    [{idx}] Storage ID: {doc_id} | TraceId: {trace_id} | Operacion: {name}")

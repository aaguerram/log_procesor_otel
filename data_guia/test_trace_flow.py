import urllib.request
import json
import time

# 1. Cargar el JSON de la traza GET
with open('emisor_mensaje/src/KafkaDemo.Web/wwwroot/data/otel_get_trace.json', 'r', encoding='utf-8') as f:
    trace_obj = json.load(f)

trace_json_str = json.dumps(trace_obj)

payload = {
    'topic': 'tp.observability.application-log.emitted.v1',
    'key': 'PK-8172201-IN',
    'value': trace_json_str
}

req = urllib.request.Request(
    'http://localhost:5000/api/messages/send',
    data=json.dumps(payload).encode('utf-8'),
    headers={'Content-Type': 'application/json'}
)

print('1. Emitiendo traza OTel GET a Kafka...')
with urllib.request.urlopen(req, timeout=10) as resp:
    res_data = json.loads(resp.read().decode('utf-8'))
    print('  - Resultado de emision:', res_data)

time.sleep(3)

# 2. Consultar el documento guardado en Cosmos DB Emulator (puerto 8081)
print('\n2. Consultando documentos en Cosmos DB (http://localhost:8081/dbs/ProdubancoObservability/colls/audit_logs/docs)...')
req_cosmos = urllib.request.Request('http://localhost:8081/dbs/ProdubancoObservability/colls/audit_logs/docs')
with urllib.request.urlopen(req_cosmos, timeout=10) as resp_cosmos:
    cosmos_data = json.loads(resp_cosmos.read().decode('utf-8'))
    docs = cosmos_data.get('Documents', [])
    print(f'  - Total documentos en Cosmos DB: {len(docs)}')
    
    # Tomar el ultimo documento guardado
    latest = docs[-1] if docs else {}
    print('\n3. Estructura del ultimo documento persistido:')
    print('  - Id:', latest.get('id'))
    print('  - TraceId:', latest.get('traceId'))
    print('  - SpanId:', latest.get('spanId'))
    print('  - ParentSpanId:', latest.get('parentSpanId'))
    print('  - Name:', latest.get('name'))
    print('  - Kind:', latest.get('kind'))
    print('  - Tags count:', len(latest.get('tags', {})) if latest.get('tags') else 0)
    print('  - ResponseBodyPreview presente:', bool(latest.get('responseBodyPreview')))
    if latest.get('responseBodyPreview'):
        resp_obj = json.loads(latest['responseBodyPreview'])
        contactos = resp_obj.get('Value', {}).get('Contactos', [])
        print(f'  - Total Contactos en responseBodyPreview: {len(contactos)} contactos')
        print(f'  - Primer contacto: {contactos[0]}')
    print('  - RawPayload presente:', bool(latest.get('rawPayload')), f"(tamano: {len(latest.get('rawPayload', ''))} bytes)")

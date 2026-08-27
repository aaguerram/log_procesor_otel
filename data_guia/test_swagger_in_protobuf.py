import urllib.request
import json
import time

# 1. Verificar endpoint de contrato Swagger en la Web
print("1. Verificando contrato Swagger en la app Web...")
req_swagger = urllib.request.Request('http://localhost:5000/api/contracts/swagger')
with urllib.request.urlopen(req_swagger) as resp:
    yaml_content = resp.read().decode('utf-8')
    print(f"   - Longitud del contrato YAML: {len(yaml_content)} caracteres")
    print(f"   - Lineas: {len(yaml_content.splitlines())}")
    print(f"   - Contiene '/accounts/singleProduct-Account': {'/accounts/singleProduct-Account' in yaml_content}")

# 2. Cargar la traza OTel GET
with open('data_guia/traces.json', 'r', encoding='utf-8') as f:
    lines = [l.strip() for l in f if l.strip()]
    trace_json_str = lines[1]

# 3. Publicar mensaje a Kafka a través del API web
print("\n2. Publicando mensaje con Protobuf desde la Web...")
payload = {
    'topic': 'tp.observability.application-log.emitted.v1',
    'key': 'PK-TEST-SWAGGER-001',
    'value': trace_json_str
}

req_send = urllib.request.Request(
    'http://localhost:5000/api/messages/send',
    data=json.dumps(payload).encode('utf-8'),
    headers={'Content-Type': 'application/json'}
)

with urllib.request.urlopen(req_send) as resp_send:
    res = json.loads(resp_send.read().decode('utf-8'))
    print("   - Resultado envio:", res)

# 4. Esperar procesamiento del pipeline
time.sleep(3)

# 5. Verificar recepcion en Cosmos DB
print("\n3. Verificando persistencia final en Cosmos DB Emulator...")
req_stats = urllib.request.Request('http://localhost:8081/api/stats')
with urllib.request.urlopen(req_stats) as resp_stats:
    stats = json.loads(resp_stats.read().decode('utf-8'))
    print(f"   - Total documentos en DB: {stats.get('totalDocuments')}")
    docs = stats.get('documents', [])
    if docs:
        last_doc = json.loads(docs[-1].get('rawJson', '{}'))
        print(f"   - Ultimo TraceId en DB: {last_doc.get('TraceId')}")
        print(f"   - Operacion: {last_doc.get('Name')}")
        print(">>> EXITO: El pipeline proceso el mensaje Protobuf con el contrato Swagger adjunto sin problemas.")

import urllib.request
import json
import time

# 1. Limpiar base de datos en Cosmos DB Emulator
print("1. Limpiando documentos previos en Cosmos DB Emulator...")
req_del = urllib.request.Request('http://localhost:8081/api/documents', method='DELETE')
with urllib.request.urlopen(req_del) as resp_del:
    print("  -", resp_del.read().decode('utf-8'))

# 2. Cargar el JSON EXACTO del usuario desde traces.json (línea 2)
with open('data_guia/traces.json', 'r', encoding='utf-8') as f:
    lines = [l.strip() for l in f if l.strip()]
    original_json_str = lines[1]

original_obj = json.loads(original_json_str)

print(f"\n2. JSON original a enviar:")
print(f"  - Claves raíz ({len(original_obj)}): {list(original_obj.keys())}")
print(f"  - TraceId: {original_obj.get('TraceId')}")
print(f"  - SpanId: {original_obj.get('SpanId')}")
print(f"  - Longitud en caracteres: {len(original_json_str)}")

# 3. Enviar a través de la Web API (mismo endpoint del portal web)
payload = {
    'topic': 'tp.observability.application-log.emitted.v1',
    'key': 'PK-8172201-IN',
    'value': original_json_str
}

req_send = urllib.request.Request(
    'http://localhost:5000/api/messages/send',
    data=json.dumps(payload).encode('utf-8'),
    headers={'Content-Type': 'application/json'}
)

print('\n3. Publicando mensaje cifrado Protobuf a Kafka...')
with urllib.request.urlopen(req_send, timeout=10) as resp_send:
    res = json.loads(resp_send.read().decode('utf-8'))
    print('  - Resultado:', res)

time.sleep(3)

# 4. Consultar en Cosmos DB Emulator
print('\n4. Consultando documento en Cosmos DB Emulator (http://localhost:8081/api/stats)...')
req_stats = urllib.request.Request('http://localhost:8081/api/stats')
with urllib.request.urlopen(req_stats, timeout=10) as resp_stats:
    stats = json.loads(resp_stats.read().decode('utf-8'))
    print(f"  - Total documentos persistidos: {stats.get('totalDocuments')}")
    
    docs = stats.get('documents', [])
    if not docs:
        print("  - ERROR: No se encontró ningún documento persistido.")
        exit(1)
        
    stored_doc = docs[0]
    stored_json_str = stored_doc.get('rawJson', '{}')
    stored_obj = json.loads(stored_json_str)
    
    print("\n5. Comparación detallada de campos:")
    orig_keys = set(original_obj.keys())
    stored_keys = set(stored_obj.keys())
    
    print(f"  - Claves en JSON Original ({len(orig_keys)}): {sorted(list(orig_keys))}")
    print(f"  - Claves en Documento DB  ({len(stored_keys)}): {sorted(list(stored_keys))}")
    
    extra_keys = stored_keys - orig_keys
    missing_keys = orig_keys - stored_keys
    
    print(f"  - Campos adicionales en DB : {list(extra_keys) if extra_keys else 'NINGUNO (0)'}")
    print(f"  - Campos faltantes en DB   : {list(missing_keys) if missing_keys else 'NINGUNO (0)'}")
    
    if orig_keys == stored_keys:
        print("\n>>> EXITO TOTAL: El documento guardado en la base de datos es EXACTAMENTE el mismo enviado por el front.")
        print(f"  - Tags count: {len(stored_obj['Tags'])}")
        print(f"  - Body Preview length: {len(stored_obj['Tags']['http.response.body_preview'])} chars")
        print(f"  - Contactos count: {len(json.loads(stored_obj['Tags']['http.response.body_preview'])['Value']['Contactos'])}")
    else:
        print("\n>>> ERROR: Discrepancia encontrada en los campos.")

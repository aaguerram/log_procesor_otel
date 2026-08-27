import json

# Leer la línea 2 exacta del archivo traces.json
with open('data_guia/traces.json', 'r', encoding='utf-8') as f:
    lines = [l.strip() for l in f if l.strip()]
    get_trace_line = lines[1] # Línea 2: GET /contacts/contacts-by-idClient/{idClient:int}/{channel}

parsed = json.loads(get_trace_line)

# Guardar en otel_get_trace.json en formato JSON exacto identado
with open('emisor_mensaje/src/KafkaDemo.Web/wwwroot/data/otel_get_trace.json', 'w', encoding='utf-8') as out:
    json.dump(parsed, out, indent=2, ensure_ascii=False)

print(f"Guardado exitosamente: {len(json.dumps(parsed))} caracteres, TraceId={parsed['TraceId']}")

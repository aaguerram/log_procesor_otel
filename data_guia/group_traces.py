import json
import yaml
import os
import re

out_dir = 'data_guia/traces_agrupados'
os.makedirs(out_dir, exist_ok=True)

# 1. Cargar Swagger YAML
swagger_file = 'data_guia/transfer-mspx-prometeus.management.standard.yaml'
with open(swagger_file, 'r', encoding='utf-8') as f:
    swagger = yaml.safe_load(f)

# 2. Cargar Traces
traces_file = 'data_guia/traces.json'
with open(traces_file, 'r', encoding='utf-8') as f:
    traces = [json.loads(line) for line in f if line.strip()]

def find_swagger_match(method, path, name):
    clean_path = path.replace('/transfer-mspx-prometeus-management', '')
    if not clean_path:
        clean_path = '/'
    
    # Buscar match exacto o por regex
    for sw_path, sw_methods in swagger.get('paths', {}).items():
        if method.lower() in sw_methods:
            pattern = re.sub(r'\{[^}]+\}', r'[^/]+', sw_path)
            if re.fullmatch(pattern, clean_path) or sw_path == clean_path:
                return sw_path, sw_methods[method.lower()]
            if sw_path in name or (clean_path.startswith(sw_path.split('{')[0]) and '{' in sw_path):
                return sw_path, sw_methods[method.lower()]

    return clean_path, None

groups = {}

for idx, tr in enumerate(traces):
    tags = tr.get('Tags', {})
    method = tags.get('http.request.method', tr.get('Name', '').split()[0]).upper()
    url_path = tags.get('url.path', '')
    name = tr.get('Name', '')

    sw_path, sw_details = find_swagger_match(method, url_path, name)
    
    clean_key = sw_path.strip('/').replace('/', '_').replace('{', '').replace('}', '')
    if not clean_key:
        clean_key = 'root'
    group_key = f"{method}_{clean_key}"

    if group_key not in groups:
        groups[group_key] = {
            'http_method': method,
            'swagger_path': sw_path if sw_details else '(Endpoint base)',
            'operation_id': sw_details.get('operationId', 'N/A') if sw_details else 'N/A',
            'summary': sw_details.get('summary', 'Endpoint base / Health probe') if sw_details else 'Endpoint base / Health probe',
            'swagger_metadata': sw_details if sw_details else {},
            'total_traces': 0,
            'traces': []
        }

    parsed_trace = dict(tr)
    if 'http.request.body_preview' in tags:
        try:
            parsed_trace['ParsedRequestBody'] = json.loads(tags['http.request.body_preview'])
        except Exception:
            parsed_trace['ParsedRequestBody'] = tags['http.request.body_preview']
            
    if 'http.response.body_preview' in tags:
        try:
            parsed_trace['ParsedResponseBody'] = json.loads(tags['http.response.body_preview'])
        except Exception:
            parsed_trace['ParsedResponseBody'] = tags['http.response.body_preview']

    groups[group_key]['total_traces'] += 1
    groups[group_key]['traces'].append(parsed_trace)

resumen = []
for key, data in groups.items():
    file_path = f"{out_dir}/{key}.json"
    with open(file_path, 'w', encoding='utf-8') as out_f:
        json.dump(data, out_f, indent=2, ensure_ascii=False)
    print(f"[OK] Generado archivo: {file_path} ({data['total_traces']} trazas | Metodo: {data['http_method']} | OperationId: {data['operation_id']})")
    resumen.append({
        'archivo': f"{key}.json",
        'metodo': data['http_method'],
        'swagger_path': data['swagger_path'],
        'operationId': data['operation_id'],
        'trazas_encontradas': data['total_traces']
    })

# Guardar resumen en markdown
with open(f"{out_dir}/README.md", 'w', encoding='utf-8') as f:
    f.write("# Resumen de Trazas Agrupadas por Metodo Swagger (OpenAPI)\n\n")
    f.write(f"Contrato Swagger evaluado: `transfer-mspx-prometeus.management.standard.yaml`\n\n")
    f.write("| Archivo Generado | Metodo HTTP | Swagger Path | OperationId | Total Trazas |\n")
    f.write("|---|---|---|---|---|\n")
    for r in resumen:
        f.write(f"| [`{r['archivo']}`](./{r['archivo']}) | **{r['metodo']}** | `{r['swagger_path']}` | `{r['operationId']}` | {r['trazas_encontradas']} |\n")

print(f"[OK] Resumen generado en {out_dir}/README.md")

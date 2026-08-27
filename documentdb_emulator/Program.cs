using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(options =>
{
    // Escuchar en HTTP 8081 para acceso directo y compatibilidad
    options.ListenAnyIP(8081);
});

var app = builder.Build();

// Almacén en memoria de Cosmos DB / DocumentDB
var documentStore = new ConcurrentDictionary<string, StoredDocument>();
var ruCounter = 0.0;
var lockObj = new object();

// Middleware para headers estándar de Azure Cosmos DB
app.Use(async (context, next) =>
{
    context.Response.Headers.Append("x-ms-activity-id", Guid.NewGuid().ToString());
    context.Response.Headers.Append("x-ms-request-charge", "1.0");
    context.Response.Headers.Append("x-ms-serviceversion", "version=2018-12-31");
    await next();
});

// 1. Endpoint REST Oficial de Azure Cosmos DB para crear/upsert documentos
app.MapPost("/dbs/{databaseName}/colls/{containerName}/docs", async (
    string databaseName,
    string containerName,
    HttpContext httpContext) =>
{
    using var reader = new StreamReader(httpContext.Request.Body);
    var body = await reader.ReadToEndAsync();

    if (string.IsNullOrWhiteSpace(body))
    {
        return Results.BadRequest(new { message = "Payload vacío" });
    }

    try
    {
        using var jsonDoc = JsonDocument.Parse(body);
        var root = jsonDoc.RootElement;

        var id = (root.TryGetProperty("id", out var idProp) && !string.IsNullOrEmpty(idProp.GetString()))
            ? idProp.GetString()!
            : ((root.TryGetProperty("TraceId", out var tProp) && !string.IsNullOrEmpty(tProp.GetString()))
                ? tProp.GetString()!
                : ((root.TryGetProperty("SpanId", out var sProp) && !string.IsNullOrEmpty(sProp.GetString()))
                    ? sProp.GetString()!
                    : Guid.NewGuid().ToString("N")));

        var partitionKey = (root.TryGetProperty("partitionKey", out var pkProp) && !string.IsNullOrEmpty(pkProp.GetString()))
            ? pkProp.GetString()!
            : ((root.TryGetProperty("TraceId", out var tp) && !string.IsNullOrEmpty(tp.GetString()))
                ? tp.GetString()!
                : "default");

        var stored = new StoredDocument(
            Id: id,
            DatabaseName: databaseName,
            ContainerName: containerName,
            PartitionKey: partitionKey,
            RawJson: body,
            StoredAt: DateTime.UtcNow);

        documentStore[id] = stored;

        lock (lockObj)
        {
            ruCounter += 1.0;
        }

        httpContext.Response.StatusCode = StatusCodes.Status201Created;
        httpContext.Response.Headers["x-ms-request-charge"] = "1.0";
        httpContext.Response.Headers["x-ms-documentdb-partitionkey"] = $"[\"{partitionKey}\"]";

        return Results.Content(body, "application/json");
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { error = ex.Message });
    }
});

// 2. Query de documentos según protocolo NoSQL Cosmos DB
app.MapGet("/dbs/{databaseName}/colls/{containerName}/docs", (string databaseName, string containerName) =>
{
    var docs = documentStore.Values
        .Where(d => d.DatabaseName.Equals(databaseName, StringComparison.OrdinalIgnoreCase) &&
                    d.ContainerName.Equals(containerName, StringComparison.OrdinalIgnoreCase))
        .OrderByDescending(d => d.StoredAt)
        .Take(100)
        .Select(d => JsonSerializer.Deserialize<object>(d.RawJson))
        .ToList();

    return Results.Ok(new
    {
        _rid = "mockRidCollection",
        Documents = docs,
        _count = docs.Count
    });
});

// 3. API de Estadísticas para la consola Web Explorer
app.MapGet("/api/stats", () =>
{
    var totalDocs = documentStore.Count;
    var partitionsCount = documentStore.Values.Select(d => d.PartitionKey).Distinct().Count();
    var latestDocs = documentStore.Values.OrderByDescending(d => d.StoredAt).Take(15).ToList();

    return Results.Ok(new
    {
        totalDocuments = totalDocs,
        totalRequestUnits = ruCounter,
        distinctPartitionKeys = partitionsCount,
        database = "ProdubancoObservability",
        container = "audit_logs",
        documents = latestDocs
    });
});

// 4. Limpiar documentos
app.MapDelete("/api/documents", () =>
{
    documentStore.Clear();
    lock (lockObj) ruCounter = 0;
    return Results.Ok(new { message = "Todos los documentos han sido eliminados." });
});

// 5. Interfaz Gráfica Cosmos DB Data Explorer UI (HTML5 / Vanilla CSS)
app.MapGet("/", () => Results.Content(GetExplorerHtml(), "text/html"));
app.MapGet("/_explorer/index.html", () => Results.Content(GetExplorerHtml(), "text/html"));
app.MapGet("/index.html", () => Results.Content(GetExplorerHtml(), "text/html"));

app.Run();

// ====================================================================
// VISTA WEB DATA EXPLORER (DISEÑO MODERNO OBSIDIAN DARK PRODUBANCO)
// ====================================================================
static string GetExplorerHtml()
{
    return """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Azure Cosmos DB / DocumentDB Explorer - Produbanco</title>
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Fira+Code:wght@400;500;600&family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
        <style>
            :root {
                --bg-primary: #0a0e17;
                --bg-card: rgba(16, 24, 40, 0.85);
                --bg-card-hover: rgba(22, 33, 56, 0.95);
                --border-color: rgba(30, 41, 59, 0.7);
                --border-glow: rgba(0, 163, 224, 0.4);
                --text-primary: #f8fafc;
                --text-secondary: #94a3b8;
                --text-muted: #64748b;
                --accent-blue: #0078d4;
                --accent-cyan: #00d2ff;
                --accent-green: #10b981;
                --accent-amber: #f59e0b;
                --accent-purple: #8b5cf6;
                --accent-red: #ef4444;
            }

            * {
                box-sizing: border-box;
                margin: 0;
                padding: 0;
            }

            body {
                font-family: 'Inter', sans-serif;
                background-color: var(--bg-primary);
                background-image: 
                    radial-gradient(at 0% 0%, rgba(0, 120, 212, 0.15) 0px, transparent 50%),
                    radial-gradient(at 100% 100%, rgba(0, 210, 255, 0.1) 0px, transparent 50%);
                color: var(--text-primary);
                min-height: 100vh;
                padding: 24px;
            }

            .container {
                max-width: 1400px;
                margin: 0 auto;
            }

            header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 24px;
                padding-bottom: 20px;
                border-bottom: 1px solid var(--border-color);
            }

            .logo-area {
                display: flex;
                align-items: center;
                gap: 16px;
            }

            .cosmos-icon {
                width: 48px;
                height: 48px;
                background: linear-gradient(135deg, #0078d4, #00d2ff);
                border-radius: 12px;
                display: flex;
                align-items: center;
                justify-content: center;
                box-shadow: 0 0 20px rgba(0, 120, 212, 0.5);
                font-size: 24px;
            }

            .title-area h1 {
                font-size: 1.5rem;
                font-weight: 700;
                background: linear-gradient(to right, #fff, #94a3b8);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }

            .title-area p {
                font-size: 0.85rem;
                color: var(--text-secondary);
            }

            .actions {
                display: flex;
                gap: 12px;
            }

            .btn {
                background: var(--bg-card);
                border: 1px solid var(--border-color);
                color: var(--text-primary);
                padding: 8px 16px;
                border-radius: 8px;
                font-size: 0.85rem;
                font-weight: 600;
                cursor: pointer;
                display: flex;
                align-items: center;
                gap: 8px;
                transition: all 0.2s;
            }

            .btn:hover {
                background: var(--bg-card-hover);
                border-color: var(--accent-cyan);
            }

            .btn-danger:hover {
                border-color: var(--accent-red);
                color: var(--accent-red);
            }

            .stats-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
                gap: 16px;
                margin-bottom: 24px;
            }

            .stat-card {
                background: var(--bg-card);
                border: 1px solid var(--border-color);
                border-radius: 12px;
                padding: 16px 20px;
                backdrop-filter: blur(12px);
                position: relative;
                overflow: hidden;
            }

            .stat-card::after {
                content: '';
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                height: 3px;
                background: linear-gradient(90deg, var(--accent-blue), var(--accent-cyan));
            }

            .stat-card .label {
                font-size: 0.75rem;
                text-transform: uppercase;
                letter-spacing: 0.05em;
                color: var(--text-secondary);
                margin-bottom: 8px;
            }

            .stat-card .value {
                font-size: 1.8rem;
                font-weight: 800;
                color: #fff;
                font-family: 'Fira Code', monospace;
            }

            .main-content {
                display: grid;
                grid-template-columns: 350px 1fr;
                gap: 20px;
            }

            .tree-panel, .viewer-panel {
                background: var(--bg-card);
                border: 1px solid var(--border-color);
                border-radius: 12px;
                padding: 20px;
                min-height: 550px;
                backdrop-filter: blur(12px);
            }

            .panel-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 16px;
                font-size: 0.95rem;
                font-weight: 600;
                border-bottom: 1px solid var(--border-color);
                padding-bottom: 10px;
            }

            .doc-list {
                list-style: none;
                max-height: 480px;
                overflow-y: auto;
            }

            .doc-item {
                padding: 10px 12px;
                margin-bottom: 8px;
                background: rgba(255, 255, 255, 0.02);
                border: 1px solid var(--border-color);
                border-radius: 8px;
                cursor: pointer;
                transition: all 0.2s;
                font-family: 'Fira Code', monospace;
                font-size: 0.8rem;
            }

            .doc-item:hover, .doc-item.active {
                background: rgba(0, 120, 212, 0.15);
                border-color: var(--accent-cyan);
            }

            .doc-item .pk {
                font-size: 0.7rem;
                color: var(--accent-cyan);
                display: block;
                margin-top: 4px;
            }

            pre.json-viewer {
                background: #050811;
                border: 1px solid var(--border-color);
                border-radius: 8px;
                padding: 16px;
                font-family: 'Fira Code', monospace;
                font-size: 0.85rem;
                color: #38bdf8;
                overflow: auto;
                max-height: 480px;
                line-height: 1.5;
            }

            .badge {
                display: inline-block;
                padding: 2px 8px;
                font-size: 0.7rem;
                border-radius: 6px;
                font-weight: 600;
            }
            .badge-live { background: rgba(16, 185, 129, 0.2); color: var(--accent-green); }
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <div class="logo-area">
                    <div class="cosmos-icon">🪐</div>
                    <div class="title-area">
                        <h1>Azure Cosmos DB / DocumentDB Data Explorer</h1>
                        <p>Produbanco Observability | NoSQL Core Engine Emulator</p>
                    </div>
                </div>
                <div class="actions">
                    <button class="btn" onclick="fetchData()">🔄 Refrescar</button>
                    <button class="btn btn-danger" onclick="clearData()">🗑️ Limpiar DB</button>
                </div>
            </header>

            <div class="stats-grid">
                <div class="stat-card">
                    <div class="label">Documentos Persistidos (audit_logs)</div>
                    <div class="value" id="stat-docs">0</div>
                </div>
                <div class="stat-card">
                    <div class="label">Request Units (RUs) Consumidas</div>
                    <div class="value" id="stat-rus">0.0</div>
                </div>
                <div class="stat-card">
                    <div class="label">Particiones Lógicas Activas</div>
                    <div class="value" id="stat-pk">0</div>
                </div>
                <div class="stat-card">
                    <div class="label">Estado del Bulk Sink</div>
                    <div class="value" style="font-size: 1.1rem; color: var(--accent-green); display: flex; align-items: center; gap: 8px; margin-top: 8px;">
                        <span class="badge badge-live">● EN VIVO</span> Micro-Lotes 500
                    </div>
                </div>
            </div>

            <div class="main-content">
                <div class="tree-panel">
                    <div class="panel-header">
                        <span>📁 ProdubancoObservability / audit_logs</span>
                        <span id="doc-counter" style="font-size: 0.75rem; color: var(--text-muted);">0 docs</span>
                    </div>
                    <ul class="doc-list" id="doc-list">
                        <li style="color: var(--text-muted); font-size: 0.8rem; text-align: center; padding: 20px;">Esperando ingesta de Kafka Bulk Sink...</li>
                    </ul>
                </div>
                <div class="viewer-panel">
                    <div class="panel-header">
                        <span id="viewer-title">JSON Document Viewer</span>
                        <span id="viewer-timestamp" style="font-size: 0.75rem; color: var(--accent-amber);"></span>
                    </div>
                    <pre class="json-viewer" id="json-viewer">// Selecciona un documento de la lista izquierda para inspeccionar su JSON completo.</pre>
                </div>
            </div>
        </div>

        <script>
            let currentDocs = [];

            async function fetchData() {
                try {
                    const res = await fetch('/api/stats');
                    const data = await res.json();

                    document.getElementById('stat-docs').innerText = data.totalDocuments.toLocaleString();
                    document.getElementById('stat-rus').innerText = data.totalRequestUnits.toFixed(1);
                    document.getElementById('stat-pk').innerText = data.distinctPartitionKeys;
                    document.getElementById('doc-counter').innerText = `${data.totalDocuments} docs`;

                    currentDocs = data.documents || [];
                    renderDocList();
                } catch (e) {
                    console.error("Error fetching Cosmos DB stats:", e);
                }
            }

            function renderDocList() {
                const list = document.getElementById('doc-list');
                if (currentDocs.length === 0) {
                    list.innerHTML = '<li style="color: var(--text-muted); font-size: 0.8rem; text-align: center; padding: 20px;">Sin documentos. Envía transacciones desde el panel web de Kafka.</li>';
                    return;
                }

                list.innerHTML = '';
                currentDocs.forEach((doc, idx) => {
                    const li = document.createElement('li');
                    li.className = 'doc-item' + (idx === 0 ? ' active' : '');
                    li.innerHTML = `<div>${doc.id}</div><span class="pk">PK: ${doc.partitionKey}</span>`;
                    li.onclick = () => selectDoc(idx, li);
                    list.appendChild(li);
                });

                if (currentDocs.length > 0) {
                    selectDoc(0, list.children[0]);
                }
            }

            function selectDoc(idx, element) {
                document.querySelectorAll('.doc-item').forEach(el => el.classList.remove('active'));
                if (element) element.classList.add('active');

                const doc = currentDocs[idx];
                if (doc) {
                    document.getElementById('viewer-title').innerText = `Documento: ${doc.id}`;
                    document.getElementById('viewer-timestamp').innerText = `Almacenado: ${new Date(doc.storedAt).toLocaleTimeString()}`;
                    try {
                        const parsed = JSON.parse(doc.rawJson);
                        document.getElementById('json-viewer').innerText = JSON.stringify(parsed, null, 2);
                    } catch {
                        document.getElementById('json-viewer').innerText = doc.rawJson;
                    }
                }
            }

            async function clearData() {
                if (confirm('¿Deseas limpiar todos los documentos en Cosmos DB?')) {
                    await fetch('/api/documents', { method: 'DELETE' });
                    fetchData();
                }
            }

            setInterval(fetchData, 2000);
            fetchData();
        </script>
    </body>
    </html>
    """;
}

public record StoredDocument(
    string Id,
    string DatabaseName,
    string ContainerName,
    string PartitionKey,
    string RawJson,
    DateTime StoredAt);

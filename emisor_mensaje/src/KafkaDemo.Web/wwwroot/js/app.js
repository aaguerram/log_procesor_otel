// Estado de la Aplicación
const state = {
  topics: [],
  clusterHealth: null,
  totalSentSession: 0,
  includeInternal: false
};

// Elementos DOM
const dom = {
  statusBadge: document.getElementById('cluster-status-badge'),
  statusText: document.getElementById('cluster-status-text'),
  btnRefresh: document.getElementById('btn-refresh'),
  statTopics: document.getElementById('stat-topics-count'),
  statPartitions: document.getElementById('stat-partitions-count'),
  statClusterHealth: document.getElementById('stat-cluster-health'),
  statSentCount: document.getElementById('stat-sent-count'),
  topicsTableBody: document.getElementById('topics-table-body'),
  chkIncludeInternal: document.getElementById('chk-include-internal'),
  btnQuick20: document.getElementById('btn-quick-20'),
  btnOpenCreateTopic: document.getElementById('btn-open-create-topic'),
  modalCreateTopic: document.getElementById('modal-create-topic'),
  formCreateTopic: document.getElementById('form-create-topic'),
  modalTopicDetails: document.getElementById('modal-topic-details'),
  topicPartitionsList: document.getElementById('topic-partitions-list'),
  detailsTopicTitle: document.getElementById('details-topic-title'),
  formSendMessage: document.getElementById('form-send-message'),
  msgTopicSelect: document.getElementById('msg-topic-select'),
  msgKeyInput: document.getElementById('msg-key-input'),
  btnRegenKey: document.getElementById('btn-regen-key'),
  tracePresetSelect: document.getElementById('trace-preset-select'),
  telemetryTypeSelect: document.getElementById('telemetry-type-select'),
  badgeTelemetryStatus: document.getElementById('badge-telemetry-status'),
  badgePresetCount: document.getElementById('badge-preset-count'),
  msgValueInput: document.getElementById('msg-value-input'),
  btnSampleJson: document.getElementById('btn-sample-json'),
  btnFormatJson: document.getElementById('btn-format-json'),
  messagesStream: document.getElementById('messages-stream'),
  btnClearLogs: document.getElementById('btn-clear-logs'),
  toastContainer: document.getElementById('toast-container')
};

// Generador de clave de particionamiento con dispersión de ultra-alta entropía (SplitMix64 Avalanche)
let _dispersedCounter = 0n;

function generateDispersedKey(businessId = '8172201-IN') {
  _dispersedCounter = (_dispersedCounter + 1n) & 0xFFFFFFFFFFFFFFFFn;
  const nowTicks = BigInt(Date.now());
  const randomSalt = BigInt(Math.floor(Math.random() * 0xFFFFFF));
  let seed = ((nowTicks ^ randomSalt) ^ (_dispersedCounter * 0x9e3779b97f4a7c15n)) & 0xFFFFFFFFFFFFFFFFn;

  if (businessId && businessId.trim() !== '') {
    let fnv = 0xcbf29ce484222325n;
    const cleanId = businessId.trim();
    for (let i = 0; i < cleanId.length; i++) {
      fnv = (fnv ^ BigInt(cleanId.charCodeAt(i))) & 0xFFFFFFFFFFFFFFFFn;
      fnv = (fnv * 0x100000001b3n) & 0xFFFFFFFFFFFFFFFFn;
    }
    seed = (seed ^ fnv) & 0xFFFFFFFFFFFFFFFFn;
  }

  // Mezclador SplitMix64 / Murmur3 Avalanche
  seed = (seed ^ (seed >> 30n)) & 0xFFFFFFFFFFFFFFFFn;
  seed = (seed * 0xbf58476d1ce4e5b9n) & 0xFFFFFFFFFFFFFFFFn;
  seed = (seed ^ (seed >> 27n)) & 0xFFFFFFFFFFFFFFFFn;
  seed = (seed * 0x94d049bb133111ebn) & 0xFFFFFFFFFFFFFFFFn;
  seed = (seed ^ (seed >> 31n)) & 0xFFFFFFFFFFFFFFFFn;

  const hex16 = seed.toString(16).toUpperCase().padStart(16, '0');
  return businessId && businessId.trim() !== '' ? `PK-${hex16}-${businessId.trim()}` : `PK-${hex16}`;
}

// Inicialización
document.addEventListener('DOMContentLoaded', () => {
  setupTabs();
  setupModals();
  setupEvents();
  
  // Carga Inicial
  refreshAll();
  initTelemetryCatalog();
  
  // Sondeo de estado cada 10s
  setInterval(checkHealth, 10000);
});

// Configuración de Tabs
function setupTabs() {
  const tabButtons = document.querySelectorAll('.tab-btn');
  const tabContents = document.querySelectorAll('.tab-content');

  tabButtons.forEach(btn => {
    btn.addEventListener('click', () => {
      tabButtons.forEach(b => b.classList.remove('active'));
      tabContents.forEach(c => c.classList.remove('active'));

      btn.classList.add('active');
      const targetId = btn.getAttribute('data-tab');
      document.getElementById(targetId)?.classList.add('active');
    });
  });
}

// Configuración de Modales
function setupModals() {
  document.querySelectorAll('[data-close-modal]').forEach(el => {
    el.addEventListener('click', () => {
      document.querySelectorAll('.modal').forEach(m => m.classList.remove('active'));
    });
  });

  dom.btnOpenCreateTopic.addEventListener('click', () => {
    dom.modalCreateTopic.classList.add('active');
    document.getElementById('new-topic-name').focus();
  });
}

// Event Listeners
function setupEvents() {
  dom.btnRefresh.addEventListener('click', refreshAll);
  
  dom.chkIncludeInternal.addEventListener('change', (e) => {
    state.includeInternal = e.target.checked;
    fetchTopics();
  });

  dom.btnQuick20.addEventListener('click', send20DemoTransactions);

  dom.formCreateTopic.addEventListener('submit', handleCreateTopic);
  dom.formSendMessage.addEventListener('submit', handleSendMessage);
  
  // Botón para regenerar ID de partición con SplitMix64
  dom.btnRegenKey?.addEventListener('click', () => {
    const selectedKey = dom.tracePresetSelect?.value;
    const businessKey = getBusinessKeyForCurrentPreset(selectedKey);
    dom.msgKeyInput.value = generateDispersedKey(businessKey);
    showToast('Nuevo ID de partición generado (SplitMix64)', 'info');
  });

  // Selector de Tipo de Señal Telemetría (Trace, Metric, Log)
  dom.telemetryTypeSelect?.addEventListener('change', (e) => {
    const signalType = e.target.value;
    updateTelemetryBadge(signalType);
    populatePresetDropdown(signalType);
  });

  // Selector Dinámico de Ejemplos
  dom.tracePresetSelect?.addEventListener('change', (e) => {
    loadSelectedPreset(e.target.value);
  });

  dom.btnFormatJson?.addEventListener('click', formatTextareaJson);

  dom.btnClearLogs.addEventListener('click', () => {
    dom.messagesStream.innerHTML = `
      <div class="console-empty">
        <p>Historial limpiado. Los nuevos eventos aparecerán aquí.</p>
      </div>`;
  });
}

function updateTelemetryBadge(type) {
  if (!dom.badgeTelemetryStatus) return;
  if (type === 'Trace') {
    dom.badgeTelemetryStatus.className = 'badge badge-system';
    dom.badgeTelemetryStatus.textContent = '🔍 Trace (Aplica Swagger)';
  } else if (type === 'Metric') {
    dom.badgeTelemetryStatus.className = 'badge badge-user';
    dom.badgeTelemetryStatus.textContent = '📊 Metric (Directo Cosmos DB)';
  } else {
    dom.badgeTelemetryStatus.className = 'badge badge-internal';
    dom.badgeTelemetryStatus.textContent = '📝 Log (Directo Cosmos DB)';
  }
}

// Recarga General
async function refreshAll() {
  await Promise.all([checkHealth(), fetchTopics()]);
}

// Comprobar Salud del Clúster
async function checkHealth() {
  try {
    const res = await fetch('/api/health');
    const data = await res.json();
    state.clusterHealth = data;

    if (data.isConnected) {
      dom.statusBadge.className = 'status-dot';
      dom.statusText.textContent = 'Broker Conectado (3 Part. / 1 Node)';
      dom.statClusterHealth.textContent = 'Saludable';
      dom.statClusterHealth.style.color = '#10b981';
    } else {
      dom.statusBadge.className = 'status-dot status-offline';
      dom.statusText.textContent = 'Desconectado de Kafka';
      dom.statClusterHealth.textContent = 'Fallo Conexión';
      dom.statClusterHealth.style.color = '#ef4444';
    }
  } catch (err) {
    dom.statusBadge.className = 'status-dot status-offline';
    dom.statusText.textContent = 'Error de API';
    dom.statClusterHealth.textContent = 'Error';
    dom.statClusterHealth.style.color = '#ef4444';
  }
}

// Obtener Lista de Tópicos
async function fetchTopics() {
  try {
    const res = await fetch(`/api/topics?includeInternal=${state.includeInternal}`);
    const topics = await res.json();
    state.topics = topics;

    renderTopicsTable(topics);
    updateTopicsSelect(topics);
    updateStats(topics);
  } catch (err) {
    showToast(`Error al obtener tópicos: ${err.message}`, 'error');
  }
}

// Renderizar Tabla de Tópicos
function renderTopicsTable(topics) {
  if (!topics || topics.length === 0) {
    dom.topicsTableBody.innerHTML = `
      <tr>
        <td colspan="4" style="text-align: center; color: var(--text-muted); padding: 32px;">
          No se encontraron tópicos. Crea uno nuevo con el botón superior.
        </td>
      </tr>
    `;
    return;
  }

  dom.topicsTableBody.innerHTML = topics.map(t => `
    <tr>
      <td>
        <strong style="color: var(--primary); font-family: monospace; font-size: 0.95rem;">
          ${escapeHtml(t.name)}
        </strong>
      </td>
      <td>
        <span class="badge ${t.partitionsCount >= 3 ? 'badge-user' : 'badge-internal'}">
          ${t.partitionsCount} Particiones
        </span>
      </td>
      <td>
        <span class="badge ${t.isInternal ? 'badge-internal' : 'badge-system'}">
          ${t.isInternal ? 'Interno' : 'Usuario'}
        </span>
      </td>
      <td style="text-align: right;">
        <button class="btn btn-secondary btn-sm" onclick="showTopicDetails('${escapeHtml(t.name)}')">
          Detalles
        </button>
        ${!t.isInternal ? `
          <button class="btn btn-danger btn-sm" onclick="handleDeleteTopic('${escapeHtml(t.name)}')">
            Eliminar
          </button>
        ` : ''}
      </td>
    </tr>
  `).join('');
}

// Actualizar Select del formulario (Solo tópico del emisor)
function updateTopicsSelect(topics) {
  let emitterTopics = topics.filter(t => !t.isInternal && t.name.includes('.emitted.'));
  if (emitterTopics.length === 0) {
    emitterTopics = topics.filter(t => t.name === 'tp.observability.application-log.emitted.v1');
  }
  if (emitterTopics.length === 0) {
    emitterTopics = [{ name: 'tp.observability.application-log.emitted.v1', partitionsCount: 40 }];
  }

  dom.msgTopicSelect.innerHTML = emitterTopics.map(t => 
    `<option value="${escapeHtml(t.name)}">${escapeHtml(t.name)} (${t.partitionsCount} part.)</option>`
  ).join('');

  dom.msgTopicSelect.value = emitterTopics[0].name;
}

// Actualizar Tarjetas de Estadísticas
function updateStats(topics) {
  const userTopics = topics.filter(t => !t.isInternal);
  const totalPartitions = userTopics.reduce((acc, t) => acc + t.partitionsCount, 0);

  dom.statTopics.textContent = userTopics.length;
  dom.statPartitions.textContent = totalPartitions;
}

// Crear Tópico
async function handleCreateTopic(e) {
  e.preventDefault();
  const name = document.getElementById('new-topic-name').value.trim();
  const partitions = parseInt(document.getElementById('new-topic-partitions').value, 10);
  const replication = parseInt(document.getElementById('new-topic-replication').value, 10);

  if (!name) {
    showToast('El nombre del tópico es obligatorio', 'error');
    return;
  }

  const btn = dom.formCreateTopic.querySelector('button[type="submit"]');
  btn.disabled = true;
  btn.textContent = 'Creando...';

  try {
    const res = await fetch('/api/topics', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        topicName: name,
        partitions: partitions,
        replicationFactor: replication
      })
    });

    const data = await res.json();
    if (!res.ok) throw new Error(data.message || 'Error al crear tópico');

    showToast(`Tópico '${name}' creado exitosamente`, 'success');
    dom.modalCreateTopic.classList.remove('active');
    dom.formCreateTopic.reset();
    await fetchTopics();
  } catch (err) {
    showToast(`Error: ${err.message}`, 'error');
  } finally {
    btn.disabled = false;
    btn.textContent = 'Crear Tópico';
  }
}

// Eliminar Tópico
window.handleDeleteTopic = async function(topicName) {
  if (!confirm(`¿Estás seguro de eliminar el tópico '${topicName}' de Kafka?\nEsta acción no se puede deshacer.`)) {
    return;
  }

  try {
    const res = await fetch(`/api/topics/${encodeURIComponent(topicName)}`, {
      method: 'DELETE'
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.message || 'Error al eliminar');

    showToast(`Tópico '${topicName}' eliminado`, 'info');
    await fetchTopics();
  } catch (err) {
    showToast(`Error: ${err.message}`, 'error');
  }
};

// Ver Detalles del Tópico
window.showTopicDetails = function(topicName) {
  const topic = state.topics.find(t => t.name === topicName);
  if (!topic) return;

  dom.detailsTopicTitle.textContent = `Tópico: ${topic.name}`;
  dom.topicPartitionsList.innerHTML = topic.partitions.map(p => `
    <div class="partition-card">
      <div class="partition-id">Partición #${p.partitionId}</div>
      <div class="partition-info">Líder: Broker ${p.leader}</div>
      <div class="partition-info">Réplicas: [${p.replicas.join(', ')}]</div>
      <div class="partition-info">ISR: [${p.inSyncReplicas.join(', ')}]</div>
    </div>
  `).join('');

  dom.modalTopicDetails.classList.add('active');
};

// Enviar 20 Transacciones Demo
async function send20DemoTransactions() {
  dom.btnQuick20.disabled = true;
  dom.btnQuick20.innerHTML = '⏳ Enviando 20 transacciones...';

  try {
    const res = await fetch('/api/messages/send-batch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        topic: 'tp.observability.application-log.emitted.v1',
        count: 20
      })
    });

    const data = await res.json();
    if (!res.ok) throw new Error(data.message || 'Fallo en envío de lote');

    const result = data.result;
    state.totalSentSession += result.totalSent;
    dom.statSentCount.textContent = state.totalSentSession;

    showToast(`✔ Lote de 20 mensajes enviado a '${result.targetTopic}' (${result.elapsedMilliseconds.toFixed(1)} ms)`, 'success');
    appendBatchToStream(result);
    await fetchTopics();
  } catch (err) {
    showToast(`Error al enviar lote: ${err.message}`, 'error');
  } finally {
    dom.btnQuick20.disabled = false;
    dom.btnQuick20.innerHTML = `
      <svg viewBox="0 0 24 24" width="16" height="16" stroke="currentColor" stroke-width="2" fill="none">
        <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>
      </svg>
      ⚡ Enviar Lote de 20 Transacciones
    `;
  }
}

// Enviar Mensaje Individual desde el Portal Web
async function handleSendMessage(e) {
  e.preventDefault();
  const topic = dom.msgTopicSelect.value;
  const key = dom.msgKeyInput.value.trim() || null;
  const value = dom.msgValueInput.value.trim();

  if (!topic) {
    showToast('Selecciona un tópico de destino', 'error');
    return;
  }

  // Validar si es JSON válido
  try {
    JSON.parse(value);
  } catch (parseErr) {
    showToast('El contenido no es un JSON válido. Revisa la sintaxis.', 'error');
    return;
  }

  const btn = document.getElementById('btn-submit-message');
  btn.disabled = true;
  btn.textContent = 'Publicando evento...';

  const telemetryType = dom.telemetryTypeSelect?.value || 'Trace';

  try {
    const res = await fetch('/api/messages/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topic, key, value, telemetryType })
    });

    const data = await res.json();
    if (!res.ok) throw new Error(data.message || 'Error al enviar');

    state.totalSentSession += 1;
    dom.statSentCount.textContent = state.totalSentSession;

    showToast(`✔ Evento [${telemetryType}] publicado en '${topic}' [P:${data.result.partition}, Offset:#${data.result.offset}]`, 'success');
    appendSingleToStream(data.result, value);

    // Regenerar automáticamente un nuevo ID de partición con SplitMix64 para el siguiente envío
    const selectedPresetKey = dom.tracePresetSelect?.value;
    const businessKey = getBusinessKeyForCurrentPreset(selectedPresetKey);
    dom.msgKeyInput.value = generateDispersedKey(businessKey);
  } catch (err) {
    showToast(`Error al publicar: ${err.message}`, 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = `
      <svg viewBox="0 0 24 24" width="18" height="18" stroke="currentColor" stroke-width="2" fill="none">
        <line x1="22" y1="2" x2="11" y2="13"/>
        <polygon points="22 2 15 22 11 13 2 9 22 2"/>
      </svg>
      Publicar Evento a Kafka
    `;
  }
}

// Renderizar eventos en consola de flujo
function appendBatchToStream(batch) {
  removeEmptyStreamNotice();

  const batchHeader = document.createElement('div');
  batchHeader.className = 'log-item log-batch';
  batchHeader.innerHTML = `
    <div class="log-header">
      <span class="log-meta">⚡ LOTE MASIVO: <strong>${batch.totalSent}</strong> eventos enviados</span>
      <span>${new Date().toLocaleTimeString()}</span>
    </div>
    <div class="log-preview">
      Destino: <strong>${batch.targetTopic}</strong> | Latencia: <strong>${batch.elapsedMilliseconds.toFixed(2)} ms</strong>
    </div>
  `;
  dom.messagesStream.prepend(batchHeader);

  batch.results.slice(0, 5).forEach(item => {
    const logItem = document.createElement('div');
    logItem.className = 'log-item log-success';
    logItem.innerHTML = `
      <div class="log-header">
        <span class="log-meta">Partición: <strong>${item.partition}</strong> | Offset: <strong>#${item.offset}</strong></span>
        <span>${new Date(item.timestamp).toLocaleTimeString()}</span>
      </div>
      <div class="log-preview">Key: <code>${item.key}</code></div>
    `;
    dom.messagesStream.prepend(logItem);
  });
}

function appendSingleToStream(item, rawPayload) {
  removeEmptyStreamNotice();

  let formatted = rawPayload;
  try {
    formatted = JSON.stringify(JSON.parse(rawPayload), null, 2);
  } catch (_) {}

  const logItem = document.createElement('div');
  logItem.className = 'log-item log-success';
  logItem.innerHTML = `
    <div class="log-header">
      <span class="log-meta">Tópico: <strong>${item.topic}</strong> | Partición: ${item.partition} | Offset: #${item.offset}</span>
      <span>${new Date(item.timestamp).toLocaleTimeString()}</span>
    </div>
    <div class="log-preview"><strong>Key:</strong> <code>${item.key || 'None'}</code>\n<pre style="margin-top: 6px; color: #cbd5e1;">${escapeHtml(formatted)}</pre></div>
  `;
  dom.messagesStream.prepend(logItem);
}

function removeEmptyStreamNotice() {
  const empty = dom.messagesStream.querySelector('.console-empty');
  if (empty) empty.remove();
}

// ===================== CATÁLOGOS DE SEÑALES OPENTELEMETRY =====================

// 1. Catálogo de Trazas OTel Disponibles (4 Ejemplos)
const OTelTraces = {
  'otel-get': {
    url: '/data/otel_get_trace.json',
    businessKey: '8172201-IN',
    label: 'GET - /contacts/contacts-by-idClient/8172201/IN (Lista 120 Contactos)'
  },
  'otel-post-1': {
    url: '/data/otel_post_trace_1.json',
    businessKey: '5103846-IN',
    label: 'POST - /contacts/local-contact (ID: 1394487 | Cédula: 1702756766)'
  },
  'otel-post-2': {
    url: '/data/otel_post_trace_2.json',
    businessKey: '5103846-IN',
    label: 'POST - /contacts/local-contact (ID: 1394495 | Cédula: 1702756766)'
  },
  'otel-post-3': {
    url: '/data/otel_post_trace_3.json',
    businessKey: '5103846-IN',
    label: 'POST - /contacts/local-contact (ID: 13944955 | Respuesta Exitosa 100000)'
  }
};

// 2. Catálogo de Métricas OTel (20 Métricas Únicas)
let cachedMetricsCatalog = null;

const OTelMetricDefinitions = [
  // Tipo: LongSum (9)
  { key: 'dotnet.gc.collections', type: 'LongSum', label: '[LongSum] dotnet.gc.collections (Colecciones GC Gen0, Gen1, Gen2)' },
  { key: 'dotnet.gc.heap.total_allocated', type: 'LongSum', label: '[LongSum] dotnet.gc.heap.total_allocated (Bytes totales en Heap)' },
  { key: 'dotnet.jit.compiled_il.size', type: 'LongSum', label: '[LongSum] dotnet.jit.compiled_il.size (Bytes de IL compilados)' },
  { key: 'dotnet.jit.compiled_methods', type: 'LongSum', label: '[LongSum] dotnet.jit.compiled_methods (Métodos compilados JIT)' },
  { key: 'dotnet.monitor.lock_contentions', type: 'LongSum', label: '[LongSum] dotnet.monitor.lock_contentions (Contenciones de bloqueo)' },
  { key: 'dotnet.thread_pool.thread.count', type: 'LongSum', label: '[LongSum] dotnet.thread_pool.thread.count (Hilos activos ThreadPool)' },
  { key: 'dotnet.thread_pool.work_item.count', type: 'LongSum', label: '[LongSum] dotnet.thread_pool.work_item.count (Work Items completados)' },
  { key: 'dotnet.thread_pool.queue.length', type: 'LongSum', label: '[LongSum] dotnet.thread_pool.queue.length (Cola pendiente ThreadPool)' },
  { key: 'dotnet.exceptions', type: 'LongSum', label: '[LongSum] dotnet.exceptions (Excepciones por error.type)' },
  // Tipo: LongSumNonMonotonic (7)
  { key: 'dotnet.process.memory.working_set', type: 'LongSumNonMonotonic', label: '[LongSumNonMonotonic] dotnet.process.memory.working_set (Working Set Memoria)' },
  { key: 'dotnet.gc.last_collection.heap.size', type: 'LongSumNonMonotonic', label: '[LongSumNonMonotonic] dotnet.gc.last_collection.heap.size (Tamaño Heap GC)' },
  { key: 'dotnet.gc.last_collection.heap.fragmentation.size', type: 'LongSumNonMonotonic', label: '[LongSumNonMonotonic] dotnet.gc.last_collection.heap.fragmentation.size (Fragmentación Heap)' },
  { key: 'dotnet.gc.last_collection.memory.committed_size', type: 'LongSumNonMonotonic', label: '[LongSumNonMonotonic] dotnet.gc.last_collection.memory.committed_size (Memoria física comprometida)' },
  { key: 'dotnet.timer.count', type: 'LongSumNonMonotonic', label: '[LongSumNonMonotonic] dotnet.timer.count (Instancias de Timers activos)' },
  { key: 'dotnet.assembly.count', type: 'LongSumNonMonotonic', label: '[LongSumNonMonotonic] dotnet.assembly.count (Ensamblados .NET cargados)' },
  { key: 'dotnet.process.cpu.count', type: 'LongSumNonMonotonic', label: '[LongSumNonMonotonic] dotnet.process.cpu.count (CPUs/Núcleos disponibles)' },
  // Tipo: DoubleSum (3)
  { key: 'dotnet.gc.pause.time', type: 'DoubleSum', label: '[DoubleSum] dotnet.gc.pause.time (Tiempo de pausas GC en seg.)' },
  { key: 'dotnet.jit.compilation.time', type: 'DoubleSum', label: '[DoubleSum] dotnet.jit.compilation.time (Tiempo compilación JIT en seg.)' },
  { key: 'dotnet.process.cpu.time', type: 'DoubleSum', label: '[DoubleSum] dotnet.process.cpu.time (Segundos de CPU User/System)' },
  // Tipo: Histogram (1)
  { key: 'dns.lookup.duration', type: 'Histogram', label: '[Histogram] dns.lookup.duration (Histograma latencia resolución DNS)' }
];

// 3. Catálogo de Logs de Ejemplo (3)
const OTelLogs = {
  'log-info': {
    businessKey: 'LOG-INFO-AUTH',
    label: '[INFO] AuthAudit - Inicio de sesión exitoso de usuario institucional',
    data: {
      timestamp: new Date().toISOString(),
      level: "Information",
      category: "Produbanco.Security.AuthService",
      message: "Usuario institucional autenticado satisfactoriamente desde canal Web.",
      eventId: 1001,
      properties: {
        userId: "USR-PROD-81722",
        channel: "WebBanking",
        ipAddress: "192.168.10.45",
        sessionId: "SESS-98B2-45E1"
      }
    }
  },
  'log-warn': {
    businessKey: 'LOG-WARN-OTP',
    label: '[WARN] SecurityThreat - Múltiples intentos fallidos de autenticación OTP',
    data: {
      timestamp: new Date().toISOString(),
      level: "Warning",
      category: "Produbanco.Security.ThreatDetection",
      message: "Se detectaron 3 intentos fallidos consecutivos de validación OTP para transferencia.",
      eventId: 2004,
      properties: {
        userId: "USR-PROD-51038",
        channel: "MobileApp",
        destinationAccount: "22005828479",
        failedAttempts: 3
      }
    }
  },
  'log-error': {
    businessKey: 'LOG-ERR-TIMEOUT',
    label: '[ERROR] DatabaseTimeout - Timeout al ejecutar consulta en base de datos secundaria',
    data: {
      timestamp: new Date().toISOString(),
      level: "Error",
      category: "Produbanco.Infrastructure.DatabasePool",
      message: "Tiempo de espera agotado (Timeout > 5000ms) en réplica de lectura de cuentas.",
      eventId: 5003,
      exception: {
        type: "System.TimeoutException",
        message: "The operation has timed out while waiting for connection pool lease.",
        stackTrace: "at Produbanco.Infrastructure.DbPool.AcquireConnectionAsync(CancellationToken ct)"
      }
    }
  }
};

// Inicializar Catálogo de Telemetría
async function initTelemetryCatalog() {
  try {
    const res = await fetch('/data/otel_metrics_catalog.json');
    if (res.ok) {
      cachedMetricsCatalog = await res.json();
    }
  } catch (err) {
    console.warn('No se pudo precargar otel_metrics_catalog.json:', err);
  }

  populatePresetDropdown('Trace');
}

// Poblar Dropdown Dinámico según el Tipo de Señal
function populatePresetDropdown(signalType) {
  if (!dom.tracePresetSelect) return;

  if (signalType === 'Trace') {
    dom.tracePresetSelect.innerHTML = Object.entries(OTelTraces).map(([k, v]) => 
      `<option value="${k}">${escapeHtml(v.label)}</option>`
    ).join('');

    if (dom.badgePresetCount) {
      dom.badgePresetCount.className = 'badge badge-system';
      dom.badgePresetCount.textContent = '4 Trazas Registradas';
    }
    loadSelectedPreset('otel-get');
  } 
  else if (signalType === 'Metric') {
    dom.tracePresetSelect.innerHTML = OTelMetricDefinitions.map(m => 
      `<option value="${m.key}">${escapeHtml(m.label)}</option>`
    ).join('');

    if (dom.badgePresetCount) {
      dom.badgePresetCount.className = 'badge badge-user';
      dom.badgePresetCount.textContent = '20 Métricas Registradas';
    }
    loadSelectedPreset(OTelMetricDefinitions[0].key);
  } 
  else if (signalType === 'Log') {
    dom.tracePresetSelect.innerHTML = Object.entries(OTelLogs).map(([k, v]) => 
      `<option value="${k}">${escapeHtml(v.label)}</option>`
    ).join('');

    if (dom.badgePresetCount) {
      dom.badgePresetCount.className = 'badge badge-internal';
      dom.badgePresetCount.textContent = '3 Logs Registrados';
    }
    loadSelectedPreset('log-info');
  }
}

// Cargar Ejemplo Seleccionado en el Área de Texto
async function loadSelectedPreset(presetKey) {
  const signalType = dom.telemetryTypeSelect?.value || 'Trace';

  if (signalType === 'Trace') {
    const config = OTelTraces[presetKey] || OTelTraces['otel-get'];
    try {
      const res = await fetch(config.url);
      if (res.ok) {
        const traceObj = await res.json();
        dom.msgValueInput.value = JSON.stringify(traceObj, null, 2);
        dom.msgKeyInput.value = generateDispersedKey(config.businessKey);
        showToast(`✔ ${config.label} cargada con éxito`, 'info');
      }
    } catch (err) {
      showToast(`Error al cargar la traza: ${err.message}`, 'error');
    }
  } 
  else if (signalType === 'Metric') {
    if (!cachedMetricsCatalog) {
      try {
        const res = await fetch('/data/otel_metrics_catalog.json');
        if (res.ok) cachedMetricsCatalog = await res.json();
      } catch (_) {}
    }

    const metricObj = cachedMetricsCatalog?.[presetKey];
    if (metricObj) {
      dom.msgValueInput.value = JSON.stringify(metricObj, null, 2);
      dom.msgKeyInput.value = generateDispersedKey(`METRIC-${presetKey}`);
      showToast(`✔ Métrica '${presetKey}' cargada (${metricObj.Type})`, 'info');
    }
  } 
  else if (signalType === 'Log') {
    const logConfig = OTelLogs[presetKey] || OTelLogs['log-info'];
    dom.msgValueInput.value = JSON.stringify(logConfig.data, null, 2);
    dom.msgKeyInput.value = generateDispersedKey(logConfig.businessKey);
    showToast(`✔ ${logConfig.label} cargado con éxito`, 'info');
  }
}

function getBusinessKeyForCurrentPreset(presetKey) {
  const signalType = dom.telemetryTypeSelect?.value || 'Trace';
  if (signalType === 'Trace') {
    return OTelTraces[presetKey]?.businessKey || '8172201-IN';
  } else if (signalType === 'Metric') {
    return `METRIC-${presetKey || 'sample'}`;
  } else {
    return OTelLogs[presetKey]?.businessKey || 'LOG-PROD';
  }
}

function formatTextareaJson() {
  try {
    const parsed = JSON.parse(dom.msgValueInput.value);
    dom.msgValueInput.value = JSON.stringify(parsed, null, 2);
    showToast('JSON formateado correctamente', 'info');
  } catch (err) {
    showToast('Error de sintaxis JSON al intentar formatear', 'error');
  }
}

// Toast Notifier
function showToast(message, type = 'info') {
  const toast = document.createElement('div');
  toast.className = `toast toast-${type}`;
  toast.textContent = message;

  dom.toastContainer.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(10px)';
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}

function escapeHtml(str) {
  if (!str) return '';
  return str.replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
}

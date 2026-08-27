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
  msgValueInput: document.getElementById('msg-value-input'),
  btnSampleJson: document.getElementById('btn-sample-json'),
  btnFormatJson: document.getElementById('btn-format-json'),
  messagesStream: document.getElementById('messages-stream'),
  btnClearLogs: document.getElementById('btn-clear-logs'),
  toastContainer: document.getElementById('toast-container')
};

// Inicialización
document.addEventListener('DOMContentLoaded', () => {
  setupTabs();
  setupModals();
  setupEvents();
  
  // Cargar JSON transaccional por defecto al inicio
  loadPresetJson('transfer');
  
  refreshAll();
  
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
  
  // Presets de JSON
  document.querySelectorAll('[data-preset]').forEach(btn => {
    btn.addEventListener('click', (e) => {
      const presetType = e.target.getAttribute('data-preset');
      loadPresetJson(presetType);
    });
  });

  dom.btnSampleJson?.addEventListener('click', () => loadPresetJson('random'));
  dom.btnFormatJson?.addEventListener('click', formatTextareaJson);

  dom.btnClearLogs.addEventListener('click', () => {
    dom.messagesStream.innerHTML = `
      <div class="console-empty">
        <p>Historial limpiado. Los nuevos eventos aparecerán aquí.</p>
      </div>`;
  });
}

// Recarga General
async function refreshAll() {
  await Promise.all([checkHealth(), fetchTopics()]);
}

// Comprobar Salud del Clúster
async function checkHealth() {
  try {
    const res = await fetch('/api/health');
    if (!res.ok) throw new Error('Error de red');
    const data = await res.json();
    state.clusterHealth = data;

    if (data.isConnected) {
      dom.statusBadge.className = 'status-badge status-connected';
      dom.statusText.textContent = `Red Hat AMQ Streams (${data.totalTopics} tópicos)`;
      dom.statClusterHealth.textContent = 'En Línea';
      dom.statClusterHealth.style.color = 'var(--accent-emerald)';
    } else {
      dom.statusBadge.className = 'status-badge status-disconnected';
      dom.statusText.textContent = 'Kafka Desconectado';
      dom.statClusterHealth.textContent = 'Sin Conexión';
      dom.statClusterHealth.style.color = 'var(--accent-rose)';
    }
  } catch (err) {
    dom.statusBadge.className = 'status-badge status-disconnected';
    dom.statusText.textContent = 'Broker Inaccesible';
    dom.statClusterHealth.textContent = 'Error';
  }
}

// Obtener Tópicos
async function fetchTopics() {
  try {
    const res = await fetch(`/api/topics?includeInternal=${state.includeInternal}`);
    if (!res.ok) throw new Error('Error al listar tópicos');
    const topics = await res.json();
    state.topics = topics;

    renderTopicsTable(topics);
    updateTopicsSelect(topics);
    updateStats(topics);
  } catch (err) {
    dom.topicsTableBody.innerHTML = `
      <tr>
        <td colspan="5" class="table-loading" style="color: var(--accent-rose);">
          Error al cargar los tópicos de Kafka: ${err.message}
        </td>
      </tr>`;
  }
}

// Renderizar Tabla de Tópicos
function renderTopicsTable(topics) {
  if (!topics || topics.length === 0) {
    dom.topicsTableBody.innerHTML = `
      <tr>
        <td colspan="5" class="table-loading">
          No hay tópicos disponibles. ¡Crea uno nuevo con el botón superior!
        </td>
      </tr>`;
    return;
  }

  dom.topicsTableBody.innerHTML = topics.map(t => `
    <tr>
      <td>
        <strong style="color: var(--text-primary); font-size: 0.95rem;">${escapeHtml(t.name)}</strong>
      </td>
      <td>
        <span class="badge badge-user">${t.partitionsCount} particiones</span>
      </td>
      <td>${t.replicationFactor}x</td>
      <td>
        <span class="badge ${t.isInternal ? 'badge-internal' : 'badge-user'}">
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

// Actualizar Select del formulario
function updateTopicsSelect(topics) {
  const userTopics = topics.filter(t => !t.isInternal);
  const currentVal = dom.msgTopicSelect.value;

  dom.msgTopicSelect.innerHTML = userTopics.map(t => 
    `<option value="${escapeHtml(t.name)}">${escapeHtml(t.name)} (${t.partitionsCount} part.)</option>`
  ).join('');

  // Mantener tp.observability.application-log.emitted.v1 por defecto
  if (userTopics.some(t => t.name === 'tp.observability.application-log.emitted.v1')) {
    dom.msgTopicSelect.value = 'tp.observability.application-log.emitted.v1';
  } else if (userTopics.some(t => t.name === currentVal)) {
    dom.msgTopicSelect.value = currentVal;
  } else if (userTopics.length > 0) {
    dom.msgTopicSelect.value = userTopics[0].name;
  }
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

  const btn = document.getElementById('btn-submit-create');
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

  try {
    const res = await fetch('/api/messages/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topic, key, value })
    });

    const data = await res.json();
    if (!res.ok) throw new Error(data.message || 'Error al enviar');

    state.totalSentSession += 1;
    dom.statSentCount.textContent = state.totalSentSession;

    showToast(`✔ Evento publicado en '${topic}' [P:${data.result.partition}, Offset:#${data.result.offset}]`, 'success');
    appendSingleToStream(data.result, value);
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
      <span class="log-meta">📦 LOTE DE ${batch.totalSent} TRANSACCIONES ENVIADO</span>
      <span>${new Date().toLocaleTimeString()}</span>
    </div>
    <div class="log-preview">Destino: <strong>${batch.targetTopic}</strong> | Tiempo de Publicación: <strong>${batch.elapsedMilliseconds.toFixed(1)} ms</strong></div>
  `;
  dom.messagesStream.prepend(batchHeader);

  batch.results.forEach((item, idx) => {
    const logItem = document.createElement('div');
    logItem.className = 'log-item log-success';
    logItem.innerHTML = `
      <div class="log-header">
        <span class="log-meta">[#${idx + 1}] Partición: ${item.partition} | Offset: #${item.offset}</span>
        <span>${new Date(item.timestamp).toLocaleTimeString()}</span>
      </div>
      <div class="log-preview">Clave: <code>${item.key || 'N/A'}</code> | Estado: <span style="color: var(--accent-emerald);">${item.status}</span></div>
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

// Cargar Presets de JSON Estructurado por Defecto
function loadPresetJson(type = 'transfer') {
  const hexSuffix = Math.floor(1000 + Math.random() * 9000).toString(16).toUpperCase();
  const randomAmount = (Math.random() * 800 + 25).toFixed(2);
  const originAcct = 'ACCT-' + Math.floor(10000000 + Math.random() * 90000000);
  const destAcct = 'ACCT-' + Math.floor(20000000 + Math.random() * 90000000);

  let presetObj = {
    eventId: crypto.randomUUID(),
    sequence: 1,
    transactionId: `TXN-${new Date().toISOString().slice(0,10).replace(/-/g,'')}-0001-${hexSuffix}`,
    originAccount: originAcct,
    destinationAccount: destAcct,
    amount: parseFloat(randomAmount),
    currency: "USD",
    transactionType: "TRANSFER",
    channel: "WEB_PORTAL",
    status: "PENDING",
    emittedAt: new Date().toISOString(),
    metadata: {
      sourceSystem: "ProdubancoWebPortal",
      clientEnvironment: "Development",
      framework: ".NET 10 Hexagonal"
    }
  };

  if (type === 'atm') {
    presetObj.transactionType = "WITHDRAWAL";
    presetObj.channel = "ATM";
    presetObj.amount = 200.00;
    presetObj.destinationAccount = null;
  } else if (type === 'qr') {
    presetObj.transactionType = "QR_PAYMENT";
    presetObj.channel = "MOBILE_APP";
    presetObj.amount = 35.50;
  }

  dom.msgValueInput.value = JSON.stringify(presetObj, null, 2);
  dom.msgKeyInput.value = presetObj.originAccount;
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

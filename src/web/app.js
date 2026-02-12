const API_BASE = '/web/api';

const state = {
  currentView: 'overview',
  projects: [],
  health: null,
  logsTimer: null,
  dbConnections: [],
  dbBindings: [],
};

const elements = {
  navLinks: Array.from(document.querySelectorAll('.nav-link')),
  viewTitle: document.getElementById('viewTitle'),
  viewSubtitle: document.getElementById('viewSubtitle'),
  refreshButton: document.getElementById('refreshButton'),
  sidebarStatus: document.getElementById('sidebarStatus'),
  logoutButton: document.getElementById('logoutButton'),
  loginOverlay: document.getElementById('loginOverlay'),
  loginForm: document.getElementById('loginForm'),
  loginToken: document.getElementById('loginToken'),
  loginMessage: document.getElementById('loginMessage'),
  healthSummary: document.getElementById('healthSummary'),
  envSummary: document.getElementById('envSummary'),
  errorList: document.getElementById('errorList'),
  projectsGrid: document.getElementById('projectsGrid'),
  projectDetail: document.getElementById('projectDetail'),
  projectDetailBody: document.getElementById('projectDetailBody'),
  logsProject: document.getElementById('logsProject'),
  logsLevel: document.getElementById('logsLevel'),
  logsRefresh: document.getElementById('logsRefresh'),
  logsList: document.getElementById('logsList'),
  cronList: document.getElementById('cronList'),
  envProject: document.getElementById('envProject'),
  envRefresh: document.getElementById('envRefresh'),
  envKeys: document.getElementById('envKeys'),
  envForm: document.getElementById('envForm'),
  envKey: document.getElementById('envKey'),
  envValue: document.getElementById('envValue'),
  patchProject: document.getElementById('patchProject'),
  patchSpec: document.getElementById('patchSpec'),
  patchPreview: document.getElementById('patchPreview'),
  patchApply: document.getElementById('patchApply'),
  patchPreviewOutput: document.getElementById('patchPreviewOutput'),
  patchResult: document.getElementById('patchResult'),

  dbOverallStatus: document.getElementById('dbOverallStatus'),
  dbConnectionForm: document.getElementById('dbConnectionForm'),
  dbConnectionName: document.getElementById('dbConnectionName'),
  dbConnectionDsn: document.getElementById('dbConnectionDsn'),
  dbConnectionSslMode: document.getElementById('dbConnectionSslMode'),
  dbConnectionSslVerify: document.getElementById('dbConnectionSslVerify'),
  dbConnectionsList: document.getElementById('dbConnectionsList'),
  dbProjectSelect: document.getElementById('dbProjectSelect'),
  dbBindingOptions: document.getElementById('dbBindingOptions'),
  dbBindingSave: document.getElementById('dbBindingSave'),
  dbDualEnabled: document.getElementById('dbDualEnabled'),
  dbDualPrimary: document.getElementById('dbDualPrimary'),
  dbDualSecondary: document.getElementById('dbDualSecondary'),
  dbDualSave: document.getElementById('dbDualSave'),
  dbSyncNow: document.getElementById('dbSyncNow'),
  dbDualStatus: document.getElementById('dbDualStatus'),
  dbConnectionSelect: document.getElementById('dbConnectionSelect'),
  dbSchemaSelect: document.getElementById('dbSchemaSelect'),
  dbTableSelect: document.getElementById('dbTableSelect'),
  dbLoadRows: document.getElementById('dbLoadRows'),
  dbSchemaInfo: document.getElementById('dbSchemaInfo'),
  dbRows: document.getElementById('dbRows'),
  dbSqlForm: document.getElementById('dbSqlForm'),
  dbSqlInput: document.getElementById('dbSqlInput'),
  dbEnableWrites: document.getElementById('dbEnableWrites'),
  dbWriteConfirm: document.getElementById('dbWriteConfirm'),
  dbSqlResult: document.getElementById('dbSqlResult'),
  dbMigrateForm: document.getElementById('dbMigrateForm'),
  dbMigrateSource: document.getElementById('dbMigrateSource'),
  dbMigrateTarget: document.getElementById('dbMigrateTarget'),
  dbMigrateResult: document.getElementById('dbMigrateResult'),
};

const subtitles = {
  overview: 'System health and environment status.',
  projects: 'Manage projects, diagnostics, and configs.',
  logs: 'Recent logs with filters and polling.',
  cronjobs: 'Cron scheduler status and jobs.',
  envvault: 'Env Vault keys (masked) and updates.',
  database: 'DB console: bindings, schema/table browser, SQL, migration, and dual sync.',
  patches: 'Apply PM Change Spec v1 patches.',
};

function setStatus(text) {
  if (elements.sidebarStatus) {
    elements.sidebarStatus.textContent = text;
  }
}

function showLogin(message) {
  elements.loginOverlay.classList.add('active');
  elements.loginMessage.textContent = message || '';
}

function hideLogin() {
  elements.loginOverlay.classList.remove('active');
  elements.loginMessage.textContent = '';
  elements.loginToken.value = '';
}

async function apiFetch(path, options = {}) {
  const response = await fetch(path, {
    credentials: 'include',
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
    ...options,
  });
  if (response.status === 401) {
    showLogin('Session expired. Please login again.');
    throw new Error('Unauthorized');
  }
  return response;
}

function setActiveView(view) {
  state.currentView = view;
  elements.navLinks.forEach((button) => {
    button.classList.toggle('active', button.dataset.view === view);
  });
  document.querySelectorAll('.view').forEach((section) => {
    section.classList.toggle('active', section.id === `view-${view}`);
  });
  elements.viewTitle.textContent = view.charAt(0).toUpperCase() + view.slice(1);
  elements.viewSubtitle.textContent = subtitles[view] || '';
  refreshCurrentView();
}

function renderStats(container, items) {
  container.innerHTML = items
    .map(
      (item) => `
        <div class="stat-item">
          <span>${item.label}</span>
          <strong>${item.value}</strong>
        </div>
      `,
    )
    .join('');
}

function renderList(container, items) {
  container.innerHTML = items
    .map(
      (item) => `
        <div class="list-item">
          <h4>${item.title}</h4>
          <div class="muted">${item.subtitle}</div>
          ${item.detail ? `<div class="muted">${item.detail}</div>` : ''}
        </div>
      `,
    )
    .join('');
}

async function loadHealth() {
  const response = await apiFetch(`${API_BASE}/health`);
  const data = await response.json();
  state.health = data;
  renderStats(elements.healthSummary, [
    { label: 'Config DB', value: data.status.configDbOk ? '✅ OK' : '❌ Error' },
    { label: 'Env Vault', value: data.status.vaultOk ? '✅ OK' : '❌ Error' },
    { label: 'Timestamp', value: data.timestamp },
  ]);
  renderStats(elements.envSummary, [
    { label: 'BOT_TOKEN', value: data.env.botTokenConfigured ? '✅ Set' : '⚠️ Missing' },
    { label: 'ADMIN_TELEGRAM_ID', value: data.env.adminTelegramConfigured ? '✅ Set' : '⚠️ Missing' },
    { label: 'DATABASE_URL', value: data.env.databaseUrlConfigured ? '✅ Set' : '⚠️ Missing' },
  ]);
  renderList(
    elements.errorList,
    data.lastErrors.map((entry) => ({
      title: entry.message,
      subtitle: `${entry.level.toUpperCase()} — ${new Date(entry.createdAt).toLocaleString()}`,
    })),
  );
  setStatus(data.status.configDbOk && data.status.vaultOk ? 'Healthy' : 'Attention needed');
}

async function loadProjects() {
  const response = await apiFetch(`${API_BASE}/projects`);
  const data = await response.json();
  state.projects = data.projects || [];
  const cards = state.projects
    .map(
      (project) => `
        <div class="card">
          <h3>${project.name}</h3>
          <div class="muted">ID: ${project.id}</div>
          <div class="stat-list">
            <div class="stat-item"><span>Repo</span><strong>${project.repoSlug || '—'}</strong></div>
            <div class="stat-item"><span>Base</span><strong>${project.baseBranch || '—'}</strong></div>
            <div class="stat-item"><span>Render URL</span><strong>${project.renderServiceUrl || '—'}</strong></div>
          </div>
          <button class="secondary" data-project="${project.id}">View details</button>
        </div>
      `,
    )
    .join('');
  elements.projectsGrid.innerHTML = cards || '<div class="card">No projects found.</div>';

  const options = ['<option value="">All projects</option>']
    .concat(
      state.projects.map(
        (project) => `<option value="${project.id}">${project.name}</option>`,
      ),
    )
    .join('');
  elements.logsProject.innerHTML = options;

  const selectOptions = state.projects
    .map((project) => `<option value="${project.id}">${project.name}</option>`)
    .join('');
  elements.envProject.innerHTML = selectOptions;
  elements.patchProject.innerHTML = selectOptions;
}

async function loadProjectDetail(projectId) {
  const response = await apiFetch(`${API_BASE}/projects/${encodeURIComponent(projectId)}`);
  const data = await response.json();
  if (!data.ok) return;
  elements.projectDetail.hidden = false;
  renderStats(elements.projectDetailBody, [
    { label: 'Repo', value: data.project.repoSlug || '—' },
    { label: 'Base branch', value: data.project.baseBranch || '—' },
    { label: 'Start command', value: data.project.startCommand || '—' },
    { label: 'Diagnostic command', value: data.project.diagnosticCommand || '—' },
    { label: 'Render URL', value: data.project.renderServiceUrl || '—' },
  ]);
}

async function loadLogs() {
  const project = elements.logsProject.value;
  const level = elements.logsLevel.value;
  const response = await apiFetch(`${API_BASE}/logs?project=${encodeURIComponent(project)}&level=${encodeURIComponent(level)}&page=0`);
  const data = await response.json();
  const entries = data.entries || [];
  if (!entries.length) {
    elements.logsList.innerHTML = '<div class="list-item">No logs yet.</div>';
    return;
  }
  renderList(
    elements.logsList,
    entries.map((entry) => ({
      title: `${entry.projectName} — ${entry.service}`,
      subtitle: `${entry.level.toUpperCase()} • ${new Date(entry.createdAt).toLocaleString()}`,
      detail: entry.message,
    })),
  );
}

async function loadCronjobs() {
  const response = await apiFetch(`${API_BASE}/cronjobs`);
  const data = await response.json();
  if (!data.ok) {
    elements.cronList.innerHTML = `<div class="list-item">${data.error || 'Cronjobs unavailable.'}</div>`;
    return;
  }
  renderList(
    elements.cronList,
    data.jobs.map((job) => ({
      title: job.title,
      subtitle: `${job.enabled ? '✅ Enabled' : '⏸️ Paused'} • ${job.schedule || '—'}`,
      detail: job.url || '',
    })),
  );
}

async function loadEnvVault() {
  const projectId = elements.envProject.value;
  const response = await apiFetch(`${API_BASE}/envvault/${encodeURIComponent(projectId)}`);
  const data = await response.json();
  if (!data.ok) {
    elements.envKeys.innerHTML = '<span class="pill">Env Vault unavailable</span>';
    return;
  }
  elements.envKeys.innerHTML = data.keys
    .map((entry) => `<span class="pill">${entry.key} · ${entry.valueMask}</span>`)
    .join('');
}


function dbConnectionOptionHtml(connection) {
  return `<option value="${connection.id}">${connection.name}</option>`;
}

function getSelectedProjectBinding() {
  const projectId = elements.dbProjectSelect.value;
  return state.dbBindings.find((item) => item.projectId === projectId) || null;
}

async function loadDbConnections() {
  const response = await apiFetch(`${API_BASE}/db/connections`);
  const data = await response.json();
  state.dbConnections = data.connections || [];
  state.dbBindings = data.bindings || [];
  renderStats(elements.dbOverallStatus, [
    { label: 'Primary DB', value: data.health?.primaryConfigured ? '✅ Configured' : '⚠️ Missing' },
    { label: 'Secondary DB', value: data.health?.secondaryConfigured ? '✅ Configured' : '⚠️ Missing' },
    { label: 'Saved connections', value: String(state.dbConnections.length) },
  ]);
  elements.dbConnectionsList.innerHTML = state.dbConnections
    .map((connection) => `<div class="list-item"><h4>${connection.name}</h4><div class="muted">${connection.dsnMasked || '••••'}</div><button class="secondary" data-db-remove="${connection.id}">Remove</button></div>`)
    .join('') || '<div class="list-item">No connections yet.</div>';
  const options = state.dbConnections.map(dbConnectionOptionHtml).join('');
  elements.dbConnectionSelect.innerHTML = options;
  elements.dbDualPrimary.innerHTML = `<option value="">Primary</option>${options}`;
  elements.dbDualSecondary.innerHTML = `<option value="">Secondary</option>${options}`;
  elements.dbMigrateSource.innerHTML = `<option value="">Select source</option>${options}`;
  elements.dbMigrateTarget.innerHTML = `<option value="">Select target</option>${options}`;

  const projectOptions = state.projects.map((project) => `<option value="${project.id}">${project.name}</option>`).join('');
  elements.dbProjectSelect.innerHTML = projectOptions;
  renderDbBindingsForProject();
  await loadDbSchemas();
}

function renderDbBindingsForProject() {
  const binding = getSelectedProjectBinding();
  const selected = new Set(binding?.connectionIds || []);
  elements.dbBindingOptions.innerHTML = state.dbConnections
    .map((connection) => `<label class="pill"><input type="checkbox" data-db-bind="${connection.id}" ${selected.has(connection.id) ? 'checked' : ''}/> ${connection.name}</label>`)
    .join('') || '<span class="pill">No connections</span>';
  const dualDb = binding?.dualDb || {};
  elements.dbDualEnabled.checked = Boolean(dualDb.enabled);
  elements.dbDualPrimary.value = dualDb.primaryConnectionId || '';
  elements.dbDualSecondary.value = dualDb.secondaryConnectionId || '';
  elements.dbDualStatus.textContent = dualDb.lastSyncAt
    ? `Last sync: ${new Date(dualDb.lastSyncAt).toLocaleString()} (${dualDb.lastSyncResult?.ok ? 'ok' : 'needs attention'})`
    : 'No sync history yet.';
}

async function loadDbSchemas() {
  const connectionId = elements.dbConnectionSelect.value;
  if (!connectionId) {
    elements.dbSchemaSelect.innerHTML = '';
    elements.dbTableSelect.innerHTML = '';
    return;
  }
  const schemaRes = await apiFetch(`${API_BASE}/db/${encodeURIComponent(connectionId)}/schemas`);
  const schemaData = await schemaRes.json();
  elements.dbSchemaSelect.innerHTML = (schemaData.schemas || []).map((s) => `<option value="${s}">${s}</option>`).join('');
  await loadDbTables();
}

async function loadDbTables() {
  const connectionId = elements.dbConnectionSelect.value;
  const schema = elements.dbSchemaSelect.value || 'public';
  if (!connectionId) return;
  const tableRes = await apiFetch(`${API_BASE}/db/${encodeURIComponent(connectionId)}/tables?schema=${encodeURIComponent(schema)}`);
  const tableData = await tableRes.json();
  elements.dbTableSelect.innerHTML = (tableData.tables || []).map((t) => `<option value="${t}">${t}</option>`).join('');
}

async function loadDbRows() {
  const connectionId = elements.dbConnectionSelect.value;
  const schema = elements.dbSchemaSelect.value || 'public';
  const table = elements.dbTableSelect.value;
  if (!connectionId || !table) return;
  const rowRes = await apiFetch(`${API_BASE}/db/${encodeURIComponent(connectionId)}/table-rows?schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}&page=0`);
  const data = await rowRes.json();
  elements.dbSchemaInfo.innerHTML = (data.columns || [])
    .map((column) => `<div class="list-item"><h4>${column.column_name}</h4><div class="muted">${column.data_type}</div></div>`)
    .join('');
  elements.dbRows.textContent = JSON.stringify(data.rows || [], null, 2);
}

async function loadDatabaseView() {
  await loadDbConnections();
}

function parseSpecPreview(text) {
  const blocks = text
    .split(/\n\s*\n/)
    .map((block) => block.trim())
    .filter(Boolean);
  if (!blocks.length) {
    return 'No blocks detected.';
  }
  const lines = blocks.map((block, index) => {
    const fileMatch = block.match(/^FILE:\s*(.+)$/m);
    const opMatch = block.match(/^OP:\s*(.+)$/m);
    return `#${index + 1} FILE=${fileMatch ? fileMatch[1].trim() : '—'} OP=${opMatch ? opMatch[1].trim() : '—'}`;
  });
  return lines.join('\n');
}

async function applyPatchSpec() {
  const projectId = elements.patchProject.value;
  const specText = elements.patchSpec.value.trim();
  if (!specText) {
    elements.patchResult.textContent = 'Paste a change spec before applying.';
    return;
  }
  elements.patchResult.textContent = 'Applying patch…';
  const response = await apiFetch(`${API_BASE}/patch/apply`, {
    method: 'POST',
    body: JSON.stringify({ projectId, specText }),
  });
  const result = await response.json();
  if (!result.ok) {
    const details = result.details ? `\n${JSON.stringify(result.details, null, 2)}` : '';
    elements.patchResult.textContent = `${result.error || 'Patch failed.'}${details}`;
    return;
  }
  const output = [];
  if (result.result?.prUrl) {
    output.push(`PR: ${result.result.prUrl}`);
  }
  if (result.result?.summary) {
    output.push(`Summary: applied=${result.result.summary.applied}, skipped=${result.result.summary.skipped}, failed=${result.result.summary.failed}`);
  }
  if (result.result?.diffPreview) {
    output.push(`Diff preview:\n${result.result.diffPreview}`);
  }
  if (result.result?.elapsedSec != null) {
    output.push(`Elapsed: ${result.result.elapsedSec}s`);
  }
  elements.patchResult.textContent = output.join('\n') || 'Patch applied.';
}

async function refreshCurrentView() {
  clearInterval(state.logsTimer);
  state.logsTimer = null;
  switch (state.currentView) {
    case 'overview':
      await loadHealth();
      break;
    case 'projects':
      await loadProjects();
      break;
    case 'logs':
      await loadLogs();
      state.logsTimer = setInterval(loadLogs, 5000);
      break;
    case 'cronjobs':
      await loadCronjobs();
      break;
    case 'envvault':
      await loadEnvVault();
      break;
    case 'database':
      await loadDatabaseView();
      break;
    case 'patches':
      elements.patchPreviewOutput.textContent = '';
      elements.patchResult.textContent = '';
      break;
    default:
      break;
  }
}

async function initDashboard() {
  await loadHealth();
  await loadProjects();
  await loadLogs();
}

function setupEventHandlers() {
  elements.navLinks.forEach((button) => {
    button.addEventListener('click', () => setActiveView(button.dataset.view));
  });

  elements.refreshButton.addEventListener('click', refreshCurrentView);

  elements.logoutButton.addEventListener('click', async () => {
    await fetch('/web/logout', { method: 'POST', credentials: 'include' });
    showLogin('Logged out. Please login again.');
  });

  elements.loginForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const token = elements.loginToken.value.trim();
    if (!token) return;
    elements.loginMessage.textContent = 'Signing in…';
    const response = await fetch('/web/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token }),
    });
    const data = await response.json();
    if (!data.ok) {
      elements.loginMessage.textContent = data.error || 'Login failed.';
      return;
    }
    hideLogin();
    await initDashboard();
    setActiveView(state.currentView);
  });

  elements.projectsGrid.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-project]');
    if (!button) return;
    loadProjectDetail(button.dataset.project);
  });

  elements.logsRefresh.addEventListener('click', loadLogs);
  elements.logsProject.addEventListener('change', loadLogs);
  elements.logsLevel.addEventListener('change', loadLogs);

  elements.envRefresh.addEventListener('click', loadEnvVault);
  elements.envProject.addEventListener('change', loadEnvVault);


  elements.dbConnectionForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const response = await apiFetch(`${API_BASE}/db/connections`, {
      method: 'POST',
      body: JSON.stringify({
        name: elements.dbConnectionName.value.trim(),
        dsn: elements.dbConnectionDsn.value.trim(),
        sslMode: elements.dbConnectionSslMode.value,
        sslVerify: elements.dbConnectionSslVerify.checked,
      }),
    });
    const data = await response.json();
    if (!data.ok) {
      elements.dbSqlResult.textContent = data.error || 'Failed to add connection.';
      return;
    }
    elements.dbConnectionName.value = '';
    elements.dbConnectionDsn.value = '';
    await loadDbConnections();
  });

  elements.dbConnectionsList.addEventListener('click', async (event) => {
    const button = event.target.closest('button[data-db-remove]');
    if (!button) return;
    await apiFetch(`${API_BASE}/db/connections/${encodeURIComponent(button.dataset.dbRemove)}`, { method: 'DELETE' });
    await loadDbConnections();
  });

  elements.dbProjectSelect.addEventListener('change', renderDbBindingsForProject);

  elements.dbBindingSave.addEventListener('click', async () => {
    const selected = Array.from(elements.dbBindingOptions.querySelectorAll('input[data-db-bind]:checked')).map((input) => input.dataset.dbBind);
    await apiFetch(`${API_BASE}/db/project-bindings`, {
      method: 'POST',
      body: JSON.stringify({ projectId: elements.dbProjectSelect.value, connectionIds: selected }),
    });
    await loadDbConnections();
  });

  elements.dbDualSave.addEventListener('click', async () => {
    const response = await apiFetch(`${API_BASE}/db/dual-mode`, {
      method: 'POST',
      body: JSON.stringify({
        projectId: elements.dbProjectSelect.value,
        enabled: elements.dbDualEnabled.checked,
        primaryConnectionId: elements.dbDualPrimary.value,
        secondaryConnectionId: elements.dbDualSecondary.value,
      }),
    });
    const data = await response.json();
    elements.dbDualStatus.textContent = data.ok ? 'Dual DB mode updated.' : data.error;
    await loadDbConnections();
  });

  elements.dbSyncNow.addEventListener('click', async () => {
    const response = await apiFetch(`${API_BASE}/db/sync`, {
      method: 'POST',
      body: JSON.stringify({ projectId: elements.dbProjectSelect.value }),
    });
    const data = await response.json();
    elements.dbDualStatus.textContent = data.ok ? `Sync OK at ${new Date(data.lastSyncAt).toLocaleString()}` : data.error;
    await loadDbConnections();
  });

  elements.dbConnectionSelect.addEventListener('change', loadDbSchemas);
  elements.dbSchemaSelect.addEventListener('change', loadDbTables);
  elements.dbLoadRows.addEventListener('click', loadDbRows);

  elements.dbSqlForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const connectionId = elements.dbConnectionSelect.value;
    const response = await apiFetch(`${API_BASE}/db/${encodeURIComponent(connectionId)}/sql`, {
      method: 'POST',
      body: JSON.stringify({
        sql: elements.dbSqlInput.value,
        enableWrite: elements.dbEnableWrites.checked,
        confirmWrite: elements.dbWriteConfirm.value.trim(),
        projectId: elements.dbProjectSelect.value,
      }),
    });
    const data = await response.json();
    elements.dbSqlResult.textContent = JSON.stringify(data, null, 2);
  });

  elements.dbMigrateForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const sourceConnectionId = elements.dbMigrateSource.value;
    const targetConnectionId = elements.dbMigrateTarget.value;
    const response = await apiFetch(`${API_BASE}/db/migrate`, {
      method: 'POST',
      body: JSON.stringify({ sourceConnectionId, targetConnectionId }),
    });
    const data = await response.json();
    elements.dbMigrateResult.textContent = JSON.stringify(data, null, 2);
  });

  elements.envForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const projectId = elements.envProject.value;
    const key = elements.envKey.value.trim();
    const value = elements.envValue.value.trim();
    if (!key || !value) return;
    const response = await apiFetch(`${API_BASE}/envvault/${encodeURIComponent(projectId)}`, {
      method: 'POST',
      body: JSON.stringify({ key, value }),
    });
    const data = await response.json();
    if (!data.ok) {
      elements.envKeys.innerHTML = `<span class="pill">${data.error || 'Failed to save.'}</span>`;
      return;
    }
    elements.envKey.value = '';
    elements.envValue.value = '';
    await loadEnvVault();
  });

  elements.patchPreview.addEventListener('click', () => {
    elements.patchPreviewOutput.textContent = parseSpecPreview(elements.patchSpec.value);
  });

  elements.patchApply.addEventListener('click', applyPatchSpec);
}

async function boot() {
  setupEventHandlers();
  try {
    await initDashboard();
    hideLogin();
  } catch (error) {
    showLogin('Please login to access the dashboard.');
  }
}

boot();

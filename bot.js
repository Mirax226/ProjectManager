require('dotenv').config();

const http = require('http');
const https = require('https');
const { Bot, InlineKeyboard, Keyboard } = require('grammy');
const { Pool } = require('pg');

const { loadProjects, saveProjects, findProjectById } = require('./projectsStore');
const { getPool: getConfigPool } = require('./configStore');
const { setUserState, getUserState, clearUserState } = require('./state');
const {
  prepareRepository,
  createWorkingBranch,
  applyPatchToRepo,
  commitAndPush,
  DEFAULT_BASE_BRANCH,
  fetchDryRun,
  getRepoPath,
} = require('./gitUtils');
const { createPullRequest, measureGithubLatency } = require('./githubUtils');
const { loadGlobalSettings, saveGlobalSettings } = require('./settingsStore');
const { loadSupabaseConnections, findSupabaseConnection } = require('./supabaseConnectionsStore');
const { runCommandInProject } = require('./shellUtils');

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_TELEGRAM_ID = process.env.ADMIN_TELEGRAM_ID;

if (!BOT_TOKEN) {
  throw new Error('BOT_TOKEN is required');
}

if (!ADMIN_TELEGRAM_ID) {
  throw new Error('ADMIN_TELEGRAM_ID is required');
}

const bot = new Bot(BOT_TOKEN);
const supabasePools = new Map();
let botStarted = false;
let botRetryTimeout = null;

async function testConfigDbConnection() {
  try {
    const db = await getConfigPool();
    if (!db) {
      return;
    }
    await db.query('SELECT 1');
    console.log('Config DB: OK');
  } catch (error) {
    console.error('Config DB connection failed', error);
  }
}

const mainKeyboard = new Keyboard()
  .text('📁 Projects')
  .row()
  .text('🗄️ Supabase')
  .row()
  .text('⚙️ Settings')
  .row()
  .text('📶 Ping test')
  .row()
  .text('❓ Help')
  .resized();

const projectsKeyboard = new Keyboard()
  .text('📄 List projects')
  .row()
  .text('➕ Add project')
  .row()
  .text('⬅️ Back')
  .resized();

bot.use(async (ctx, next) => {
  if (!ctx.from) return;
  if (String(ctx.from.id) !== String(ADMIN_TELEGRAM_ID)) {
    if (ctx.updateType === 'message' || ctx.updateType === 'callback_query') {
      await ctx.reply('Unauthorized');
    }
    return;
  }
  return next();
});

bot.on('message', async (ctx, next) => {
  const state = getUserState(ctx.from?.id);
  if (state) {
    await handleStatefulMessage(ctx, state);
    return;
  }
  return next();
});

bot.command('start', async (ctx) => {
  await ctx.reply('Patch Runner Bot ready.', { reply_markup: mainKeyboard });
});

bot.hears('❓ Help', async (ctx) => {
  await ctx.reply(
    'Use this bot to manage projects and apply git patches that will be turned into GitHub PRs.',
    { reply_markup: mainKeyboard },
  );
});

bot.hears('📁 Projects', async (ctx) => {
  await ctx.reply('Project actions:', { reply_markup: projectsKeyboard });
});

bot.hears('🗄️ Supabase', async (ctx) => {
  await renderSupabaseConnectionsMenu(ctx);
});

bot.hears('⬅️ Back', async (ctx) => {
  await ctx.reply('Main menu:', { reply_markup: mainKeyboard });
});

bot.hears('➕ Add project', async (ctx) => {
  setUserState(ctx.from.id, { type: 'add_project', step: 1, data: {} });
  await ctx.reply('Send a short ID for the project (e.g. daily-system)');
});

bot.hears('📄 List projects', async (ctx) => {
  await renderProjectsList(ctx);
});

bot.hears('⚙️ Settings', async (ctx) => {
  await renderGlobalSettings(ctx);
});

bot.hears('📶 Ping test', async (ctx) => {
  const parts = [];
  try {
    const gh = await measureGithubLatency();
    parts.push(`GitHub API: ~${gh} ms`);
  } catch (error) {
    console.error('GitHub ping failed', error);
    parts.push('GitHub API: failed');
  }

  try {
    const projects = await loadProjects();
    const globalSettings = await loadGlobalSettings();
    if (projects.length) {
      const effectiveBaseBranch = projects[0].baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;
      const start = Date.now();
      await fetchDryRun(projects[0], effectiveBaseBranch);
      parts.push(`git fetch (first project): ~${Date.now() - start} ms`);
    } else {
      parts.push('git fetch: no projects yet');
    }
  } catch (error) {
    console.error('Git fetch ping failed', error);
    parts.push('git fetch: failed');
  }

  await ctx.reply(parts.join('\n'), { reply_markup: mainKeyboard });
});

bot.on('callback_query:data', async (ctx) => {
  const data = ctx.callbackQuery.data;
  if (data.startsWith('proj:')) {
    await handleProjectCallback(ctx, data);
    return;
  }
  if (data.startsWith('gsettings:')) {
    await handleGlobalSettingsCallback(ctx, data);
    return;
  }
  if (data.startsWith('supabase:')) {
    await handleSupabaseCallback(ctx, data);
    return;
  }
});

async function handleStatefulMessage(ctx, state) {
  switch (state.type) {
    case 'add_project':
      await handleAddProjectStep(ctx, state);
      break;
    case 'await_patch':
      await handlePatchApplication(ctx, state);
      break;
    case 'rename_project':
      await handleRenameProjectStep(ctx, state);
      break;
    case 'change_base_branch':
      await handleChangeBaseBranchStep(ctx, state);
      break;
    case 'edit_command_input':
      await handleEditCommandInput(ctx, state);
      break;
    case 'edit_render_url':
      await handleEditRenderUrl(ctx, state);
      break;
    case 'edit_supabase':
      await handleEditSupabase(ctx, state);
      break;
    case 'global_change_base':
      await handleGlobalBaseChange(ctx, state);
      break;
    case 'supabase_console':
      await handleSupabaseConsoleMessage(ctx, state);
      break;
    default:
      clearUserState(ctx.from.id);
      break;
  }
}

async function renderProjectsList(ctx) {
  const projects = await loadProjects();
  const globalSettings = await loadGlobalSettings();
  if (!projects.length) {
    await ctx.reply('No projects configured yet.', { reply_markup: projectsKeyboard });
    return;
  }

  const lines = projects
    .map((p, idx) => {
      const effectiveBase = p.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;
      return `${idx + 1}. ${p.id} (base: ${effectiveBase})`;
    })
    .join('\n');

  const inline = new InlineKeyboard();
  projects.forEach((project) => {
    inline
      .text('🧩 Apply patch', `proj:${project.id}:apply_patch`)
      .text('⚙️ Settings', `proj:${project.id}:settings`)
      .text('🗑️ Delete', `proj:${project.id}:delete`)
      .row();
  });

  await ctx.reply(lines, { reply_markup: inline });
}

async function handleProjectCallback(ctx, data) {
  await ctx.answerCallbackQuery();
  const [, projectId, action, extra] = data.split(':');
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project && action !== 'confirm_delete' && action !== 'cancel_delete') {
    await ctx.reply('Project not found.');
    return;
  }

  switch (action) {
    case 'apply_patch':
      setUserState(ctx.from.id, { type: 'await_patch', projectId });
      await ctx.reply('Send the git patch as text or as a .patch/.diff file.');
      break;
    case 'settings':
      await renderProjectSettings(ctx, projectId);
      break;
    case 'delete':
      await renderDeleteConfirmation(ctx, projectId);
      break;
    case 'confirm_delete':
      await deleteProject(ctx, projectId);
      break;
    case 'cancel_delete':
      await ctx.reply('Deletion cancelled.');
      await renderProjectsList(ctx);
      break;
    case 'rename':
      setUserState(ctx.from.id, { type: 'rename_project', step: 1, projectId });
      await ctx.reply('Send the new project name.');
      break;
    case 'change_base':
      setUserState(ctx.from.id, { type: 'change_base_branch', projectId });
      await ctx.reply('Send the new base branch.');
      break;
    case 'commands':
      await renderCommandsScreen(ctx, projectId);
      break;
    case 'cmd_edit':
      setUserState(ctx.from.id, {
        type: 'edit_command_input',
        projectId,
        field: extra,
      });
      await ctx.reply(`Send new value for ${extra}.`);
      break;
    case 'cmd_clear':
      await updateProjectField(projectId, extra, undefined);
      await ctx.reply(`${extra} cleared.`);
      await renderProjectSettings(ctx, projectId);
      break;
    case 'diagnostics':
      await runProjectDiagnostics(ctx, projectId);
      break;
    case 'render':
      await renderRenderUrlsScreen(ctx, projectId);
      break;
    case 'render_ping':
      await pingRenderService(ctx, projectId);
      break;
    case 'render_keepalive_url':
      await showKeepAliveUrl(ctx, projectId);
      break;
    case 'render_deploy':
      await triggerRenderDeploy(ctx, projectId);
      break;
    case 'render_edit':
      setUserState(ctx.from.id, {
        type: 'edit_render_url',
        projectId,
        field: extra,
      });
      await ctx.reply(`Send new value for ${extra}.`);
      break;
    case 'render_clear':
      await updateProjectField(projectId, extra, undefined);
      await ctx.reply(`${extra} cleared.`);
      await renderProjectSettings(ctx, projectId);
      break;
    case 'supabase':
      await renderSupabaseScreen(ctx, projectId);
      break;
    case 'supabase_edit':
      setUserState(ctx.from.id, { type: 'edit_supabase', projectId });
      await ctx.reply('Send new supabaseConnectionId.');
      break;
    case 'supabase_clear':
      await updateProjectField(projectId, 'supabaseConnectionId', undefined);
      await ctx.reply('supabaseConnectionId cleared.');
      await renderProjectSettings(ctx, projectId);
      break;
    case 'set_default':
      await setDefaultProject(projectId);
      await ctx.reply('Default project updated.');
      await renderProjectSettings(ctx, projectId);
      break;
    case 'back':
      await renderProjectsList(ctx);
      break;
    default:
      break;
  }
}

async function handleGlobalSettingsCallback(ctx, data) {
  await ctx.answerCallbackQuery();
  const [, action] = data.split(':');
  switch (action) {
    case 'change_default_base':
      setUserState(ctx.from.id, { type: 'global_change_base' });
      await ctx.reply('Send new default base branch.');
      break;
    case 'clear_default_base':
      await clearDefaultBaseBranch();
      await ctx.reply('Default base branch cleared (using environment default).');
      await renderGlobalSettings(ctx);
      break;
    case 'clear_default_project':
      await clearDefaultProject();
      await ctx.reply('Default project cleared.');
      await renderGlobalSettings(ctx);
      break;
    case 'back':
      await ctx.reply('Main menu:', { reply_markup: mainKeyboard });
      break;
    default:
      break;
  }
}

async function handleSupabaseCallback(ctx, data) {
  await ctx.answerCallbackQuery();
  const [, action, connectionId] = data.split(':');
  switch (action) {
    case 'back':
      clearUserState(ctx.from.id);
      await ctx.reply('Main menu:', { reply_markup: mainKeyboard });
      break;
    case 'connections':
      await renderSupabaseConnectionsMenu(ctx);
      break;
    case 'conn':
      setUserState(ctx.from.id, { type: 'supabase_console', connectionId, mode: null });
      await renderSupabaseConnectionMenu(ctx, connectionId);
      break;
    case 'tables':
      setUserState(ctx.from.id, { type: 'supabase_console', connectionId, mode: null });
      await listSupabaseTables(ctx, connectionId);
      break;
    case 'sql':
      setUserState(ctx.from.id, { type: 'supabase_console', connectionId, mode: 'awaiting-sql' });
      await promptSupabaseSql(ctx, connectionId);
      break;
    default:
      break;
  }
}

async function renderSupabaseConnectionsMenu(ctx) {
  const connections = await loadSupabaseConnections();
  if (!connections.length) {
    await ctx.reply('No Supabase connections configured.', { reply_markup: mainKeyboard });
    return;
  }
  const inline = new InlineKeyboard();
  connections.forEach((connection) => {
    inline.text(`🗄️ ${connection.name}`, `supabase:conn:${connection.id}`).row();
  });
  inline.text('⬅️ Back', 'supabase:back');
  await ctx.reply('Select a Supabase connection:', { reply_markup: inline });
}

async function renderSupabaseConnectionMenu(ctx, connectionId) {
  const connection = await findSupabaseConnection(connectionId);
  if (!connection) {
    await ctx.reply('Supabase connection not found.');
    return;
  }
  const inline = new InlineKeyboard()
    .text('📋 List tables', `supabase:tables:${connectionId}`)
    .row()
    .text('📝 Run SQL', `supabase:sql:${connectionId}`)
    .row()
    .text('⬅️ Back', 'supabase:connections');

  await ctx.reply(`Supabase: ${connection.name}`, { reply_markup: inline });
}

async function handleSupabaseConsoleMessage(ctx, state) {
  if (state.mode !== 'awaiting-sql') {
    return;
  }
  const sql = ctx.message.text?.trim();
  if (!sql) {
    await ctx.reply('Please send the SQL query as text.');
    return;
  }
  await runSupabaseSql(ctx, state.connectionId, sql);
  setUserState(ctx.from.id, { type: 'supabase_console', connectionId: state.connectionId, mode: null });
  await renderSupabaseConnectionMenu(ctx, state.connectionId);
}

async function promptSupabaseSql(ctx, connectionId) {
  const connection = await findSupabaseConnection(connectionId);
  if (!connection) {
    await ctx.reply('Supabase connection not found.');
    return;
  }
  await ctx.reply(`Send the SQL query to execute on ${connection.name}.`);
}

async function listSupabaseTables(ctx, connectionId) {
  const query = `
    SELECT schemaname, tablename
    FROM pg_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY schemaname, tablename;
  `;
  try {
    const { rows } = await runSupabaseQuery(connectionId, query);
    const lines = rows.map((row) => `- ${row.schemaname}.${row.tablename}`);
    const output = truncateMessage(`Supabase tables:\n${lines.join('\n')}`, 3500);
    await ctx.reply(output);
  } catch (error) {
    await ctx.reply(`SQL error: ${error.message}`);
  }
}

async function runSupabaseSql(ctx, connectionId, sql) {
  try {
    const result = await runSupabaseQuery(connectionId, sql);
    if (result.rows && result.rows.length) {
      const lines = result.rows.slice(0, 50).map((row) => formatSqlRow(row));
      const output = truncateMessage(lines.join('\n'), 3500);
      await ctx.reply(output);
      return;
    }
    if (result.command === 'SELECT') {
      await ctx.reply('No rows returned.');
      return;
    }
    await ctx.reply(`Query executed. ${result.rowCount || 0} rows affected.`);
  } catch (error) {
    await ctx.reply(`SQL error: ${error.message}`);
  }
}

async function runSupabaseQuery(connectionId, sql) {
  const connection = await findSupabaseConnection(connectionId);
  if (!connection) {
    throw new Error('Supabase connection not found.');
  }
  const dsn = process.env[connection.envKey];
  if (!dsn) {
    throw new Error(`Supabase DSN not configured for ${connection.name}.`);
  }
  let pool = supabasePools.get(connectionId);
  if (!pool) {
    pool = new Pool({ connectionString: dsn });
    supabasePools.set(connectionId, pool);
  }
  return pool.query(sql);
}

function formatSqlRow(row) {
  return Object.entries(row)
    .map(([key, value]) => `${key}: ${value === null ? 'null' : String(value)}`)
    .join(' | ');
}

function truncateMessage(text, limit) {
  if (text.length <= limit) return text;
  return `${text.slice(0, limit)}\n... (truncated)`;
}

async function handleAddProjectStep(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text for this step.');
    return;
  }

  const data = state.data;
  if (state.step === 1) {
    data.id = text;
    setUserState(ctx.from.id, { type: 'add_project', step: 2, data });
    await ctx.reply('Send the GitHub owner (e.g. Mirax226)');
    return;
  }

  if (state.step === 2) {
    data.owner = text;
    setUserState(ctx.from.id, { type: 'add_project', step: 3, data });
    await ctx.reply('Send the GitHub repo name (e.g. daily-system-bot-v2)');
    return;
  }

  if (state.step === 3) {
    data.repo = text;
    setUserState(ctx.from.id, { type: 'add_project', step: 4, data });
    await ctx.reply('Send base branch (or leave empty to use default)');
    return;
  }

  if (state.step === 4) {
    const globalSettings = await loadGlobalSettings();
    data.baseBranch = text || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;

    const projects = await loadProjects();
    if (findProjectById(projects, data.id)) {
      await ctx.reply('A project with this ID already exists.');
      clearUserState(ctx.from.id);
      return;
    }

    const project = {
      id: data.id,
      name: data.id,
      owner: data.owner,
      repo: data.repo,
      repoUrl: `https://github.com/${data.owner}/${data.repo}`,
      baseBranch: data.baseBranch,
      workingDir: getRepoPath({ owner: data.owner, repo: data.repo }),
    };

    projects.push(project);
    await saveProjects(projects);
    clearUserState(ctx.from.id);

    await ctx.reply(
      `Saved project:\nID: ${project.id}\nOwner: ${project.owner}\nRepo: ${project.repo}\nBase: ${project.baseBranch}`,
      { reply_markup: projectsKeyboard },
    );
  }
}

async function handlePatchApplication(ctx, state) {
  const projectId = state.projectId;
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  const startTime = Date.now();
  const globalSettings = await loadGlobalSettings();

  try {
    const patchText = await extractPatchText(ctx);
    if (!patchText) {
      await ctx.reply('No patch text found.');
      return;
    }

    const effectiveBaseBranch = project.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;

    await ctx.reply('Updating repository…');
    const { git, repoDir } = await prepareRepository(project, effectiveBaseBranch);
    // Build a safe branch name from project.id + timestamp
    const timestamp = formatTimestamp(new Date());
    const safeProjectId = String(project.id)
      .normalize('NFKD')
      .replace(/[^\w\-/.]+/g, '-') // فقط حروف، عدد، _, -, /, . را نگه می‌داریم
      .replace(/-+/g, '-')         // چند - پشت سر هم → یک -
      .replace(/\/+/g, '/')        // چند / پشت سر هم → یک /
      .replace(/^-+|-+$/g, '')     // حذف - از ابتدا/انتها
      .toLowerCase()
      .slice(0, 50);               // خیلی بلند نشود

    const branchName = `patch/${safeProjectId}/${timestamp}`;

    await ctx.reply('Creating branch…');
    await createWorkingBranch(git, effectiveBaseBranch, branchName);



    await ctx.reply('Creating branch…');
    await createWorkingBranch(git, effectiveBaseBranch, branchName);

    await ctx.reply('Applying patch…');
    await applyPatchToRepo(git, repoDir, patchText);

    await ctx.reply('Committing and pushing…');
    const hasChanges = await commitAndPush(git, branchName);
    if (!hasChanges) {
      await ctx.reply('Patch applied but no changes detected.');
      return;
    }

    await ctx.reply('Creating Pull Request…');
    const prBody = buildPrBody(patchText);
    const pr = await createPullRequest({
      owner: project.owner,
      repo: project.repo,
      baseBranch: effectiveBaseBranch || DEFAULT_BASE_BRANCH,
      headBranch: branchName,
      title: `Automated patch: ${project.id}`,
      body: prBody,
    });

    const elapsed = Math.round((Date.now() - startTime) / 1000);
    const inline = new InlineKeyboard().url('View PR', pr.html_url);
    await ctx.reply(`Patch applied successfully.\nElapsed: ~${elapsed}s`, { reply_markup: inline });
  } catch (error) {
    console.error('Failed to apply patch', error);
    await ctx.reply(`Failed to apply patch: ${error.message}`);
  } finally {
    clearUserState(ctx.from.id);
  }
}

async function handleRenameProjectStep(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }

  if (state.step === 1) {
    setUserState(ctx.from.id, {
      type: 'rename_project',
      step: 2,
      projectId: state.projectId,
      data: { newName: text },
    });
    await ctx.reply('Send new ID (or leave empty to keep current).');
    return;
  }

  if (state.step === 2) {
    const newId = text || state.projectId;
    const projects = await loadProjects();
    const idx = projects.findIndex((p) => p.id === state.projectId);
    if (idx === -1) {
      await ctx.reply('Project not found.');
      clearUserState(ctx.from.id);
      return;
    }

    if (newId !== state.projectId && projects.find((p) => p.id === newId)) {
      await ctx.reply('A project with this ID already exists.');
      return;
    }

    const updatedProject = { ...projects[idx], name: state.data.newName, id: newId };
    projects[idx] = updatedProject;
    await saveProjects(projects);

    const settings = await loadGlobalSettings();
    if (settings.defaultProjectId === state.projectId) {
      settings.defaultProjectId = newId;
      await saveGlobalSettings(settings);
    }

    clearUserState(ctx.from.id);
    await ctx.reply('Project updated.');
    await renderProjectSettings(ctx, newId);
  }
}

async function handleChangeBaseBranchStep(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }

  const updated = await updateProjectField(state.projectId, 'baseBranch', text);
  if (!updated) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  clearUserState(ctx.from.id);
  await ctx.reply('Base branch updated.');
  await renderProjectSettings(ctx, state.projectId);
}

async function handleEditCommandInput(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }

  const updated = await updateProjectField(state.projectId, state.field, text);
  if (!updated) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  clearUserState(ctx.from.id);
  await ctx.reply(`${state.field} updated.`);
  await renderProjectSettings(ctx, state.projectId);
}

async function handleEditRenderUrl(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }

  const updated = await updateProjectField(state.projectId, state.field, text);
  if (!updated) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  clearUserState(ctx.from.id);
  await ctx.reply(`${state.field} updated.`);
  await renderProjectSettings(ctx, state.projectId);
}

async function handleEditSupabase(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }

  const updated = await updateProjectField(state.projectId, 'supabaseConnectionId', text);
  if (!updated) {
    await ctx.reply('Project not found.');
    clearUserState(ctx.from.id);
    return;
  }

  clearUserState(ctx.from.id);
  await ctx.reply('supabaseConnectionId updated.');
  await renderProjectSettings(ctx, state.projectId);
}

async function handleGlobalBaseChange(ctx, state) {
  const text = ctx.message.text?.trim();
  if (!text) {
    await ctx.reply('Please send text.');
    return;
  }

  const settings = await loadGlobalSettings();
  settings.defaultBaseBranch = text;
  await saveGlobalSettings(settings);
  clearUserState(ctx.from.id);
  await ctx.reply('Default base branch updated.');
  await renderGlobalSettings(ctx);
}

async function runProjectDiagnostics(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    return;
  }

  const command = project.diagnosticCommand || project.testCommand;
  if (!command) {
    await ctx.reply('No diagnostic/test command configured for this project.');
    return;
  }

  const globalSettings = await loadGlobalSettings();
  const effectiveBaseBranch = project.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;

  try {
    if (project.owner && project.repo) {
      await prepareRepository(project, effectiveBaseBranch);
    }
  } catch (error) {
    console.error('Failed to prepare repository for diagnostics', error);
  }

  const workingDir = project.workingDir || (project.owner && project.repo ? getRepoPath(project) : undefined);
  if (!workingDir) {
    await ctx.reply('No working directory configured for this project.');
    return;
  }

  if (!project.workingDir) {
    await updateProjectField(projectId, 'workingDir', workingDir);
  }

  const result = await runCommandInProject({ ...project, workingDir }, command);
  if (result.exitCode === 0) {
    await ctx.reply(
      `🧪 Diagnostics finished successfully.\nProject: ${project.name || project.id}\nDuration: ${result.durationMs} ms\n\nLast output:\n${result.stdout || '(no output)'}`,
    );
    return;
  }

  const errorExcerpt = result.stderr || result.stdout || '(no output)';
  await ctx.reply(
    `🧪 Diagnostics FAILED (exit code ${result.exitCode}).\nProject: ${project.name || project.id}\nDuration: ${result.durationMs} ms\n\nError excerpt:\n${errorExcerpt}`,
  );
}

async function pingRenderService(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  if (!project.renderServiceUrl) {
    await ctx.reply('No render service URL configured for this project.');
    return;
  }
  const start = Date.now();
  try {
    const response = await requestUrl('GET', project.renderServiceUrl);
    const durationMs = Date.now() - start;
    await ctx.reply(`Ping Render: HTTP ${response.status} in ~${durationMs} ms`);
  } catch (error) {
    await ctx.reply(`Ping Render failed: ${error.message}`);
  }
}

async function showKeepAliveUrl(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  const baseUrl = getPublicBaseUrl();
  const url = `${baseUrl.replace(/\/$/, '')}/keep-alive/${project.id}`;
  await ctx.reply(`Keep-alive URL:\n${url}`);
}

async function triggerRenderDeploy(ctx, projectId) {
  const project = await getProjectById(projectId, ctx);
  if (!project) return;
  if (!project.renderDeployHookUrl) {
    await ctx.reply('No deploy hook URL configured for this project.');
    return;
  }
  const start = Date.now();
  try {
    const response = await requestUrl('POST', project.renderDeployHookUrl);
    const durationMs = Date.now() - start;
    const bodySnippet = response.body ? response.body.slice(0, 200) : '';
    const details = bodySnippet ? `\nBody: ${bodySnippet}` : '';
    await ctx.reply(`Render deploy hook: HTTP ${response.status} in ~${durationMs} ms${details}`);
  } catch (error) {
    await ctx.reply(`Render deploy hook failed: ${error.message}`);
  }
}

async function getProjectById(projectId, ctx) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project && ctx) {
    await ctx.reply('Project not found.');
  }
  return project;
}

async function renderProjectSettings(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    return;
  }
  const globalSettings = await loadGlobalSettings();
  const effectiveBase = project.baseBranch || globalSettings.defaultBaseBranch || DEFAULT_BASE_BRANCH;

  const lines = [
    `Name: ${project.name || '-'}`,
    `ID: ${project.id}`,
    `Repo: ${project.owner ? `${project.owner}/${project.repo}` : project.repoUrl || '-'}`,
    `Repo URL: ${project.repoUrl || '-'}`,
    `Working dir: ${project.workingDir || '-'}`,
    `Base branch: ${effectiveBase}`,
    `startCommand: ${project.startCommand || '-'}`,
    `testCommand: ${project.testCommand || '-'}`,
    `diagnosticCommand: ${project.diagnosticCommand || '-'}`,
    `renderServiceUrl: ${project.renderServiceUrl || '-'}`,
    `renderDeployHookUrl: ${project.renderDeployHookUrl || '-'}`,
    `supabaseConnectionId: ${project.supabaseConnectionId || '-'}`,
  ];

  const inline = new InlineKeyboard()
    .text('✏️ Edit project', `proj:${project.id}:rename`)
    .row()
    .text('🌿 Change base branch', `proj:${project.id}:change_base`)
    .row()
    .text('🧰 Edit commands', `proj:${project.id}:commands`)
    .row()
    .text('🧪 Run diagnostics', `proj:${project.id}:diagnostics`)
    .row()
    .text('🛰️ Edit Render URLs', `proj:${project.id}:render`);

  if (project.renderServiceUrl) {
    inline
      .row()
      .text('📡 Ping Render now', `proj:${project.id}:render_ping`)
      .row()
      .text('🔗 Show keep-alive URL', `proj:${project.id}:render_keepalive_url`);
  }

  if (project.renderDeployHookUrl) {
    inline.row().text('🚀 Deploy (Render)', `proj:${project.id}:render_deploy`);
  }

  inline
    .row()
    .text('🗄️ Edit Supabase binding', `proj:${project.id}:supabase`)
    .row()
    .text('⭐ Set as default project', `proj:${project.id}:set_default`)
    .row()
    .text('⬅️ Back', `proj:${project.id}:back`);

  await ctx.reply(lines.join('\n'), { reply_markup: inline });
}

async function renderDeleteConfirmation(ctx, projectId) {
  const inline = new InlineKeyboard()
    .text('🗑️ Yes, delete', `proj:${projectId}:confirm_delete`)
    .text('⬅️ Cancel', `proj:${projectId}:cancel_delete`);
  await ctx.reply(`Are you sure you want to delete project ${projectId}?`, { reply_markup: inline });
}

async function deleteProject(ctx, projectId) {
  const projects = await loadProjects();
  const filtered = projects.filter((p) => p.id !== projectId);
  if (filtered.length === projects.length) {
    await ctx.reply('Project not found.');
    return;
  }
  await saveProjects(filtered);
  const settings = await loadGlobalSettings();
  if (settings.defaultProjectId === projectId) {
    settings.defaultProjectId = undefined;
    await saveGlobalSettings(settings);
  }
  await ctx.reply(`Project ${projectId} deleted.`);
  await renderProjectsList(ctx);
}

async function renderCommandsScreen(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    return;
  }

  const lines = [
    `startCommand: ${project.startCommand || '-'}`,
    `testCommand: ${project.testCommand || '-'}`,
    `diagnosticCommand: ${project.diagnosticCommand || '-'}`,
  ];

  const inline = new InlineKeyboard()
    .text('✏️ Edit startCommand', `proj:${project.id}:cmd_edit:startCommand`)
    .row()
    .text('✏️ Edit testCommand', `proj:${project.id}:cmd_edit:testCommand`)
    .row()
    .text('✏️ Edit diagnosticCommand', `proj:${project.id}:cmd_edit:diagnosticCommand`)
    .row()
    .text('🧹 Clear startCommand', `proj:${project.id}:cmd_clear:startCommand`)
    .row()
    .text('🧹 Clear testCommand', `proj:${project.id}:cmd_clear:testCommand`)
    .row()
    .text('🧹 Clear diagnosticCommand', `proj:${project.id}:cmd_clear:diagnosticCommand`)
    .row()
    .text('⬅️ Back', `proj:${project.id}:settings`);

  await ctx.reply(lines.join('\n'), { reply_markup: inline });
}

async function renderRenderUrlsScreen(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    return;
  }

  const lines = [
    `renderServiceUrl: ${project.renderServiceUrl || '-'}`,
    `renderDeployHookUrl: ${project.renderDeployHookUrl || '-'}`,
  ];

  const inline = new InlineKeyboard()
    .text('✏️ Edit service URL', `proj:${project.id}:render_edit:renderServiceUrl`)
    .row()
    .text('✏️ Edit deploy hook URL', `proj:${project.id}:render_edit:renderDeployHookUrl`)
    .row()
    .text('🧹 Clear service URL', `proj:${project.id}:render_clear:renderServiceUrl`)
    .row()
    .text('🧹 Clear deploy hook URL', `proj:${project.id}:render_clear:renderDeployHookUrl`)
    .row()
    .text('⬅️ Back', `proj:${project.id}:settings`);

  await ctx.reply(lines.join('\n'), { reply_markup: inline });
}

async function renderSupabaseScreen(ctx, projectId) {
  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    return;
  }

  const lines = [`supabaseConnectionId: ${project.supabaseConnectionId || '-'}`];
  const inline = new InlineKeyboard()
    .text('✏️ Edit', `proj:${project.id}:supabase_edit`)
    .text('🧹 Clear', `proj:${project.id}:supabase_clear`)
    .row()
    .text('⬅️ Back', `proj:${project.id}:settings`);

  await ctx.reply(lines.join('\n'), { reply_markup: inline });
}

async function renderGlobalSettings(ctx) {
  const settings = await loadGlobalSettings();
  const projects = await loadProjects();
  const defaultProject = settings.defaultProjectId
    ? findProjectById(projects, settings.defaultProjectId)
    : undefined;
  const lines = [
    `defaultBaseBranch: ${settings.defaultBaseBranch || DEFAULT_BASE_BRANCH}`,
    `defaultProjectId: ${settings.defaultProjectId || '-'}` +
      (defaultProject ? ` (${defaultProject.name || defaultProject.id})` : ''),
  ];

  const inline = new InlineKeyboard()
    .text('✏️ Change default base branch', 'gsettings:change_default_base')
    .row()
    .text('🧹 Clear default base branch', 'gsettings:clear_default_base')
    .row()
    .text('🧹 Clear default project', 'gsettings:clear_default_project')
    .row()
    .text('⬅️ Back', 'gsettings:back');

  await ctx.reply(lines.join('\n'), { reply_markup: inline });
}

async function setDefaultProject(projectId) {
  const settings = await loadGlobalSettings();
  settings.defaultProjectId = projectId;
  await saveGlobalSettings(settings);
}

async function clearDefaultProject() {
  const settings = await loadGlobalSettings();
  settings.defaultProjectId = undefined;
  await saveGlobalSettings(settings);
}

async function clearDefaultBaseBranch() {
  const settings = await loadGlobalSettings();
  delete settings.defaultBaseBranch;
  await saveGlobalSettings(settings);
}

async function updateProjectField(projectId, field, value) {
  const projects = await loadProjects();
  const idx = projects.findIndex((p) => p.id === projectId);
  if (idx === -1) {
    return false;
  }
  projects[idx] = { ...projects[idx], [field]: value };
  await saveProjects(projects);
  return true;
}

function requestUrl(method, targetUrl, body) {
  return new Promise((resolve, reject) => {
    const url = new URL(targetUrl);
    const isHttps = url.protocol === 'https:';
    const lib = isHttps ? https : http;
    const options = {
      method,
      hostname: url.hostname,
      port: url.port || (isHttps ? 443 : 80),
      path: `${url.pathname}${url.search}`,
      headers: body
        ? {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(body),
          }
        : undefined,
    };

    const start = Date.now();
    const req = lib.request(options, (res) => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        resolve({ status: res.statusCode, body: data, durationMs: Date.now() - start });
      });
    });

    req.on('error', reject);
    req.setTimeout(15000, () => {
      req.destroy(new Error('Request timed out'));
    });

    if (body) {
      req.write(body);
    }
    req.end();
  });
}

function getPublicBaseUrl() {
  return (
    process.env.PUBLIC_BASE_URL ||
    process.env.RENDER_EXTERNAL_URL ||
    `http://localhost:${port}`
  );
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk) => {
      data += chunk;
      if (data.length > 5 * 1024 * 1024) {
        req.destroy();
      }
    });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

function parseRenderErrorPayload(body) {
  if (!body) {
    return { message: '', level: '' };
  }
  try {
    const parsed = JSON.parse(body);
    return {
      message: parsed.message || parsed.error || JSON.stringify(parsed).slice(0, 1000),
      level: parsed.level || parsed.severity || '',
    };
  } catch (error) {
    return {
      message: body.slice(0, 1000),
      level: '',
    };
  }
}

function formatTimestamp(date) {
  const pad = (n) => String(n).padStart(2, '0');
  const yyyy = date.getFullYear();
  const mm = pad(date.getMonth() + 1);
  const dd = pad(date.getDate());
  const hh = pad(date.getHours());
  const min = pad(date.getMinutes());
  const ss = pad(date.getSeconds());
  return `${yyyy}${mm}${dd}-${hh}${min}${ss}`;
}

async function extractPatchText(ctx) {
  if (ctx.message.document) {
    const doc = ctx.message.document;
    const fileName = doc.file_name || '';
    if (!fileName.endsWith('.patch') && !fileName.endsWith('.diff')) {
      await ctx.reply('Unsupported file type. Please send a .patch or .diff file.');
      return null;
    }
    return downloadTelegramFile(ctx, doc.file_id);
  }

  const text = ctx.message.text;
  if (!text) {
    return null;
  }
  return text;
}

function downloadTelegramFile(ctx, fileId) {
  return ctx.api.getFile(fileId).then((file) => {
    const fileUrl = `https://api.telegram.org/file/bot${BOT_TOKEN}/${file.file_path}`;
    return new Promise((resolve, reject) => {
      https
        .get(fileUrl, (res) => {
          if (res.statusCode !== 200) {
            reject(new Error(`Failed to download file: ${res.statusCode}`));
            return;
          }
          let data = '';
          res.setEncoding('utf8');
          res.on('data', (chunk) => {
            data += chunk;
          });
          res.on('end', () => resolve(data));
        })
        .on('error', reject);
    });
  });
}

function buildPrBody(patchText) {
  const preview = patchText.split('\n').slice(0, 20).join('\n');
  return `Automated patch at ${new Date().toISOString()}\n\nPreview:\n\n${preview}`;
}

bot.catch((err) => {
  console.error('Bot error:', err);
});

const port = process.env.PORT || 3000;

async function initializeConfig() {
  try {
    await loadProjects();
    await loadGlobalSettings();
  } catch (error) {
    console.error('Failed to load initial configuration', error);
  }
}

function startHttpServer() {
  return new Promise((resolve) => {
    http
      .createServer(async (req, res) => {
        const url = new URL(req.url, `http://${req.headers.host}`);
        if (req.method === 'GET' && url.pathname.startsWith('/keep-alive/')) {
          const projectId = decodeURIComponent(url.pathname.split('/')[2] || '');
          const project = projectId ? await getProjectById(projectId) : null;
          if (!project) {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: false, error: 'Project not found.' }));
            return;
          }
          if (!project.renderServiceUrl) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: false, error: 'renderServiceUrl not configured.' }));
            return;
          }
          const start = Date.now();
          try {
            const response = await requestUrl('GET', project.renderServiceUrl);
            const durationMs = Date.now() - start;
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: true, status: response.status, durationMs }));
          } catch (error) {
            const durationMs = Date.now() - start;
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: false, status: null, durationMs, error: error.message }));
            await bot.api.sendMessage(
              ADMIN_TELEGRAM_ID,
              `Render keep-alive FAILED for project ${project.name || project.id}.`,
            );
          }
          return;
        }

        if (req.method === 'POST' && url.pathname.startsWith('/render-error-hook/')) {
          const projectId = decodeURIComponent(url.pathname.split('/')[2] || '');
          const body = await readRequestBody(req);
          const { message, level } = parseRenderErrorPayload(body);
          const timestamp = new Date().toISOString();
          const text = `⚠️ Render error for project ${projectId} at ${timestamp}.\nLevel: ${
            level || '-'
          }\nMessage: ${message || '-'}`;
          await bot.api.sendMessage(ADMIN_TELEGRAM_ID, text);
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ ok: true }));
          return;
        }

        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('Path Applier is running.\n');
      })
      .listen(port, () => {
        console.log(`HTTP health server listening on port ${port}`);
        resolve();
      });
  });
}

async function startBotPolling() {
  if (botStarted) {
    console.log('[Path Applier] bot.start() already called, skipping.');
    return;
  }
  botStarted = true;
  if (botRetryTimeout) {
    clearTimeout(botRetryTimeout);
    botRetryTimeout = null;
  }

  try {
    await bot.start();
    console.log('[Path Applier] Bot polling started.');
  } catch (error) {
    botStarted = false;
    if (
      error?.error_code === 409 &&
      typeof error.description === 'string' &&
      error.description.includes('terminated by other getUpdates request')
    ) {
      console.error(
        '[Path Applier] Telegram returned 409 (another getUpdates in progress). Will retry in 15 seconds.',
      );
      if (!botRetryTimeout) {
        botRetryTimeout = setTimeout(() => {
          botRetryTimeout = null;
          startBotPolling().catch((retryError) => {
            console.error(
              '[Path Applier] Retry failed:',
              retryError?.stack || retryError,
            );
          });
        }, 15000);
      }
      return;
    }
    console.error('[Path Applier] Failed to start bot polling:', error?.stack || error);
    throw error;
  }
}

async function startBot() {
  await startHttpServer();
  await testConfigDbConnection();
  await initializeConfig();
  try {
    await bot.api.deleteWebhook({ drop_pending_updates: false });
    console.log('[Path Applier] Webhook deleted (if any). Using long polling.');
  } catch (error) {
    console.error('[Path Applier] Failed to delete webhook:', error?.stack || error);
  }
  await startBotPolling();
}

startBot().catch((error) => {
  console.error('Failed to start bot', error?.stack || error);
});

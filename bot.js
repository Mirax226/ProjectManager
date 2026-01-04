require('dotenv').config();

const https = require('https');
const { Bot, InlineKeyboard, Keyboard } = require('grammy');

const { loadProjects, saveProjects, findProjectById } = require('./projectsStore');
const { setUserState, getUserState, clearUserState } = require('./state');
const {
  prepareRepository,
  createWorkingBranch,
  applyPatchToRepo,
  commitAndPush,
  DEFAULT_BASE_BRANCH,
  fetchDryRun,
} = require('./gitUtils');
const { createPullRequest, measureGithubLatency } = require('./githubUtils');
const { loadGlobalSettings, saveGlobalSettings } = require('./settingsStore');

const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_TELEGRAM_ID = process.env.ADMIN_TELEGRAM_ID;

if (!BOT_TOKEN) {
  throw new Error('BOT_TOKEN is required');
}

if (!ADMIN_TELEGRAM_ID) {
  throw new Error('ADMIN_TELEGRAM_ID is required');
}

const bot = new Bot(BOT_TOKEN);

const mainKeyboard = new Keyboard()
  .text('Projects')
  .row()
  .text('Ping test')
  .row()
  .text('Help')
  .resized();

const projectsKeyboard = new Keyboard()
  .text('List projects')
  .row()
  .text('Add project')
  .row()
  .text('Global settings')
  .row()
  .text('Back')
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

bot.hears('Help', async (ctx) => {
  await ctx.reply(
    'Use this bot to manage projects and apply git patches that will be turned into GitHub PRs.',
    { reply_markup: mainKeyboard },
  );
});

bot.hears('Projects', async (ctx) => {
  await ctx.reply('Project actions:', { reply_markup: projectsKeyboard });
});

bot.hears('Back', async (ctx) => {
  await ctx.reply('Main menu:', { reply_markup: mainKeyboard });
});

bot.hears('Add project', async (ctx) => {
  setUserState(ctx.from.id, { type: 'add_project', step: 1, data: {} });
  await ctx.reply('Send a short ID for the project (e.g. daily-system)');
});

bot.hears('List projects', async (ctx) => {
  await renderProjectsList(ctx);
});

bot.hears('Global settings', async (ctx) => {
  await renderGlobalSettings(ctx);
});

bot.hears('Ping test', async (ctx) => {
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
      .text('Apply patch', `proj:${project.id}:apply_patch`)
      .text('Settings', `proj:${project.id}:settings`)
      .text('Delete', `proj:${project.id}:delete`)
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
    case 'render':
      await renderRenderUrlsScreen(ctx, projectId);
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
      await ctx.reply('Project actions:', { reply_markup: projectsKeyboard });
      break;
    default:
      break;
  }
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
      baseBranch: data.baseBranch,
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

    const branchName = `patch/${project.id}/${formatTimestamp(new Date())}`;
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
    `Repo: ${project.owner}/${project.repo}`,
    `Base branch: ${effectiveBase}`,
    `startCommand: ${project.startCommand || '-'}`,
    `testCommand: ${project.testCommand || '-'}`,
    `diagnosticCommand: ${project.diagnosticCommand || '-'}`,
    `renderServiceUrl: ${project.renderServiceUrl || '-'}`,
    `renderDeployHookUrl: ${project.renderDeployHookUrl || '-'}`,
    `supabaseConnectionId: ${project.supabaseConnectionId || '-'}`,
  ];

  const inline = new InlineKeyboard()
    .text('Rename project', `proj:${project.id}:rename`)
    .row()
    .text('Change base branch', `proj:${project.id}:change_base`)
    .row()
    .text('Edit commands', `proj:${project.id}:commands`)
    .row()
    .text('Edit Render URLs', `proj:${project.id}:render`)
    .row()
    .text('Edit Supabase binding', `proj:${project.id}:supabase`)
    .row()
    .text('Set as default project', `proj:${project.id}:set_default`)
    .row()
    .text('Back', `proj:${project.id}:back`);

  await ctx.reply(lines.join('\n'), { reply_markup: inline });
}

async function renderDeleteConfirmation(ctx, projectId) {
  const inline = new InlineKeyboard()
    .text('Yes, delete', `proj:${projectId}:confirm_delete`)
    .text('Cancel', `proj:${projectId}:cancel_delete`);
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
    .text('Edit startCommand', `proj:${project.id}:cmd_edit:startCommand`)
    .row()
    .text('Edit testCommand', `proj:${project.id}:cmd_edit:testCommand`)
    .row()
    .text('Edit diagnosticCommand', `proj:${project.id}:cmd_edit:diagnosticCommand`)
    .row()
    .text('Clear startCommand', `proj:${project.id}:cmd_clear:startCommand`)
    .row()
    .text('Clear testCommand', `proj:${project.id}:cmd_clear:testCommand`)
    .row()
    .text('Clear diagnosticCommand', `proj:${project.id}:cmd_clear:diagnosticCommand`)
    .row()
    .text('Back', `proj:${project.id}:settings`);

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
    .text('Edit service URL', `proj:${project.id}:render_edit:renderServiceUrl`)
    .row()
    .text('Edit deploy hook URL', `proj:${project.id}:render_edit:renderDeployHookUrl`)
    .row()
    .text('Clear service URL', `proj:${project.id}:render_clear:renderServiceUrl`)
    .row()
    .text('Clear deploy hook URL', `proj:${project.id}:render_clear:renderDeployHookUrl`)
    .row()
    .text('Back', `proj:${project.id}:settings`);

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
    .text('Edit', `proj:${project.id}:supabase_edit`)
    .text('Clear', `proj:${project.id}:supabase_clear`)
    .row()
    .text('Back', `proj:${project.id}:settings`);

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
    .text('Change default base branch', 'gsettings:change_default_base')
    .row()
    .text('Clear default base branch', 'gsettings:clear_default_base')
    .row()
    .text('Clear default project', 'gsettings:clear_default_project')
    .row()
    .text('Back', 'gsettings:back');

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

bot.start();

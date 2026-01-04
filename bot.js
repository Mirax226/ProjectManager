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
  if (state?.type === 'add_project') {
    await handleAddProjectStep(ctx, state);
    return;
  }

  if (state?.type === 'await_patch') {
    await handlePatchApplication(ctx, state);
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
  const projects = await loadProjects();
  if (!projects.length) {
    await ctx.reply('No projects configured yet.', { reply_markup: projectsKeyboard });
    return;
  }

  const lines = projects
    .map((p) => `ID: ${p.id}\nOwner: ${p.owner}\nRepo: ${p.repo}\nBase: ${p.baseBranch || DEFAULT_BASE_BRANCH}`)
    .join('\n\n');

  const inline = new InlineKeyboard();
  projects.forEach((project) => {
    inline.text(`Apply patch (${project.id})`, `proj:${project.id}:apply_patch`).row();
  });

  await ctx.reply(lines, { reply_markup: inline });
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
    if (projects.length) {
      const start = Date.now();
      await fetchDryRun(projects[0]);
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
  if (!data.startsWith('proj:')) return;
  await ctx.answerCallbackQuery();
  const [, projectId, action] = data.split(':');
  if (action !== 'apply_patch') return;

  const projects = await loadProjects();
  const project = findProjectById(projects, projectId);
  if (!project) {
    await ctx.reply('Project not found.');
    return;
  }

  setUserState(ctx.from.id, { type: 'await_patch', project });
  await ctx.reply('Send the git patch as text or as a .patch/.diff file.');
});

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
    await ctx.reply('Send base branch (or leave empty to use DEFAULT_BASE_BRANCH)');
    return;
  }

  if (state.step === 4) {
    data.baseBranch = text || DEFAULT_BASE_BRANCH;

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
  const project = state.project;
  const startTime = Date.now();

  try {
    const patchText = await extractPatchText(ctx);
    if (!patchText) {
      await ctx.reply('No patch text found.');
      return;
    }

    await ctx.reply('Updating repository…');
    const { git, repoDir, baseBranch } = await prepareRepository(project);

    const branchName = `patch/${project.id}/${formatTimestamp(new Date())}`;
    await ctx.reply('Creating branch…');
    await createWorkingBranch(git, baseBranch, branchName);

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
      baseBranch: baseBranch || DEFAULT_BASE_BRANCH,
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

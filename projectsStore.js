const fs = require('fs/promises');
const path = require('path');
const configStore = require('./configStore');

const PROJECTS_FILE = path.join(__dirname, 'projects.json');
const DEFAULT_WORKDIR = process.env.WORKDIR || '/tmp/patch-runner-bot';

function deriveRepoSlug(project) {
  if (project?.repoSlug) return project.repoSlug;
  if (project?.repoOwner && project?.repoName) {
    return `${project.repoOwner}/${project.repoName}`;
  }
  if (project?.owner && project?.repo) {
    return `${project.owner}/${project.repo}`;
  }
  if (typeof project?.repo === 'string' && project.repo.includes('/')) {
    return project.repo;
  }
  return undefined;
}

function getDefaultWorkingDir(repoSlug) {
  if (!repoSlug || !repoSlug.includes('/')) return undefined;
  const [owner, repo] = repoSlug.split('/');
  if (!owner || !repo) return undefined;
  return path.join(DEFAULT_WORKDIR, `${owner}__${repo}`);
}

function normalizeProject(rawProject) {
  const project = { ...(rawProject || {}) };
  const repoSlug = deriveRepoSlug(project);
  if (repoSlug) {
    project.repoSlug = repoSlug;
    const [owner, repo] = repoSlug.split('/');
    project.owner = project.owner || owner;
    project.repo = project.repo || repo;
  }

  if (!project.repoUrl && project.repoSlug) {
    project.repoUrl = `https://github.com/${project.repoSlug}`;
  }

  if (!project.workingDir && project.repoSlug) {
    project.workingDir = '.';
  }

  if (typeof project.isWorkingDirCustom !== 'boolean') {
    const defaultDir = project.repoSlug ? getDefaultWorkingDir(project.repoSlug) : undefined;
    const workingDir = project.workingDir;
    const isRelativeDefault = workingDir === '.' || workingDir === './' || workingDir === '';
    const isAbsoluteDefault = Boolean(defaultDir && workingDir === defaultDir);
    const isCustom =
      Boolean(workingDir) && !isRelativeDefault && !isAbsoluteDefault;
    project.isWorkingDirCustom = isCustom;
  }

  project.githubTokenEnvKey = project.githubTokenEnvKey || 'GITHUB_TOKEN';
  project.projectType = project.projectType || project.project_type || 'other';
  if (!project.project_type) {
    project.project_type = project.projectType;
  }
  project.defaultEnvSetId = project.defaultEnvSetId || project.default_env_set_id;
  if (project.defaultEnvSetId && !project.default_env_set_id) {
    project.default_env_set_id = project.defaultEnvSetId;
  }

  return project;
}

async function ensureProjectsFile() {
  try {
    await fs.access(PROJECTS_FILE);
  } catch (err) {
    await fs.writeFile(PROJECTS_FILE, '[]', 'utf-8');
  }
}

async function loadProjects() {
  const dbProjects = await configStore.loadProjects();
  if (dbProjects.length) {
    return dbProjects.map((project) => normalizeProject(project));
  }

  await ensureProjectsFile();
  try {
    const content = await fs.readFile(PROJECTS_FILE, 'utf-8');
    const parsed = JSON.parse(content);
    if (parsed.length && !dbProjects.length) {
      await configStore.saveProjects(parsed.map((project) => normalizeProject(project)));
    }
    return parsed.map((project) => normalizeProject(project));
  } catch (error) {
    console.error('Failed to load projects.json', error);
    return [];
  }
}

async function saveProjects(projects) {
  try {
    const normalized = projects.map((project) => normalizeProject(project));
    await configStore.saveProjects(normalized);
    await fs.writeFile(PROJECTS_FILE, JSON.stringify(normalized, null, 2), 'utf-8');
  } catch (error) {
    console.error('Failed to save projects.json', error);
    throw error;
  }
}

function findProjectById(projects, id) {
  return projects.find((p) => p.id === id);
}

module.exports = {
  loadProjects,
  saveProjects,
  findProjectById,
  normalizeProject,
  getDefaultWorkingDir,
  PROJECTS_FILE,
};

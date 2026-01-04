const fs = require('fs/promises');
const path = require('path');
const configStore = require('./configStore');

const PROJECTS_FILE = path.join(__dirname, 'projects.json');

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
    return dbProjects;
  }

  await ensureProjectsFile();
  try {
    const content = await fs.readFile(PROJECTS_FILE, 'utf-8');
    const parsed = JSON.parse(content);
    if (parsed.length && !dbProjects.length) {
      await configStore.saveProjects(parsed);
    }
    return parsed;
  } catch (error) {
    console.error('Failed to load projects.json', error);
    return [];
  }
}

async function saveProjects(projects) {
  try {
    await configStore.saveProjects(projects);
    await fs.writeFile(PROJECTS_FILE, JSON.stringify(projects, null, 2), 'utf-8');
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
  PROJECTS_FILE,
};

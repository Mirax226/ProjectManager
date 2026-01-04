const fs = require('fs/promises');
const path = require('path');

const PROJECTS_FILE = path.join(__dirname, 'projects.json');

async function ensureProjectsFile() {
  try {
    await fs.access(PROJECTS_FILE);
  } catch (err) {
    await fs.writeFile(PROJECTS_FILE, '[]', 'utf-8');
  }
}

async function loadProjects() {
  await ensureProjectsFile();
  try {
    const content = await fs.readFile(PROJECTS_FILE, 'utf-8');
    return JSON.parse(content);
  } catch (error) {
    console.error('Failed to load projects.json', error);
    return [];
  }
}

async function saveProjects(projects) {
  try {
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

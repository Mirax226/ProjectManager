const { exec } = require('child_process');
const { promisify } = require('util');

const execAsync = promisify(exec);

function trimOutput(text, limit) {
  if (!text) return '';
  const trimmed = text.trim();
  if (trimmed.length <= limit) return trimmed;
  return trimmed.slice(-limit);
}

async function runCommandInProject(project, command) {
  const start = Date.now();
  try {
    const { stdout, stderr } = await execAsync(command, {
      cwd: project.workingDir,
      maxBuffer: 10 * 1024 * 1024,
    });
    return {
      exitCode: 0,
      stdout: trimOutput(stdout, 2000),
      stderr: trimOutput(stderr, 2000),
      durationMs: Date.now() - start,
    };
  } catch (error) {
    return {
      exitCode: typeof error.code === 'number' ? error.code : 1,
      stdout: trimOutput(error.stdout, 2000),
      stderr: trimOutput(error.stderr || error.message, 2000),
      durationMs: Date.now() - start,
    };
  }
}

module.exports = {
  runCommandInProject,
};

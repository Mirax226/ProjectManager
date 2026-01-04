# Patch Runner Bot

Telegram bot that applies git patches to GitHub repositories and opens pull requests automatically.

## Requirements
- Node.js 18+
- Environment variables (can be stored in a `.env` file):
  - `BOT_TOKEN` – Telegram bot token
  - `ADMIN_TELEGRAM_ID` – only this user can interact with the bot
  - `GITHUB_TOKEN` – GitHub personal access token with `repo` scope
  - `WORKDIR` – directory where repositories are cloned (default `/tmp/patch-runner-bot`)
  - `DEFAULT_BASE_BRANCH` – default branch name (default `main`)
  - `PORT` – optional, for webhook usage

## Install
```
npm install
```

## Run
```
node bot.js
```

## Usage
1. Start the bot with `/start` from the admin Telegram account.
2. Use **Projects** → **Add project** to register repositories (owner/repo/base branch).
3. Manage settings per project from **Projects** → **List projects** → **Settings**:
   - Rename project or change base branch.
   - Add/edit/clear start, test, and diagnostic commands.
   - Add/edit/clear Render service and deploy hook URLs.
   - Add/edit/clear Supabase binding.
   - Mark a project as the default project.
4. Use **Projects** → **Global settings** to override default base branch or clear defaults.
5. Choose **Projects** → **List projects** and tap **Apply patch** for a project, then send a git patch as text or a `.patch`/`.diff` file.
6. The bot updates the repo, creates a branch, applies the patch, pushes it, and opens a PR. A link to the PR is sent back.
7. Use **Ping test** to check GitHub API and git fetch latency.

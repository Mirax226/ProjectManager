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
2. Use **Projects** → **Add project** to register repositories.
3. Choose **Projects** → **List projects** and tap **Apply patch** for a project.
4. Send a git patch as text or a `.patch`/`.diff` file.
5. The bot updates the repo, creates a branch, applies the patch, pushes it, and opens a PR. A link to the PR is sent back.
6. Use **Ping test** to check GitHub API and git fetch latency.

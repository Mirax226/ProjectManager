class ProgressReporter {
  constructor({ bot, chatId, initialText = '⏳ Working…', totalSteps = 1 }) {
    this.bot = bot;
    this.chatId = chatId;
    this.totalSteps = Math.max(1, Number(totalSteps) || 1);
    this.initialText = initialText;
    this.messageId = null;
    this.currentStep = 0;
  }

  async start() {
    if (!this.bot || !this.chatId) return;
    const sent = await this.bot.api.sendMessage(this.chatId, this.initialText, { disable_web_page_preview: true });
    this.messageId = sent?.message_id || null;
  }

  async step(title) {
    this.currentStep = Math.min(this.totalSteps, this.currentStep + 1);
    const percent = Math.round((this.currentStep / this.totalSteps) * 100);
    await this.update(`${percent}% — ${title}`);
  }

  async update(text) {
    if (!this.bot || !this.chatId || !this.messageId) return;
    await this.bot.api.editMessageText(this.chatId, this.messageId, `⏳ Ping test in progress\n${text}`, {
      disable_web_page_preview: true,
    }).catch(() => null);
  }

  async done() {
    if (!this.bot || !this.chatId || !this.messageId) return;
    await this.bot.api.deleteMessage(this.chatId, this.messageId).catch(() => null);
  }
}

module.exports = { ProgressReporter };

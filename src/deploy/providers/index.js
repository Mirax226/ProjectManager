const renderProvider = require('./renderProvider');

function getProvider(provider) {
  if (provider === 'render' || !provider) return renderProvider;
  return renderProvider;
}

module.exports = { getProvider };

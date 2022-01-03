const { WebhookClient } = require('discord.js')

const sendMessage = (alertType, publicKey, oldData, newData) => {
  // Log error message in the console
  console.error(alertType, publicKey, oldData, newData)

  // Prepare the text sent to Discord
  const text = `**${alertType}**\nOld: ${oldData} - New: ${newData}\n\`${publicKey}\``

  const webhookClient = new WebhookClient({ url: process.env.DISCORD_WEBHOOK_URL })
  webhookClient.send({
    username: 'Validator Monitoring Bot',
    content: text
  })
}

module.exports = {
  sendMessage
}

// sendMessage('BALANCE-NOT-INCREASING', '8910a8fa90a09c4496507fe0052167b37505015328a0f33f66da248bed6d2ea2357ec0049bcbc8356078914d317cebf7', '1', '0')

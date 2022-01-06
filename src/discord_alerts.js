const { WebhookClient } = require('discord.js')

const sendMessage = (alertType, publicKey, oldData, newData) => {
  // Log error message in the console
  console.error(alertType, publicKey, oldData, newData)

  // Create the Beaconchain direct link to the validator
  const network = process.argv[2]
  console.log(network)
  const beaconchainUrl = process.env['BEACONCHAIN_ENDPOINT_' + network.toUpperCase()] + '/validator/' + publicKey + '#attestations'

  // Prepare the text sent to Discord
  const text = `**${alertType}**\nNetwork: ${network}\n${oldData} 🡺 ${newData}\n[${publicKey}](<${beaconchainUrl}>)`

  const webhookClient = new WebhookClient({ url: process.env.DISCORD_WEBHOOK_URL })
  webhookClient.send({
    username: 'Validator Monitoring Bot',
    content: text
  })
}

module.exports = {
  sendMessage
}

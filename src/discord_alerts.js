const { WebhookClient } = require('discord.js')

const sendValidatorMessage = async (alertType, serverIdentifier, publicKey, oldData, newData) => {
  // Log error message in the console
  console.log(alertType, serverIdentifier, publicKey, oldData, newData)

  // Create the Beaconchain direct link to the validator
  const network = process.argv[2]
  const beaconchainUrl = process.env['BEACONCHAIN_ENDPOINT_' + network.toUpperCase()] + '/validator/' + publicKey + '#attestations'

  // Prepare the text sent to Discord
  const text = `**${alertType}**\nServer identifier: ${serverIdentifier}\nNetwork: ${network}\n${oldData} 🡺 ${newData}\n[${publicKey}](<${beaconchainUrl}>)`

  const webhookClient = new WebhookClient({ url: getDiscordWebhook(network) })
  webhookClient.send({
    username: 'Validator Monitoring Bot',
    content: text
  })

  // Sleep 1 second to avoid rate limitting
  await new Promise(resolve => setTimeout(resolve, 1000))
}

const sendMessage = async (alertType, message) => {
  // Log error message in the console
  console.log(alertType, message)

  const network = process.argv[2]

  // Prepare the text sent to Discord
  const text = `**${alertType}**\nNetwork: ${network}\nMessage:\n\n${message}`

  const webhookClient = new WebhookClient({ url: getDiscordWebhook(network) })
  webhookClient.send({
    username: 'Validator Monitoring Bot',
    content: text
  })

  // Sleep 1 second to avoid rate limitting
  await new Promise(resolve => setTimeout(resolve, 1000))
}

// Gets the Discord webhook from the .env file
const getDiscordWebhook = (network) => {
  return process.env['DISCORD_WEBHOOK_URL_' + network.toUpperCase()]
}

module.exports = {
  sendValidatorMessage,
  sendMessage
}

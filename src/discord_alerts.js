const { WebhookClient } = require('discord.js')

const sendValidatorMessage = async (alertType, serverHostname, publicKey, oldData, newData) => {
  // Log error message in the console
  console.log(alertType, serverHostname, publicKey, oldData, newData)

  // Create the Beaconchain direct link to the validator
  const network = process.argv[2]
  const beaconchainUrl = process.env['BEACONCHAIN_ENDPOINT_' + network.toUpperCase()] + '/validator/' + publicKey + '#attestations'

  // Prepare the text sent to Discord
  const title = `**${alertType}**`
  let description
  if (newData) {
    description = `**Server hostname:** ${serverHostname}\n**Network:** ${network}\n${oldData} 🡺 ${newData}\n[${publicKey}](<${beaconchainUrl}>)`
  } else {
    description = `**Server hostname:** ${serverHostname}\n**Network:** ${network}\n${oldData}\n[${publicKey}](<${beaconchainUrl}>)`
  }

  // Select a color depending on the importance
  let color
  if (alertType === 'ATTESTATIONS-MISSED') {
    color = 'ff9966' // Orange
  } else if (alertType === 'SLASH-CHANGE') {
    color = 'cc3300' // Red
  } else if (alertType === 'STATUS-CHANGE') {
    if (newData === 'active_online') {
      color = '99cc33' // Green
    } else if (newData === 'active_offline') {
      color = 'ffcc00' // Yellow
    }
  }
  // Default color as a backup (it shouldnt appear)
  if (!color) {
    color = '636264' // Gray
  }

  const webhookClient = new WebhookClient({ url: getDiscordWebhook(network) })
  webhookClient.send({
    username: 'Validator Monitoring Bot',
    embeds: [
      {
        title: title,
        description: description,
        color: color
      }
    ]
  })

  // Sleep 1 second to avoid rate limitting
  await new Promise(resolve => setTimeout(resolve, 2000))
}

const sendMessage = async (alertType, message) => {
  // Log error message in the console
  console.log(alertType, message)

  const network = process.argv[2]

  // Prepare the text sent to Discord
  const title = `**${alertType}**`
  const description = `Network: ${network}\nMessage:\n${message}`

  const webhookClient = new WebhookClient({ url: getDiscordWebhook(network) })
  webhookClient.send({
    username: 'Validator Monitoring Bot',
    embeds: [
      {
        title: title,
        description: description,
        color: 'ffcc00' // Yelow
      }
    ]
  })

  // Sleep 1 second to avoid rate limitting
  await new Promise(resolve => setTimeout(resolve, 2000))
}

// Gets the Discord webhook from the .env file
const getDiscordWebhook = (network) => {
  return process.env['DISCORD_WEBHOOK_URL_' + network.toUpperCase()]
}

module.exports = {
  sendValidatorMessage,
  sendMessage
}

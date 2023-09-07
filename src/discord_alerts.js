const { WebhookClient } = require('discord.js')

const sendValidatorMessage = async (alertType, protocol, isAlertActive, vcLocation, validatorIndex, oldData, newData) => {
  // Log error message in the console
  console.log(alertType, protocol, vcLocation, validatorIndex, oldData, newData)

  // Create the Beaconchain direct link to the validator
  const network = process.argv[2]
  const beaconchainUrl = process.env['BEACONCHAIN_ENDPOINT_' + network.toUpperCase()] + '/validator/' + validatorIndex

  // Prepare the text sent to Discord
  const title = `**${alertType}**`
  let description
  if (newData) {
    description = `**VC Location:** ${vcLocation}\n**Protocol:** ${protocol}\n**Network:** ${network}\n${oldData} 🡺 ${newData}\n[${validatorIndex}](<${beaconchainUrl}>)`
  } else {
    if (validatorIndex) {
      description = `**VC Location:** ${vcLocation}\n**Protocol:** ${protocol}\n**Network:** ${network}\n${oldData}\n[${validatorIndex}](<${beaconchainUrl}>)`
    } else {
      description = `**VC Location:** ${vcLocation}\n**Protocol:** ${protocol}\n**Network:** ${network}\n${oldData}`
    }    
  }

  // Select a color depending on the importance
  let color
  if (alertType === 'ATTESTATIONS-MISSED-DELAYED' || alertType === 'BLOCK-MISSED' || alertType === 'BLOCK-EMPTY' || alertType === 'BALANCE-DECREASING') {
    color = 'ff9966' // Orange
  } else if (alertType === 'BLOCK-PROPOSED') {
    color = '99cc33' // Green
  } else if (alertType === 'SLASH-CHANGE') {
    color = 'cc3300' // Red
  } else if (alertType === 'STATUS-CHANGE') {
    if (newData === 'active_online') {
      color = '99cc33' // Green
    } else if (newData === 'active_offline') {
      color = 'ffcc00' // Yellow
    }
  }
  // Default color
  if (!color) {
    color = '636264' // Gray
  }

  if (isAlertActive) {
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
  }

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

const path = require('path')
require('dotenv').config({ path: path.join(__dirname, '../.env') })
const db = require('./db')
const fetch = require('node-fetch')
const discordAlerts = require('./discord_alerts')

// Beaconchain API Docs: https://beaconcha.in/api/v1/docs/index.html
const BEACONCHAIN_VALIDATOR_INFO = '$endpoint/api/v1/validator/$validators'

const checkValidators = async (network) => {
  // Get all the saved validator data randomly
  const savedValidators = await db.query('SELECT public_key, network FROM beacon_chain_validators_monitoring WHERE network = ? ORDER BY RAND()', network)

  // Since the Beaconchain API call rate is very limited (ten requests per minute)
  // we perform requests with multiple validators.
  // The theoretical max number of validators per request is 100, but we reach at an URL length limit
  // 70 validators per request is a safe number
  const savedValidatorsChunks = arrayToChunks(savedValidators, 70)
  for (const savedValidatorsChunk of savedValidatorsChunks) {
    // Prepare the data to perform the request
    const publicKeyChunkString = savedValidatorsChunk.map((key) => key.public_key).toString()
    const beaconchainEndpoint = getBeaconchainEndpoint(network)
    const beaconchainUrl = BEACONCHAIN_VALIDATOR_INFO.replace('$endpoint', beaconchainEndpoint).replace('$validators', publicKeyChunkString)

    // Perform a request to the Beaconchain API
    const res = await fetch(beaconchainUrl)
    const beaconchainData = await res.json()

    // Handle failed requests
    if (!beaconchainData.status || beaconchainData.status !== 'OK') {
      await discordAlerts.sendMessage('BEACONCHAIN-API-ERROR', JSON.stringify(beaconchainData, null, 2))
      continue
    }

    // Process the data returned and continue checking validators
    await processBeaconchainData(beaconchainData.data)

    // Sleep 10 seconds to avoid rate limitting
    await new Promise(resolve => setTimeout(resolve, 10000))
  }
  console.log('Checking done.', savedValidators.length, 'validators checked')
}

// Checks the changes between the saved validator data and the new data
// And alerts if anything have changed that should not happen
const processBeaconchainData = async (beaconchainData) => {
  // Normalice the data. Convert requests with 1 validators -which are returned as objects- to an array
  if (typeof beaconchainData === 'object' && !Array.isArray(beaconchainData) && beaconchainData !== null) {
    beaconchainData = [beaconchainData]
  }

  // Iterate all validators returned in the response
  for (const validatorData of beaconchainData) {
    const savedValidatorData = (await db.query('SELECT balance, status, slashed, server_identifier FROM beacon_chain_validators_monitoring WHERE public_key = ? LIMIT 1',
      validatorData.pubkey.replace('0x', '')))[0]

    // Convert slash tinyint to boolean
    if (savedValidatorData.slashed === 0) {
      savedValidatorData.slashed = false
    } else if (savedValidatorData.slashed === 1) {
      savedValidatorData.slashed = true
    }
    // The balance should always increase if the saved data is not null
    if (validatorData.balance < savedValidatorData.balance && savedValidatorData.balance && savedValidatorData.status !== 'pending') {
      await discordAlerts.sendValidatorMessage('BALANCE-DECREASING', validatorData.server_identifier, validatorData.pubkey, savedValidatorData.balance, validatorData.balance)
    }
    // Check slash changes if the saved data is not null
    if (validatorData.slashed !== savedValidatorData.slashed && savedValidatorData.slashed !== null) {
      await discordAlerts.sendValidatorMessage('SLASH-CHANGE', validatorData.server_identifier, validatorData.pubkey, savedValidatorData.slashed, validatorData.slashed)
    }
    // Check status changes if the saved data is not null
    if (validatorData.status !== savedValidatorData.status && savedValidatorData.status) {
      await discordAlerts.sendValidatorMessage('STATUS-CHANGE', validatorData.server_identifier, validatorData.pubkey, savedValidatorData.status, validatorData.status)
    }

    // Update validator data
    await db.query('UPDATE beacon_chain_validators_monitoring SET balance = ?, slashed = ?, status = ? WHERE public_key = ?',
      [validatorData.balance, validatorData.slashed, validatorData.status, validatorData.pubkey.replace('0x', '')])
  }
}

// Divide an array into smaller chunks with a max chunk size
const arrayToChunks = (array, chunkSize = 100) => {
  const chunkedArray = []

  let counter = 0
  let counterChunk = -1 // Needed to not produce an empty array with the first iteration

  for (const element of array) {
    const divisionRemainder = counter % chunkSize

    // If we reached the maximum chunk size, create a new subarray
    if (divisionRemainder === 0) {
      counterChunk++
      chunkedArray[counterChunk] = []
    }
    chunkedArray[counterChunk].push(element)

    counter++
  }
  return chunkedArray
}

// Gets the endpoint from the .env file
const getBeaconchainEndpoint = (network) => {
  return process.env['BEACONCHAIN_ENDPOINT_' + network.toUpperCase()]
}

// Usage: node src/check_validators.js <network>
if (process.argv[2]) {
  checkValidators(process.argv[2])
} else {
  console.error('Example usage: node src/check_validators.js gnosis')
}

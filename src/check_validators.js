const db = require('./db')
const fetch = require('node-fetch')
const discordAlerts = require('./discord_alerts')
const path = require('path')
require('dotenv').config({ path: path.join(__dirname, '../.env') })

// Beaconchain API Docs: https://beaconcha.in/api/v1/docs/index.html
const BEACONCHAIN_VALIDATOR_INFO = '$endpoint/api/v1/validator/$validators'

const checkValidators = async (network) => {
  // Get all the saved validator data
  const savedValidators = await db.query('SELECT public_key, network FROM beacon_monitoring WHERE network = ?', network)

  // Since the Beaconchain API call rate is very limited -ten requests per minute-
  // we perform requests of 100 validators at a time, which is the maximum per request
  const savedValidatorsChunks = arrayToChunks(savedValidators)
  for (const savedValidatorsChunk of savedValidatorsChunks) {
    // Prepare the data to perform the request
    const publicKeyChunkString = savedValidatorsChunk.map((key) => key.public_key).toString()
    const beaconchainEndpoint = getBeaconchainEndpoint(network)
    const beaconchainUrl = BEACONCHAIN_VALIDATOR_INFO.replace('$endpoint', beaconchainEndpoint).replace('$validators', publicKeyChunkString)

    // Perform a request to the Beaconchain API
    const res = await fetch(beaconchainUrl)
    const beaconchainData = await res.json()

    // Process the data returned and continue checking validators
    await processBeaconchainData(beaconchainData.data)
  }
}

// Checks the changes between the saved validator data and the new data
// And alerts if anything have changed that should not happen
const processBeaconchainData = async (beaconchainData) => {
  // Normalice the data. Convert requests with 1 validators -which are returned as objects- to array
  if (typeof beaconchainData === 'object' && !Array.isArray(beaconchainData) && beaconchainData !== null) {
    beaconchainData = [beaconchainData]
  }

  // Iterate all validators returned in the response
  for (const validatorData of beaconchainData) {
    const savedValidatorData = (await db.query('SELECT * FROM beacon_monitoring WHERE public_key = ? LIMIT 1', validatorData.pubkey.replace('0x', '')))[0]
    // The balance should always increase
    if (validatorData.balance <= savedValidatorData.balance) {
      discordAlerts.sendMessage('BALANCE-NOT-INCREASING', validatorData.pubkey, savedValidatorData.balance, validatorData.balance)
    }
    // Check slash changes
    if (validatorData.slashed !== savedValidatorData.slashed) {
      discordAlerts.sendMessage('SLASH-CHANGE', validatorData.pubkey, savedValidatorData.slashed, validatorData.slashed)
    }
    // Check status changes
    if (validatorData.status !== savedValidatorData.status) {
      discordAlerts.sendMessage('STATUS-CHANGE', validatorData.pubkey, savedValidatorData.status, validatorData.status)
    }

    // Update validator data
    await db.query('UPDATE beacon_monitoring SET balance = ?, slashed = ?, status = ? WHERE public_key = ?',
      [validatorData.balance, validatorData.slashed, validatorData.status, validatorData.pubkey.replace('0x', '')])
  }
}

// Divide an array into smaller chunks with a max chunk size
const arrayToChunks = (array, chunkSize = 100) => {
  const chunkedArray = []

  let counter = 0
  let counterChunk = 0

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
  return process.env.BEACONCHAIN_ENDPOINT_[network.toUpperCase()]
}

// get saved data
// beaconchain https://beaconcha.in/api/v1/docs/index.html
// update
// log
// quick check

// Usage: node src/check_validators.js <network>
if (process.argv[2] && process.argv[3]) {
  checkValidators(process.argv[2], process.argv[3])
} else {
  console.error('Example usage: node src/check_validators.js gnosis')
}

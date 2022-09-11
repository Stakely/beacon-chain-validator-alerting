const path = require('path')
require('dotenv').config({ path: path.join(__dirname, '../.env') })
const db = require('./db')
const fetch = require('node-fetch')
const discordAlerts = require('./discord_alerts')

// Beaconchain API Docs: https://beaconcha.in/api/v1/docs/index.html
const BEACONCHAIN_VALIDATOR_INFO = '$endpoint/api/v1/validator/$validators'
const BEACONCHAIN_VALIDATOR_ATTESTATIONS = '$endpoint/api/v1/validator/$validators/attestations'

const checkValidators = async (network) => {
  // Convert all the public keys to indexes if there is any
  await convertPublicKeysToIndexes(network)

  // Get all the saved validator data randomly
  const savedValidators = await db.query('SELECT validator_index, network FROM beacon_chain_validators_monitoring WHERE network = ? ORDER BY RAND()', network)

  // Since the Beaconchain API call rate is limited
  // we perform requests with multiple validators.
  // The theoretical max number of validators per request is 100, but we reach at an URL length limit
  // 75 validators per request is a safe number
  // Update 2022: using validator indexes instead of public keys allows us to requests 100 validators per requiest
  const savedValidatorsChunks = arrayToChunks(savedValidators, 100)
  for (const savedValidatorsChunk of savedValidatorsChunks) {
    // Prepare the data to perform the request
    const indexesChunkString = savedValidatorsChunk.map((key) => key.validator_index).toString()
    const beaconchainEndpoint = getBeaconchainEndpoint(network)
    const beaconchainUrl = BEACONCHAIN_VALIDATOR_INFO.replace('$endpoint', beaconchainEndpoint).replace('$validators', indexesChunkString)

    // Perform a request to the Beaconchain API
    const res = await fetch(beaconchainUrl, {
      headers: {
        apikey: process.env.BEACONCHAIN_API_KEY
      }
    })
    const beaconchainData = await res.json()

    // Handle failed requests
    if (!beaconchainData.status || beaconchainData.status !== 'OK') {
      await discordAlerts.sendMessage('BEACONCHAIN-API-ERROR', JSON.stringify(beaconchainData, null, 2))
      continue
    }

    // Process the data returned and continue checking validators
    await processBeaconchainData(beaconchainData.data)

    // Check attestation performance with a random validator
    const validatorIndex = savedValidatorsChunk[0].validator_index
    const beaconchainAttestationsUrl = BEACONCHAIN_VALIDATOR_ATTESTATIONS.replace('$endpoint', beaconchainEndpoint).replace('$validators', validatorIndex)
    const resLatestAttestations = await fetch(beaconchainAttestationsUrl, {
      headers: {
        apikey: process.env.BEACONCHAIN_API_KEY
      }
    })
    const latestAttestationsData = await resLatestAttestations.json()

    // Process the data returned and continue checking validators
    await processLatestAttestationData(latestAttestationsData, validatorIndex)

    // Set this sleep to avoid rate limitting if you are using the free plan
    // await new Promise(resolve => setTimeout(resolve, 15000))
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
    const savedValidatorData = (await db.query('SELECT balance, status, slashed, server_hostname FROM beacon_chain_validators_monitoring WHERE validator_index = ? LIMIT 1',
      validatorData.validatorindex))[0]

    // Convert slash tinyint to boolean
    if (savedValidatorData.slashed === 0) {
      savedValidatorData.slashed = false
    } else if (savedValidatorData.slashed === 1) {
      savedValidatorData.slashed = true
    }
    // This message was too spammy. Replaced by the attestation check
    // The balance should always increase if the saved data is not null
    // if (validatorData.balance < savedValidatorData.balance && savedValidatorData.balance && savedValidatorData.status !== 'pending') {
    //  await discordAlerts.sendValidatorMessage('BALANCE-DECREASING', savedValidatorData.server_hostname, validatorData.validatorindex, savedValidatorData.balance, validatorData.balance)
    // }
    // Check slash changes if the saved data is not null
    if (validatorData.slashed !== savedValidatorData.slashed && savedValidatorData.slashed !== null) {
      await discordAlerts.sendValidatorMessage('SLASH-CHANGE', savedValidatorData.server_hostname, validatorData.validatorindex, savedValidatorData.slashed, validatorData.slashed)
    }
    // Check status changes even if the saved data is null (validator starts validating)
    if (validatorData.status !== savedValidatorData.status) {
      // These changes are almost everytime false positives
      if ((savedValidatorData.status === 'active_offline' && validatorData.status === 'active_online') || (savedValidatorData.status === 'active_online' && validatorData.status === 'active_offline')) {
        // Spam
      } else {
        await discordAlerts.sendValidatorMessage('STATUS-CHANGE', savedValidatorData.server_hostname, validatorData.validatorindex, savedValidatorData.status, validatorData.status)
      }
    }

    // Update validator data
    await db.query('UPDATE beacon_chain_validators_monitoring SET balance = ?, slashed = ?, status = ? WHERE validator_index = ?',
      [validatorData.balance, validatorData.slashed, validatorData.status, validatorData.validatorindex])
  }
}

const processLatestAttestationData = async (latestAttestations, validatorIndex) => {
  // Search for missed attestations in the last 10 epochs
  let missedAttestationsCount = 0
  const missedSlots = []

  // Remove the latest attestation slot since the epoch may not be justified yet. We really check the last 9 epochs
  latestAttestations.data.shift()

  for (const attestation of latestAttestations.data) {
    if (attestation.status === 0) {
      missedAttestationsCount++
      missedSlots.push(attestation.attesterslot)
    }
  }
  // Notify if one or more attestations were lost
  if (missedAttestationsCount >= 1) {
    // Get information about that validator
    const savedValidatorData = (await db.query('SELECT server_hostname FROM beacon_chain_validators_monitoring WHERE validator_index = ? LIMIT 1', validatorIndex))[0]
    await discordAlerts.sendValidatorMessage('ATTESTATIONS-MISSED', savedValidatorData.server_hostname, validatorIndex, `A total of ${missedAttestationsCount} attestations were missed during the lastest 9 justified epochs.\nMissed epochs: ${missedSlots.join(', ')}`)
  }
}

const convertPublicKeysToIndexes = async (network) => {
  // Get all the saved validators without index
  const savedValidators = await db.query('SELECT public_key, network FROM beacon_chain_validators_monitoring WHERE network = ? AND validator_index IS NULL', network)
  const beaconchainEndpoint = getBeaconchainEndpoint(network)
  for (const savedValidator of savedValidators) {
    // Perform a request to the Beaconchain API
    const beaconchainUrl = BEACONCHAIN_VALIDATOR_INFO.replace('$endpoint', beaconchainEndpoint).replace('$validators', savedValidator.public_key)
    const res = await fetch(beaconchainUrl, {
      headers: {
        apikey: process.env.BEACONCHAIN_API_KEY
      }
    })
    const beaconchainData = await res.json()
    const validatorIndex = beaconchainData.data.validatorindex
    // Update validator data
    await db.query('UPDATE beacon_chain_validators_monitoring SET validator_index = ? WHERE public_key = ?', [validatorIndex, savedValidator.public_key])
    // Sleep since there may be many requests here
    await new Promise(r => setTimeout(r, 2000));
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

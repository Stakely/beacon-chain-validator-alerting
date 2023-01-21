const path = require('path')
require('dotenv').config({ path: path.join(__dirname, '../.env') })
const db = require('./db')
const fetch = require('node-fetch')
const discordAlerts = require('./discord_alerts')

// Beaconchain API Docs: https://beaconcha.in/api/v1/docs/index.html

const NETWORK = process.argv[2]
const BEACONCHAIN_VALIDATOR_INFO = '$endpoint/api/v1/validator/$validators'
const BEACONCHAIN_VALIDATOR_ATTESTATIONS = '$endpoint/api/v1/validator/$validators/attestations'
const BEACONCHAIN_VALIDATOR_BLOCKS = '$endpoint/api/v1/validator/$validators/proposals'
const BEACONCHAIN_VALIDATOR_SYNC_COMMITTEES = '$endpoint/api/v1/sync_committee/'
const BEACONCHAIN_VALIDATOR_EPOCH = '$endpoint/api/v1/epoch/$epoch'
const BEACONCHAIN_ENDPOINT = process.env['BEACONCHAIN_ENDPOINT_' + NETWORK.toUpperCase()]
const BEACONCHAIN_EXPLORER = BEACONCHAIN_ENDPOINT + '/validator/$validatorIndex#attestations'
let  BEACONCHAIN_EXPLORER_SLOT = BEACONCHAIN_ENDPOINT + '/slot/$slot'
// The Gnosis explorer uses an old endpoint for slots
if (NETWORK === 'gnosis') BEACONCHAIN_EXPLORER_SLOT = BEACONCHAIN_ENDPOINT + '/block/$slot'
const BEACONCHAIN_EXECUTION = '$endpoint/api/v1/execution/block/$block'
const EXEC_EXPLORER = process.env['EXEC_EXPLORER_' + NETWORK.toUpperCase()]

const checkValidators = async () => {
  console.time('elapsed')
  console.log(new Date(), 'Starting BeaconChain Validator Alerting for the network: ' + NETWORK)

  // Convert all the public keys to indexes if there is any left
  await convertPublicKeysToIndexes()

  // Check Beaconchain data
  await checkBeaconchainData()

  // Check sync comitees (data not available in prater)
  if (NETWORK !== 'prater')  await checkSyncCommittees()

  // Check blocks
  await checkBlocks()

  // Check attestations
  await checkAttestations()

  console.log(new Date(), 'Finished')
  console.timeEnd('elapsed')
}

const convertPublicKeysToIndexes = async () => {
  // Get a random sample of 70 saved validators without index
  // We do not fetch all the validators here since requests are limited
  const savedValidators = await db.query('SELECT public_key FROM beacon_chain_validators_monitoring WHERE network = ? AND validator_index IS NULL ORDER BY RAND() LIMIT 70', NETWORK)
  // Iterate validators in groups of 70 instead of 100 since the url is too large using public keys
  const savedValidatorsChunks = arrayToChunks(savedValidators, 70)
  for (const savedValidatorsChunk of savedValidatorsChunks) {
    // Prepare the data to perform the request
    const publicKeysChunkString = savedValidatorsChunk.map((key) => key.public_key).toString()
    const beaconchainUrl = BEACONCHAIN_VALIDATOR_INFO.replace('$endpoint', BEACONCHAIN_ENDPOINT).replace('$validators', publicKeysChunkString)
    // Perform the request to the Beaconchain API
    const res = await fetch(beaconchainUrl, {
      headers: {
        apikey: process.env.BEACONCHAIN_API_KEY
      }
    })
    let beaconchainData = await res.json()
    beaconchainData = beaconchainData.data

    // If no results found
    if (!beaconchainData) {
      console.log('Some validator indexes are not available on-chain')
      continue
    }

    // Normalice the data. Convert requests with 1 validators -which are returned as objects- to an array
    if (typeof beaconchainData === 'object' && !Array.isArray(beaconchainData) && beaconchainData !== null) {
      beaconchainData = [beaconchainData]
    }

    // Update validator index
    for (const beaconchainValidator of beaconchainData) {
      await db.query('UPDATE beacon_chain_validators_monitoring SET validator_index = ? WHERE public_key = ?', [beaconchainValidator.validatorindex, beaconchainValidator.pubkey.replace('0x','')])
    }
  }
  console.log('Validator indexes check done.', savedValidators.length, 'validators checked')
}

const checkBeaconchainData = async () => {
  // Get all the saved validator data randomly
  const savedValidators = await db.query('SELECT validator_index, server_hostname, balance, status, slashed, slashed FROM beacon_chain_validators_monitoring WHERE network = ? AND validator_index IS NOT NULL ORDER BY RAND()', NETWORK)

  // The maximum number of validators per request is 100
  const savedValidatorsChunks = arrayToChunks(savedValidators, 100)
  for (const savedValidatorsChunk of savedValidatorsChunks) {
    // Prepare the data to perform the request
    const indexesChunkString = savedValidatorsChunk.map((key) => key.validator_index).toString()
    const beaconchainUrl = BEACONCHAIN_VALIDATOR_INFO.replace('$endpoint', BEACONCHAIN_ENDPOINT).replace('$validators', indexesChunkString)

    // Perform a request to the Beaconchain API
    let res, beaconchainData
    try {
      res = await fetch(beaconchainUrl, {
        headers: {
          apikey: process.env.BEACONCHAIN_API_KEY
        }
      })
      beaconchainData = await res.json()
    } catch (error) {
      console.error('Beaconchain API error. URL', beaconchainUrl, 'response', JSON.stringify(res))
      continue
    }

    // Handle failed requests
    if (!beaconchainData.status || beaconchainData.status !== 'OK') {
      await discordAlerts.sendMessage('BEACONCHAIN-API-ERROR', JSON.stringify(beaconchainData, null, 2))
      continue
    }
    beaconchainData = beaconchainData.data

    // Process Beaconchain API results
    // Checks the changes between the saved validator data and the new data
    // And alerts if anything have changed that should not happen

    // Normalice the data. Convert requests with 1 validators -which are returned as objects- to an array
    if (typeof beaconchainData === 'object' && !Array.isArray(beaconchainData) && beaconchainData !== null) {
      beaconchainData = [beaconchainData]
    }

    // Iterate all validators returned in the response
    for (const validatorData of beaconchainData) {
      // Get saved validator data from the request we made previously from the db
      const savedValidatorData = savedValidators.find(validator => validator.validator_index === validatorData.validatorindex)

      // Convert slash data tinyint to boolean
      if (savedValidatorData.slashed === 0) {
        savedValidatorData.slashed = false
      } else if (savedValidatorData.slashed === 1) {
        savedValidatorData.slashed = true
      }
    
      // Check for large balance decrease. Missed sync committee duties should trigger this
       if (validatorData.balance < savedValidatorData.balance - 40000 && savedValidatorData.balance && savedValidatorData.status !== 'pending') {
        await discordAlerts.sendValidatorMessage('BALANCE-DECREASING', savedValidatorData.server_hostname, validatorData.validatorindex, savedValidatorData.balance / 1e9, validatorData.balance / 1e9)
      }
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
  console.log('Beaconchain data check done.', savedValidators.length, 'validators checked')
}

const checkSyncCommittees = async () => {
  // Get all the saved validators
  const savedValidators = await db.query('SELECT validator_index, server_hostname, last_epoch_checked FROM beacon_chain_validators_monitoring WHERE network = ? AND validator_index IS NOT NULL', NETWORK)

  const beaconchainUrlLatest = BEACONCHAIN_VALIDATOR_SYNC_COMMITTEES.replace('$endpoint', BEACONCHAIN_ENDPOINT) + 'latest'
  const res = await fetch(beaconchainUrlLatest, {
    headers: {
      apikey: process.env.BEACONCHAIN_API_KEY
    }
  })
  let beaconchainDataLatest = await res.json()

  // Handle failed requests
  if (!beaconchainDataLatest.status || beaconchainDataLatest.status !== 'OK') {
    await discordAlerts.sendMessage('BEACONCHAIN-API-ERROR', JSON.stringify(beaconchainDataLatest, null, 2))
  }

  // Checks if any validator is in the current sync committee
  const latestValidators = beaconchainDataLatest.data.validators
  for (const latestValidator of latestValidators) {
    for (const savedValidator of savedValidators) {
      if (latestValidator === savedValidator.validator_index) {
        if (savedValidator.last_epoch_checked < beaconchainDataLatest.data.start_epoch) {
          await discordAlerts.sendValidatorMessage('SYNC-COMMITTEE', savedValidator.server_hostname, latestValidator,
            `Validator in **current** sync commitee.\n Start epoch: ${beaconchainDataLatest.data.start_epoch}, end epoch: ${beaconchainDataLatest.data.end_epoch}, period: ${beaconchainDataLatest.data.period}`)
        }
      }
    }
  }

  const beaconchainUrlNext = BEACONCHAIN_VALIDATOR_SYNC_COMMITTEES.replace('$endpoint', BEACONCHAIN_ENDPOINT) + 'next'
  const resp = await fetch(beaconchainUrlNext, {
    headers: {
      apikey: process.env.BEACONCHAIN_API_KEY
    }
  })
  let beaconchainDataNext = await resp.json()

  // Handle failed requests
  if (!beaconchainDataNext.status || beaconchainDataNext.status !== 'OK') {
    await discordAlerts.sendMessage('BEACONCHAIN-API-ERROR', JSON.stringify(beaconchainDataNext, null, 2))
  }

  // Checks if any validator is in the next sync committee
  const nextValidators = beaconchainDataNext.data.validators
  for (const nextValidator of nextValidators) {
    for (const savedValidator of savedValidators) {
      if (nextValidator === savedValidator.validator_index) {
        // We use latest here since it is the start of the check window
        if (savedValidator.last_epoch_checked < beaconchainDataLatest.data.start_epoch) {
          await discordAlerts.sendValidatorMessage('SYNC-COMMITTEE', savedValidator.server_hostname, nextValidator,
            `Validator in **next** sync commitee.\nStart epoch: ${beaconchainDataNext.data.start_epoch}, end epoch: ${beaconchainDataNext.data.end_epoch}, period: ${beaconchainDataNext.data.period}`)
        }
      }
    }
  }
  console.log('Sync committees check done. ', savedValidators.length, 'validators checked')
}

const checkBlocks = async () => {
  // Get all the saved validator data randomly
  const savedValidators = await db.query('SELECT validator_index, last_epoch_checked, server_hostname FROM beacon_chain_validators_monitoring WHERE network = ? AND validator_index IS NOT NULL ORDER BY RAND()', NETWORK)

  // Get the last epoch to discard non-finalized data
  const beaconchainUrl = BEACONCHAIN_VALIDATOR_EPOCH.replace('$endpoint', BEACONCHAIN_ENDPOINT).replace('$epoch', 'latest')
  const res = await fetch(beaconchainUrl, {
    headers: {
      apikey: process.env.BEACONCHAIN_API_KEY
    }
  })
  let beaconchainData = await res.json()
  const latestEpoch = beaconchainData.data.epoch

  // The maximum number of validators per request is 100
  const savedValidatorsChunks = arrayToChunks(savedValidators, 100)
  for (const savedValidatorsChunk of savedValidatorsChunks) {
    // Prepare the data to perform the request
    const indexesChunkString = savedValidatorsChunk.map((key) => key.validator_index).toString()
    const beaconchainUrl = BEACONCHAIN_VALIDATOR_BLOCKS.replace('$endpoint', BEACONCHAIN_ENDPOINT).replace('$validators', indexesChunkString)

    // Perform a request to the Beaconchain API
    const res = await fetch(beaconchainUrl, {
      headers: {
        apikey: process.env.BEACONCHAIN_API_KEY
      }
    })
    let beaconchainData = await res.json()

    // Handle failed requests
    if (!beaconchainData.status || beaconchainData.status !== 'OK') {
      await discordAlerts.sendMessage('BEACONCHAIN-API-ERROR', JSON.stringify(beaconchainData, null, 2))
      continue
    }
    beaconchainData = beaconchainData.data

    // If no results found
    if (beaconchainData.length === 0) {
      continue
    }

    // Normalice the data. Convert requests with 1 validators -which are returned as objects- to an array
    if (typeof beaconchainData === 'object' && !Array.isArray(beaconchainData) && beaconchainData !== null) {
      beaconchainData = [beaconchainData]
    }

    // Check block proposal issues from the last check to the latest justified epoch
    for (const validatorData of beaconchainData) {
      const savedValidatorData = savedValidators.find(validator => validator.validator_index === validatorData.proposer)
      if (validatorData.epoch < latestEpoch && validatorData.epoch > savedValidatorData.last_epoch_checked) {
        let blockReward, feeRecipient
        // The Gnosis beaconchain API does not offer this feature
        if (NETWORK !== 'gnosis') {
          // Get execution block information
          const beaconchainUrl = BEACONCHAIN_EXECUTION.replace('$endpoint', BEACONCHAIN_ENDPOINT).replace('$block', validatorData.exec_block_number)
          // Perform a request to the Beaconchain API
          const res = await fetch(beaconchainUrl, {
            headers: {
              apikey: process.env.BEACONCHAIN_API_KEY
            }
          })
          const executionData = await res.json()
          // When there is no execution data like an empty block
          if (executionData.data[0]) {
            blockReward = Number(executionData.data[0].producerReward) / 1e18
            feeRecipient = executionData.data[0].feeRecipient
          }
        }

        // Block information in a string
        const blockInfo = `Validator: [${validatorData.proposer}](<${BEACONCHAIN_EXPLORER.replace('$validatorIndex', validatorData.proposer)}>)
Slot: [${validatorData.slot}](<${BEACONCHAIN_EXPLORER_SLOT.replace('$slot', validatorData.slot)}>)
Epoch: ${validatorData.epoch}
Exec block number: [${validatorData.exec_block_number}](<${EXEC_EXPLORER}${validatorData.exec_block_number}>)
Exec fee recipient: ${validatorData.exec_fee_recipient}
Exec gas limit: ${validatorData.exec_gas_limit}
Exec gas used: ${validatorData.exec_gas_used}
Exec transactions count: ${validatorData.exec_transactions_count}
Exec blockreward: ${blockReward} ETH
Exec feerecipient: ${feeRecipient}
Graffiti: ${validatorData.graffiti_text}`

        // Only nofify in these cases
        if (validatorData.status !== '1') {
          await discordAlerts.sendValidatorMessage('BLOCK-MISSED', savedValidatorData.server_hostname, null, blockInfo)
        } else if (validatorData.exec_transactions_count === 0) {
          await discordAlerts.sendValidatorMessage('BLOCK-EMPTY', savedValidatorData.server_hostname, null, blockInfo)
        } else if (process.env.NOTIFY_SUCCESSFUL_BLOCKS === 'true'){
          await discordAlerts.sendValidatorMessage('BLOCK-PROPOSED', savedValidatorData.server_hostname, null, blockInfo)
        } else if (blockReward > Number(process.env.NOTIFY_LARGE_BLOCKS_THRESHOLD)) {
          await discordAlerts.sendValidatorMessage('BLOCK-PROPOSED', savedValidatorData.server_hostname, null, blockInfo)
        }
      }
    }
  }
  console.log('Blocks check done. ', savedValidators.length, 'validators checked')
}

const checkAttestations = async () => {
  // Get all the saved validator data randomly
  const savedValidators = await db.query('SELECT validator_index, last_epoch_checked, server_hostname FROM beacon_chain_validators_monitoring WHERE network = ? AND validator_index IS NOT NULL ORDER BY RAND()', NETWORK)

  // Extract all the data first and then send an aggregate message by hostname
  const aggregatedMissedAttestations = {}

  // The maximum number of validators per request is 100
  const savedValidatorsChunks = arrayToChunks(savedValidators, 100)
  for (const savedValidatorsChunk of savedValidatorsChunks) {
    // Prepare the data to perform the request
    const indexesChunkString = savedValidatorsChunk.map((key) => key.validator_index).toString()
    const beaconchainUrl = BEACONCHAIN_VALIDATOR_ATTESTATIONS.replace('$endpoint', BEACONCHAIN_ENDPOINT).replace('$validators', indexesChunkString)

    // Perform a request to the Beaconchain API
    const res = await fetch(beaconchainUrl, {
      headers: {
        apikey: process.env.BEACONCHAIN_API_KEY
      }
    })
    let beaconchainData = await res.json()

    // Handle failed requests
    if (!beaconchainData.status || beaconchainData.status !== 'OK') {
      await discordAlerts.sendMessage('BEACONCHAIN-API-ERROR', JSON.stringify(beaconchainData, null, 2))
      continue
    }
    beaconchainData = beaconchainData.data

    // Normalice the data. Convert requests with 1 validators -which are returned as objects- to an array
    if (typeof beaconchainData === 'object' && !Array.isArray(beaconchainData) && beaconchainData !== null) {
      beaconchainData = [beaconchainData]
    }

    // The Prater API does not work well
    if (beaconchainData.length === 0) {
      console.log('The API returned no attestations')
      continue
    }

    // Two latest epoches are always discarded since they have not finalized
    const lastEpoch = beaconchainData[0].epoch - 2

    for (const validatorData of beaconchainData) {
      const savedValidatorData = savedValidators.find(validator => validator.validator_index === validatorData.validatorindex)
      if (validatorData.epoch <= lastEpoch && validatorData.status !== 1 && validatorData.epoch > savedValidatorData.last_epoch_checked) {
        if (!aggregatedMissedAttestations[savedValidatorData.server_hostname]) {
          aggregatedMissedAttestations[savedValidatorData.server_hostname] = []
        }
        // Save the missed attestations to send the data aggregated by hostname
        aggregatedMissedAttestations[savedValidatorData.server_hostname].push({
          validatorIndex: validatorData.validatorindex,
          epoch: validatorData.epoch,
        })
      }
    }
    // Update all the last checked finalized epoch for all the validators
    const indexesChunk = savedValidatorsChunk.map((key) => key.validator_index)
    for (const indexChunk of indexesChunk) {
      await db.query('UPDATE beacon_chain_validators_monitoring SET last_epoch_checked = ? WHERE validator_index = ?', [lastEpoch, indexChunk])
    }
  }

  // Send attestations warnings grouped by server hostname
  for (const hostname in aggregatedMissedAttestations) {
    let text = `**Total attestations:** ${aggregatedMissedAttestations[hostname].length}`
    let attestationsCount = 0
    for (const missedAttestation of aggregatedMissedAttestations[hostname]) {
      // Limit up to 10 attestations per message
      if (attestationsCount >= 10) {
        text = text + `\n**Truncated**`
        break
      }
      text = text + `\nValidator\t[${missedAttestation.validatorIndex}](<${BEACONCHAIN_EXPLORER.replace('$validatorIndex', missedAttestation.validatorIndex)}>)\t-\tEpoch\t${missedAttestation.epoch}`
      attestationsCount++
    }
    await discordAlerts.sendValidatorMessage('ATTESTATIONS-MISSED-DELAYED', hostname, null, text)
  }
  console.log('Attestations check done. ', savedValidators.length, 'validators checked')
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

// Usage: node src/check_validators.js <network>
if (process.argv[2]) {
  checkValidators(process.argv[2])
} else {
  console.error('Example usage: node src/check_validators.js gnosis')
}

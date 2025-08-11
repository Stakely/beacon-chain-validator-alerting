const path = require('path')
require('dotenv').config({ path: path.join(__dirname, '../.env') })
const db = require('./db')
const fetch = require('node-fetch')
const discordAlerts = require('./discord_alerts')
const goteth = require('./goteth')

// Beaconchain API Docs: https://beaconcha.in/api/v1/docs/index.html

const NETWORK = process.argv[2]
const DATA_SOURCE_MODE = process.env.DATA_SOURCE_MODE || 'beaconchain'
const GOTETH_CHUNK_SIZE = parseInt(process.env.GOTETH_CHUNK_SIZE) || 1000


// API endpoint templates for beaconchain
const BEACONCHAIN_VALIDATOR_INFO = '$endpoint/api/v1/validator/$validators'
const BEACONCHAIN_VALIDATOR_ATTESTATIONS = '$endpoint/api/v1/validator/$validators/attestations'
const BEACONCHAIN_VALIDATOR_BLOCKS = '$endpoint/api/v1/validator/$validators/proposals'
const BEACONCHAIN_VALIDATOR_SYNC_COMMITTEES = '$endpoint/api/v1/sync_committee/'
const BEACONCHAIN_VALIDATOR_EPOCH = '$endpoint/api/v1/epoch/$epoch'
const BEACONCHAIN_EXECUTION = '$endpoint/api/v1/execution/block/$block'

const BEACONCHAIN_ENDPOINT = process.env['BEACONCHAIN_ENDPOINT_' + NETWORK.toUpperCase()]
const BEACONCHAIN_API_KEY = process.env.BEACONCHAIN_API_KEY

// Status normalization functions for goteth equivalents
function normalizeStatus(status) {
  // Handle goteth status equivalents
  if (status === 'in_activation_queue' || status === 0) {
    return 'pending'
  }
  if (status === 'active' || status === 1) {
    return 'active'
  }
  return status
}

function getEffectiveStatus(status, exit_epoch) {
  // Get the effective status considering exit_epoch
  const normalizedStatus = normalizeStatus(status)
  if (normalizedStatus === 'active') {
    if (exit_epoch !== '18446744073709551615' && exit_epoch !== 18446744073709551615) {
      return 'exiting_online'
    } else {
      return 'active_online'
    }
  }
  return status
}

function isEquivalentStatus(status1, status2, f_exit_epoch) {
  const normalized1 = normalizeStatus(status1)
  const normalized2 = normalizeStatus(status2)
  
  // Determine the effective status1 based on f_exit_epoch
  let effectiveStatus1 = status1
  if (normalized1 === 'active') {
    if (f_exit_epoch !== '18446744073709551615' && f_exit_epoch !== 18446744073709551615) {
      effectiveStatus1 = 'exiting_online'
    } else {
      effectiveStatus1 = 'active_online'
    }
  }
  
  // Check for specific transitions that should trigger alerts (NOT considered equivalent)
  // active_online → exiting_online should alert
  if ((status2 === 'active_online' || normalized2 === 'active') && effectiveStatus1 === 'exiting_online') {
    return false
  }
  // active → exiting_online should alert
  if (normalized2 === 'active' && effectiveStatus1 === 'exiting_online') {
    return false
  }

  // Check if both are equivalent to active (active, active_online, active_offline, exiting_online)
  // But exclude the transitions we want to alert on above
  if ((effectiveStatus1 === 'active_online' || effectiveStatus1 === 'active_offline' || effectiveStatus1 === 'exiting_online' || normalized1 === 'active') &&
      (normalized2 === 'active' || status2 === 'active_online' || status2 === 'active_offline' || status2 === 'exiting_online')) {
    return true
  }
  
  return normalizeStatus(effectiveStatus1) === normalized2
}

function isPendingStatus(status) {
  return normalizeStatus(status) === 'pending'
}

function isActiveTransition(oldStatus, newStatus) {
  // Check for active_offline <-> active_online transitions (considered spam)
  // BUT do NOT consider transitions TO exiting_online as spam
 
  // If the NEW status is exiting_online, this is NOT a spam transition
  if (newStatus === 'exiting_online') {
    return false
  }

  const isOldActiveOffline = (oldStatus === 'active_offline' || (normalizeStatus(oldStatus) === 'active'))
  const isNewActiveOnline = (newStatus === 'active_online' || (normalizeStatus(newStatus) === 'active'))
  const isOldActiveOnline = (oldStatus === 'active_online' || (normalizeStatus(oldStatus) === 'active'))
  const isNewActiveOffline = (newStatus === 'active_offline' || (normalizeStatus(newStatus) === 'active'))
  
  return (isOldActiveOffline && isNewActiveOnline) || (isOldActiveOnline && isNewActiveOffline)
}

// Data fetching functions - abstraction layer for different data sources
const dataFetcher = {
  // Beaconchain fetch-based implementation
  async fetchValidatorInfo(validators) {
    if (DATA_SOURCE_MODE === 'goteth') {
      return await gotethFetcher.getValidatorInfo(validators)
    } else {
      const url = BEACONCHAIN_VALIDATOR_INFO.replace('$endpoint', BEACONCHAIN_ENDPOINT).replace('$validators', validators.join(','))
      const res = await fetch(url, { headers: { apikey: BEACONCHAIN_API_KEY } })
      return await res.json()
    }
  },

  async fetchValidatorAttestations(validators) {
    if (DATA_SOURCE_MODE === 'goteth') {
      return await gotethFetcher.getValidatorAttestations(validators)
    } else {
      const url = BEACONCHAIN_VALIDATOR_ATTESTATIONS.replace('$endpoint', BEACONCHAIN_ENDPOINT).replace('$validators', validators.join(','))
      const res = await fetch(url, { headers: { apikey: BEACONCHAIN_API_KEY } })
      return await res.json()
    }
  },

  async fetchValidatorBlocks(validators, indexesWithLastEpochCheckedArray, epoch) {
    if (DATA_SOURCE_MODE === 'goteth') {
      // return await gotethFetcher.getValidatorBlocks(validators, indexesWithLastEpochCheckedArray, epoch)
      return await gotethFetcher.getValidatorBlocksFromEpoch( indexesWithLastEpochCheckedArray)
    } else {
      const url = BEACONCHAIN_VALIDATOR_BLOCKS.replace('$endpoint', BEACONCHAIN_ENDPOINT).replace('$validators', validators.join(','))
      const res = await fetch(url, { headers: { apikey: BEACONCHAIN_API_KEY } })
      return await res.json()
    }
  },

  async fetchSyncCommittee(period) {
    if (DATA_SOURCE_MODE === 'goteth') {
      return await gotethFetcher.getSyncCommittee(period)
    } else {
      const url = BEACONCHAIN_VALIDATOR_SYNC_COMMITTEES.replace('$endpoint', BEACONCHAIN_ENDPOINT) + period
      const res = await fetch(url, { headers: { apikey: BEACONCHAIN_API_KEY } })
      return await res.json()
    }
  },

  async fetchEpoch(epoch) {
    if (DATA_SOURCE_MODE === 'goteth') {
      return await gotethFetcher.getEpoch(epoch)
    } else {
      const url = BEACONCHAIN_VALIDATOR_EPOCH.replace('$endpoint', BEACONCHAIN_ENDPOINT).replace('$epoch', epoch)
      const res = await fetch(url, { headers: { apikey: BEACONCHAIN_API_KEY } })
      return await res.json()
    }
  },

  async fetchExecutionBlock(blockNumber) {
    if (DATA_SOURCE_MODE === 'goteth') {
      return await gotethFetcher.getExecutionBlock(blockNumber)
    } else {
      const url = BEACONCHAIN_EXECUTION.replace('$endpoint', BEACONCHAIN_ENDPOINT).replace('$block', blockNumber)
      const res = await fetch(url, { headers: { apikey: BEACONCHAIN_API_KEY } })
      return await res.json()
    }
  },

  async fetchMissingSyncCommittee(indexesWithLastEpochCheckedArray) {
    if (DATA_SOURCE_MODE === 'goteth') {
      return await gotethFetcher.getMissingSyncCommittee(indexesWithLastEpochCheckedArray)
    } else {
      console.log('fetchMissingSyncCommittee not implemented for beaconchain')
      return []
    }
  },

  async fetchValidatorConsolidationEvents(validatorPubkeys) {
    if (DATA_SOURCE_MODE === 'goteth') {
      return await gotethFetcher.getValidatorConsolidationEvents(validatorPubkeys)
    } else {
      console.log('fetchValidatorConsolidationEvents not implemented for beaconchain')
      return []
    }
  },
}

// Goteth function-based implementation
const gotethFetcher = {
  async getValidatorInfo(validators) {
    return await goteth.getValidatorInfo(NETWORK, validators)
  },

  async getValidatorAttestations(validators) {
    return await goteth.getValidatorAttestations(NETWORK, validators)
  },

  async getValidatorBlocks(validators, indexesWithLastEpochCheckedArray, epoch) {
    return await goteth.getValidatorBlocks(NETWORK, validators, indexesWithLastEpochCheckedArray, epoch)
  },

  async getValidatorBlocksFromEpoch(validatorEpochPairs) {
    return await goteth.getValidatorBlocksFromEpoch(NETWORK, validatorEpochPairs)
  },

  async getSyncCommittee(period) {
    return await goteth.getSyncCommittee(NETWORK, period)
  },

  async getEpoch(epoch) {
    return await goteth.getEpoch(NETWORK, epoch)
  },

  async getExecutionBlock(blockNumber) {
    return await goteth.getExecutionBlock(NETWORK, blockNumber)
  },

  async getMissingSyncCommittee(indexesWithLastEpochCheckedArray) {
    return await goteth.getMissingSyncCommittee(NETWORK, indexesWithLastEpochCheckedArray)
  },

  async getValidatorConsolidationEvents(validatorPubkeys) {
    return await goteth.getValidatorConsolidationEvents(NETWORK, validatorPubkeys)
  }
}

const BEACONCHAIN_EXPLORER = BEACONCHAIN_ENDPOINT + '/validator/$validatorIndex#attestations'
let BEACONCHAIN_EXPLORER_SLOT = BEACONCHAIN_ENDPOINT + '/slot/$slot'
// The Gnosis explorer uses an old endpoint for slots
if (NETWORK === 'gnosis') BEACONCHAIN_EXPLORER_SLOT = BEACONCHAIN_ENDPOINT + '/block/$slot'
const EXEC_EXPLORER = process.env['EXEC_EXPLORER_' + NETWORK.toUpperCase()]

const checkValidators = async () => {
  console.time('elapsed')
  console.log(new Date(), 'Starting BeaconChain Validator Alerting for the network: ' + NETWORK)
  console.log('Data source mode:', DATA_SOURCE_MODE)

  // Get the last finalized epoch for all data source modes
  let latestEpoch
  try {
    const epochData = await dataFetcher.fetchEpoch('latest')
    if (!epochData.status || epochData.status !== 'OK') {
      await discordAlerts.sendMessage('API-ERROR', JSON.stringify(epochData, null, 2))
      return
    }
    latestEpoch = Number(epochData.data.epoch)
    console.log('Latest finalized epoch:', latestEpoch)
  } catch (error) {
    console.error(`${DATA_SOURCE_MODE.toUpperCase()} epoch fetching error:`, error.message)
    await discordAlerts.sendMessage('API-ERROR', error.message)
    return
  }

  // Convert all the public keys to indexes if there is any left
  await convertPublicKeysToIndexes()

  // Check Beaconchain data
  await checkBeaconchainData()

  // Check sync comitees (data not available in prater)
  if (DATA_SOURCE_MODE === 'beaconchain') await checkSyncCommittees()

  // Check sync committee missed // only works with goteth
  if (DATA_SOURCE_MODE === 'goteth') await checkSyncCommitteeMissed()

  // Check blocks
  await checkBlocks(latestEpoch)

  // Check attestations
  await checkAttestations()

  // Check consolidation events // only works with goteth
  if (DATA_SOURCE_MODE === 'goteth') await checkConsolidationEvents()

  // Update last epoch checked for all validators
  await updateLastEpochChecked(latestEpoch)

  console.log(new Date(), 'Finished')
  console.timeEnd('elapsed')
  process.exit()
}

const convertPublicKeysToIndexes = async () => {
  // Get saved validators without index
  const savedValidators = await db.query('SELECT public_key FROM beacon_chain_validators_monitoring WHERE network = ? AND validator_index IS NULL AND is_alert_active = 1 ORDER BY RAND()', NETWORK)

  // Chunk based on data source: beaconchain has URL length limits with public keys, goteth has query size limits
  const chunkSize = DATA_SOURCE_MODE === 'goteth' ? GOTETH_CHUNK_SIZE : 70  // Large chunks for goteth (configurable), 70 for beaconchain (URL length limit)
  const savedValidatorsChunks = chunkSize >= savedValidators.length ? [savedValidators] : arrayToChunks(savedValidators, chunkSize)

  console.log(`Processing ${savedValidators.length} validators for index conversion in ${savedValidatorsChunks.length} chunk(s) using ${DATA_SOURCE_MODE} mode (chunk size: ${chunkSize})`)
  for (const savedValidatorsChunk of savedValidatorsChunks) {
    // Prepare the data to perform the request
    const publicKeysArray = savedValidatorsChunk.map((key) => key.public_key)
    // Perform the request through the abstraction layer
    const response = await dataFetcher.fetchValidatorInfo(publicKeysArray)
    let beaconchainData = response.data

    // If no results found
    if (!beaconchainData || beaconchainData.length === 0) {
      console.log('Some validator indexes are not available on-chain')
      continue
    }

    // Normalice the data. Convert requests with 1 validators -which are returned as objects- to an array
    if (typeof beaconchainData === 'object' && !Array.isArray(beaconchainData) && beaconchainData !== null) {
      beaconchainData = [beaconchainData]
    }

    // Update validator index
    for (const beaconchainValidator of beaconchainData) {
      await db.query('UPDATE beacon_chain_validators_monitoring SET validator_index = ? WHERE public_key = ? AND network = ?', [beaconchainValidator.validatorindex, beaconchainValidator.pubkey.replace('0x',''), NETWORK])
    }
  }
  console.log('Validator indexes check done.', savedValidators.length, 'validators checked')
}

const checkBeaconchainData = async () => {
  // Get all the saved validator data randomly
  const savedValidators = await db.query('SELECT validator_index, protocol, is_alert_active, vc_location, balance, status, slashed, slashed FROM beacon_chain_validators_monitoring WHERE network = ? AND validator_index IS NOT NULL AND is_alert_active = 1 ORDER BY RAND()', NETWORK)

  // Chunk based on data source: beaconchain has limits, goteth has query size limits
  const chunkSize = DATA_SOURCE_MODE === 'goteth' ? GOTETH_CHUNK_SIZE : 100  // Large chunks for goteth (configurable), 100 for beaconchain
  const savedValidatorsChunks = chunkSize >= savedValidators.length ? [savedValidators] : arrayToChunks(savedValidators, chunkSize)

  console.log(`Processing ${savedValidators.length} validators in ${savedValidatorsChunks.length} chunk(s) using ${DATA_SOURCE_MODE} mode (chunk size: ${chunkSize})`)

  for (const savedValidatorsChunk of savedValidatorsChunks) {
    // Prepare the data to perform the request
    const indexesArray = savedValidatorsChunk.map((key) => key.validator_index)

    // Perform a request through the abstraction layer
    let beaconchainData
    try {
      const response = await dataFetcher.fetchValidatorInfo(indexesArray)
      beaconchainData = response
    } catch (error) {
      console.error(`${DATA_SOURCE_MODE.toUpperCase()} data fetching error:`, error.message)
      await discordAlerts.sendMessage('API-ERROR', error.message)
      continue
    }

    // Handle failed requests
    if (!beaconchainData.status || beaconchainData.status !== 'OK') {
      await discordAlerts.sendMessage('API-ERROR', JSON.stringify(beaconchainData, null, 2))
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
      const savedValidatorData = savedValidators.find(validator => validator.validator_index === Number(validatorData.validatorindex))

      // Convert slash data tinyint to boolean
      if (savedValidatorData.slashed === 0) {
        savedValidatorData.slashed = false
      } else if (savedValidatorData.slashed === 1) {
        savedValidatorData.slashed = true
      }
    
      // Check for large balance decrease. Missed sync committee duties should trigger this
       if (validatorData.balance < savedValidatorData.balance - 40000 && savedValidatorData.balance && !isPendingStatus(savedValidatorData.status)) {
        //await discordAlerts.sendValidatorMessage('BALANCE-DECREASING', savedValidatorData.protocol, savedValidatorData.is_alert_active savedValidatorData.vc_location, validatorData.validatorindex, savedValidatorData.balance / 1e9, validatorData.balance / 1e9)
      }
      // Check slash changes if the saved data is not null
      if (validatorData.slashed !== savedValidatorData.slashed && savedValidatorData.slashed !== null) {
        await discordAlerts.sendValidatorMessage('SLASH-CHANGE', savedValidatorData.protocol, savedValidatorData.is_alert_active, savedValidatorData.vc_location, validatorData.validatorindex, savedValidatorData.slashed, validatorData.slashed)
      }
      // These changes are almost everytime false positives
      const effectiveNewStatus = getEffectiveStatus(validatorData.status, validatorData.exit_epoch)
      validatorData.status = effectiveNewStatus
      // console.log('effectiveNewStatus', effectiveNewStatus)
      // Check status changes even if the saved data is null (validator starts validating)
      if (!isEquivalentStatus(validatorData.status, savedValidatorData.status, validatorData.exit_epoch)) {
        if (isActiveTransition(savedValidatorData.status, effectiveNewStatus)) {
          console.log('isActiveTransition', savedValidatorData.status, effectiveNewStatus)
          // Spam
        } else {
          await discordAlerts.sendValidatorMessage('STATUS-CHANGE', savedValidatorData.protocol, savedValidatorData.is_alert_active, savedValidatorData.vc_location, validatorData.validatorindex, savedValidatorData.status, effectiveNewStatus)
        }
      }

      // Update the in-memory data to prevent stale data issues in subsequent chunks
      savedValidatorData.balance = validatorData.balance
      savedValidatorData.slashed = validatorData.slashed
      savedValidatorData.status = validatorData.status

      // Update validator data
      await db.query('UPDATE beacon_chain_validators_monitoring SET balance = ?, slashed = ?, status = ? WHERE validator_index = ? AND network = ?',
        [validatorData.balance, validatorData.slashed, validatorData.status, Number(validatorData.validatorindex), NETWORK])
    }
  }
  console.log('Beaconchain data check done.', savedValidators.length, 'validators checked')
}

const checkSyncCommitteeMissed = async () => {
  // Get all the saved validator data randomly
  const savedValidators = await db.query('SELECT validator_index, protocol, is_alert_active, last_epoch_checked, vc_location, balance, status, slashed FROM beacon_chain_validators_monitoring WHERE network = ? AND validator_index IS NOT NULL AND is_alert_active = 1 ORDER BY RAND()', NETWORK)

  // Chunk based on data source: beaconchain has limits, goteth has query size limits
  const chunkSize = DATA_SOURCE_MODE === 'goteth' ? GOTETH_CHUNK_SIZE : 100  // Large chunks for goteth (configurable), 100 for beaconchain
  const savedValidatorsChunks = chunkSize >= savedValidators.length ? [savedValidators] : arrayToChunks(savedValidators, chunkSize)

  console.log(`Processing ${savedValidators.length} validators for sync committee check in ${savedValidatorsChunks.length} chunk(s) using ${DATA_SOURCE_MODE} mode`)
  for (const savedValidatorsChunk of savedValidatorsChunks) {
    const indexesWithLastEpochCheckedArray = savedValidatorsChunk.map((key) => ({validator_index: key.validator_index, last_epoch_checked: key.last_epoch_checked}))
    const validatorData = await dataFetcher.fetchMissingSyncCommittee(indexesWithLastEpochCheckedArray)

    if (validatorData.data.length > 0) {

      for (const validator of validatorData.data) {
        const foundSavedValidator = savedValidatorsChunk.find(savedValidator => savedValidator.validator_index === Number(validator.validator_index))

        if (!foundSavedValidator) {
          console.log('Validator not found in saved validators', validator.validator_index)
          await discordAlerts.sendMessage('API-ERROR', `checkSyncCommitteeMissed | Validator not found in saved validators : ${validator.validator_index}`)
          continue
        }
        if (foundSavedValidator.last_epoch_checked < validator.epoch) {
          await discordAlerts.sendValidatorMessage('SYNC-COMMITTEE-MISSED', foundSavedValidator.protocol, foundSavedValidator.is_alert_active, foundSavedValidator.vc_location, validator.validator_index, `Validator in epoch ${validator.epoch} Missed sync committee, with less than ${process.env.SYNC_COMMITTEE_PARTICIPATIONS_NUMBER_TARGET} sync committee with (${validator.sync_committee_participations_included}). participations included `)
        } else {
          console.log('Validator already checked in epoch | Should have been notified before', validator.epoch)
        }
      }
    }
  }
}

const checkSyncCommittees = async () => {
  // Get all the saved validators
  const savedValidators = await db.query('SELECT validator_index, protocol, is_alert_active, vc_location, last_epoch_checked FROM beacon_chain_validators_monitoring WHERE network = ? AND validator_index IS NOT NULL AND is_alert_active = 1', NETWORK)

  let beaconchainDataLatest
  try {
    beaconchainDataLatest = await dataFetcher.fetchSyncCommittee('latest')
  } catch (error) {
    console.error(`${DATA_SOURCE_MODE.toUpperCase()} sync committee fetching error:`, error.message)
    await discordAlerts.sendMessage('API-ERROR', error.message)
    return
  }

  // Handle failed requests
  if (!beaconchainDataLatest.status || beaconchainDataLatest.status !== 'OK') {
    await discordAlerts.sendMessage('API-ERROR', JSON.stringify(beaconchainDataLatest, null, 2))
  }

  // Checks if any validator is in the current sync committee
  const latestValidators = beaconchainDataLatest.data.validators
  for (const latestValidator of latestValidators) {
    for (const savedValidator of savedValidators) {
      if (latestValidator === savedValidator.validator_index) {
        if (savedValidator.last_epoch_checked < beaconchainDataLatest.data.start_epoch) {
          await discordAlerts.sendValidatorMessage('SYNC-COMMITTEE', savedValidator.protocol, savedValidator.is_alert_active, savedValidator.vc_location, latestValidator,
            `Validator in **current** sync commitee.\n Start epoch: ${beaconchainDataLatest.data.start_epoch}, end epoch: ${beaconchainDataLatest.data.end_epoch}, period: ${beaconchainDataLatest.data.period}`)
        }
      }
    }
  }

  let beaconchainDataNext
  try {
    beaconchainDataNext = await dataFetcher.fetchSyncCommittee('next')
  } catch (error) {
    console.error(`${DATA_SOURCE_MODE.toUpperCase()} sync committee (next) fetching error:`, error.message)
    await discordAlerts.sendMessage('API-ERROR', error.message)
    return
  }

  // Handle failed requests
  if (!beaconchainDataNext.status || beaconchainDataNext.status !== 'OK') {
    await discordAlerts.sendMessage('API-ERROR', JSON.stringify(beaconchainDataNext, null, 2))
  }

  // Checks if any validator is in the next sync committee
  const nextValidators = beaconchainDataNext.data.validators
  for (const nextValidator of nextValidators) {
    for (const savedValidator of savedValidators) {
      if (nextValidator === savedValidator.validator_index) {
        // We use latest here since it is the start of the check window
        if (savedValidator.last_epoch_checked < beaconchainDataLatest.data.start_epoch) {
          await discordAlerts.sendValidatorMessage('SYNC-COMMITTEE', savedValidator.protocol, savedValidator.is_alert_active, savedValidator.vc_location, nextValidator,
            `Validator in **next** sync commitee.\nStart epoch: ${beaconchainDataNext.data.start_epoch}, end epoch: ${beaconchainDataNext.data.end_epoch}, period: ${beaconchainDataNext.data.period}`)
        }
      }
    }
  }
  console.log('Sync committees check done. ', savedValidators.length, 'validators checked')
}

const checkBlocks = async (latestEpoch) => {
  // Get all the saved validator data randomly
  const savedValidators = await db.query('SELECT validator_index, last_epoch_checked, protocol, is_alert_active, vc_location FROM beacon_chain_validators_monitoring WHERE network = ? AND validator_index IS NOT NULL AND is_alert_active = 1 ORDER BY RAND()', NETWORK)

  // Chunk based on data source: beaconchain has limits, goteth has query size limits
  const chunkSize = DATA_SOURCE_MODE === 'goteth' ? GOTETH_CHUNK_SIZE : 100  // Large chunks for goteth (configurable), 100 for beaconchain
  const savedValidatorsChunks = chunkSize >= savedValidators.length ? [savedValidators] : arrayToChunks(savedValidators, chunkSize)

  console.log(`Processing ${savedValidators.length} validators for blocks check in ${savedValidatorsChunks.length} chunk(s) using ${DATA_SOURCE_MODE} mode`)
  for (const savedValidatorsChunk of savedValidatorsChunks) {
    // Prepare the data to perform the request
    const indexesArray = savedValidatorsChunk.map((key) => key.validator_index)
    const indexesWithLastEpochCheckedArray = savedValidatorsChunk.map((key) => ({validator_index: key.validator_index, last_epoch_checked: key.last_epoch_checked}))
    // Perform a request through the abstraction layer
    let beaconchainData
    try {
      const response = await dataFetcher.fetchValidatorBlocks(indexesArray, indexesWithLastEpochCheckedArray, latestEpoch -1)
      beaconchainData = response
    } catch (error) {
      console.error(`${DATA_SOURCE_MODE.toUpperCase()} blocks fetching error:`, error.message)
      await discordAlerts.sendMessage('API-ERROR', error.message)
      continue
    }

    // Handle failed requests
    if (!beaconchainData.status || beaconchainData.status !== 'OK') {
      await discordAlerts.sendMessage('API-ERROR', JSON.stringify(beaconchainData, null, 2))
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
      const savedValidatorData = savedValidators.find(validator => validator.validator_index === Number(validatorData.proposer))
      if (validatorData.epoch < latestEpoch && validatorData.epoch > savedValidatorData.last_epoch_checked) {
        let blockReward, feeRecipient
        // The Gnosis beaconchain API does not offer this feature
        if (NETWORK !== 'gnosis') {
          // Get execution block information
          try {
            const executionData = await dataFetcher.fetchExecutionBlock(validatorData.exec_block_number)
            // When there is no execution data like an empty block
            if (executionData.data[0]) {
              blockReward = executionData?.data[0].producerReward ? Number(executionData.data[0].producerReward) / 1e18 : 'UNKNOWN'
              feeRecipient = executionData.data[0].feeRecipient
            }
          } catch (error) {
            console.error(`${DATA_SOURCE_MODE.toUpperCase()} execution block fetching error:`, error.message)
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
Exec blockreward: ${blockReward || 'UNKNOWN'} ETH
Exec feerecipient: ${feeRecipient || 'UNKNOWN'}
Graffiti: ${validatorData.graffiti_text}`

        // Only nofify in these cases
        if (String(validatorData.status) === '2') {
          await discordAlerts.sendValidatorMessage('BLOCK-ORPHANED', savedValidatorData.protocol, savedValidatorData.is_alert_active, savedValidatorData.vc_location, null, blockInfo)
        } else if (String(validatorData.status) !== '1') {
          await discordAlerts.sendValidatorMessage('BLOCK-MISSED', savedValidatorData.protocol, savedValidatorData.is_alert_active, savedValidatorData.vc_location, null, blockInfo)
        } else if (validatorData.exec_transactions_count === 0) {
          await discordAlerts.sendValidatorMessage('BLOCK-EMPTY', savedValidatorData.protocol, savedValidatorData.is_alert_active, savedValidatorData.vc_location, null, blockInfo)
        } else if (process.env.NOTIFY_SUCCESSFUL_BLOCKS === 'true'){
          await discordAlerts.sendValidatorMessage('BLOCK-PROPOSED', savedValidatorData.protocol, savedValidatorData.is_alert_active, savedValidatorData.vc_location, null, blockInfo)
        } else if (blockReward > Number(process.env.NOTIFY_LARGE_BLOCKS_THRESHOLD)) {
          await discordAlerts.sendValidatorMessage('BLOCK-PROPOSED', savedValidatorData.protocol, savedValidatorData.is_alert_active, savedValidatorData.vc_location, null, blockInfo)
        }
      }
    }
  }
  console.log('Blocks check done. ', savedValidators.length, 'validators checked')
}

const checkAttestations = async () => {
  // Get all the saved validator data randomly
  const savedValidators = await db.query('SELECT validator_index, last_epoch_checked, protocol, vc_location FROM beacon_chain_validators_monitoring WHERE network = ? AND is_alert_active = 1 AND validator_index IS NOT NULL ORDER BY RAND()', NETWORK)

  // Extract all the data first and then send an aggregate message by hostname
  const aggregatedMissedAttestations = {}

  // Chunk based on data source: beaconchain has limits, goteth has query size limits
  const chunkSize = DATA_SOURCE_MODE === 'goteth' ? GOTETH_CHUNK_SIZE : 100  // Large chunks for goteth (configurable), 100 for beaconchain
  const savedValidatorsChunks = chunkSize >= savedValidators.length ? [savedValidators] : arrayToChunks(savedValidators, chunkSize)

  console.log(`Processing ${savedValidators.length} validators for attestations check in ${savedValidatorsChunks.length} chunk(s) using ${DATA_SOURCE_MODE} mode`)
  for (const savedValidatorsChunk of savedValidatorsChunks) {
    // Prepare the data to perform the request
    const indexesArray = savedValidatorsChunk.map((key) => key.validator_index)

    // Perform a request through the abstraction layer
    let beaconchainData
    try {
      const response = await dataFetcher.fetchValidatorAttestations(indexesArray)
      beaconchainData = response
    } catch (error) {
      console.error(`${DATA_SOURCE_MODE.toUpperCase()} attestations fetching error:`, error.message)
      await discordAlerts.sendMessage('API-ERROR', error.message)
      continue
    }

    // Handle failed requests
    if (!beaconchainData.status || beaconchainData.status !== 'OK') {
      await discordAlerts.sendMessage('API-ERROR', JSON.stringify(beaconchainData, null, 2))
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
      const savedValidatorData = savedValidators.find(validator => validator.validator_index === Number(validatorData.validatorindex))
      if (!savedValidatorData) {
        console.log('Validator not found in saved validators', validatorData.validatorindex)
        // force the validator to be checked
        // convertPublicKeysToIndexes
      }
      // For goteth mode, skip the epoch <= lastEpoch check, but keep other conditions
      const epochCondition = DATA_SOURCE_MODE === 'goteth' ? true : validatorData.epoch <= lastEpoch
      if (epochCondition && Number(validatorData.status) !== 1) {
        if (!aggregatedMissedAttestations[savedValidatorData.vc_location]) {
          aggregatedMissedAttestations[savedValidatorData.vc_location] = []
        }

        // Save the missed attestations to send the data aggregated by hostname
        aggregatedMissedAttestations[savedValidatorData.vc_location].push({
          validatorIndex: validatorData.validatorindex,
          epoch: validatorData.epoch,
          protocol: savedValidatorData.protocol
        })
      }
    }

    if (DATA_SOURCE_MODE === 'beaconchain') {
      // Update all the last checked finalized epoch for all the validators
      const indexesChunk = savedValidatorsChunk.map((key) => key.validator_index)
      for (const indexChunk of indexesChunk) {
        await db.query('UPDATE beacon_chain_validators_monitoring SET last_epoch_checked = ? WHERE validator_index = ? AND network = ?', [lastEpoch, indexChunk, NETWORK])
      }
    }
  }

  // Send attestations warnings grouped by vc location
  for (const vcLocation in aggregatedMissedAttestations) {
    let text = `**Total attestations:** ${aggregatedMissedAttestations[vcLocation].length}`
    let attestationsCount = 0

    // Get affected protocols grouped by hostname
    const protocols = []
    for (const missedAttestation of aggregatedMissedAttestations[vcLocation]) {
      protocols.push(missedAttestation.protocol)
    }

    for (const missedAttestation of aggregatedMissedAttestations[vcLocation]) {
      // Limit up to 10 attestations per message
      if (attestationsCount >= 10) {
        text = text + `\n**Truncated**`
        break
      }
      text = text + `\nValidator\t[${missedAttestation.validatorIndex}](<${BEACONCHAIN_EXPLORER.replace('$validatorIndex', missedAttestation.validatorIndex)}>)\t-\tEpoch\t${missedAttestation.epoch}`
      attestationsCount++
    }
    await discordAlerts.sendValidatorMessage('ATTESTATIONS-MISSED-DELAYED', [...new Set(protocols)].join(', '), true, vcLocation, null, text)
  }
  console.log('Attestations check done. ', savedValidators.length, 'validators checked')
}

const checkConsolidationEvents = async () => {
  // Get all the saved validator data randomly
  const savedValidators = await db.query('SELECT validator_index, protocol, is_alert_active, last_epoch_checked, public_key, vc_location, balance, status, slashed FROM beacon_chain_validators_monitoring WHERE network = ? AND validator_index IS NOT NULL AND is_alert_active = 1 ORDER BY RAND()', NETWORK)

  // Chunk based on data source: beaconchain has limits, goteth has query size limits
  const chunkSize = DATA_SOURCE_MODE === 'goteth' ? GOTETH_CHUNK_SIZE : 100  // Large chunks for goteth (configurable), 100 for beaconchain
  const savedValidatorsChunks = chunkSize >= savedValidators.length ? [savedValidators] : arrayToChunks(savedValidators, chunkSize)

  console.log(`Processing ${savedValidators.length} validators for consolidation events check in ${savedValidatorsChunks.length} chunk(s) using ${DATA_SOURCE_MODE} mode`)
  for (const savedValidatorsChunk of savedValidatorsChunks) {
    const validatorIndexes = savedValidatorsChunk.map((key) => key.validator_index)
    const validatorPubkeys = savedValidatorsChunk.map((key) => key.public_key)

    const validatorData = await dataFetcher.fetchValidatorConsolidationEvents(validatorPubkeys)

    if (validatorData.data.length > 0) {

      for (const validator of validatorData.data) {
        const foundSavedValidator = savedValidatorsChunk.find(savedValidator => savedValidator.public_key === validator.source_pubkey || savedValidator.public_key === validator.target_pubkey)

        if (!foundSavedValidator) {
          console.log('Validator not found in saved validators', validator.validator_index)
          await discordAlerts.sendMessage('API-ERROR', `checkSyncCommitteeMissed | Validator not found in saved validators : ${validator.validator_index}`)
          continue
        }
        if (foundSavedValidator.last_epoch_checked < validator.epoch) {
          await discordAlerts.sendValidatorMessage('CONSOLIDATION-EVENT', foundSavedValidator.protocol, foundSavedValidator.is_alert_active, foundSavedValidator.vc_location, validator.validator_index, foundSavedValidator, `Validator in epoch ${validator.epoch} had a consolidation event\nSource: ${validator.source_pubkey}\nTarget: ${validator.target_pubkey}\nResult: ${validator.result} [result_ref](https://github.com/migalabs/goteth/blob/master/docs/tables.md#reference-for-f_result-1)`)
        } else {
          console.log('Validator already checked in epoch | Should have been notified before', validator.epoch)
        }
      }
    }
  }
}

const updateLastEpochChecked = async (latestEpoch) => {
  // Update last_epoch_checked for all active validators in a single query
  const result = await db.query(
    'UPDATE beacon_chain_validators_monitoring SET last_epoch_checked = ? WHERE network = ? AND validator_index IS NOT NULL AND is_alert_active = 1',
    [latestEpoch, NETWORK]
  )
  
  console.log(`Successfully updated last_epoch_checked to ${latestEpoch} for ${result.affectedRows || result.changes || 'unknown number of'} validators`)
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

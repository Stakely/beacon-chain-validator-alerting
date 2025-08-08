// Goteth data fetching module using ClickHouse
// This module provides ClickHouse-based data extraction for validator information

const { createClient } = require('@clickhouse/client')
const https = require('https')

// ClickHouse configuration
const clickhouseConfig = {
  host: process.env.CH_HOST || 'localhost',
  httpPort: parseInt(process.env.CH_HTTP_PORT || '8123'),
  nativePort: parseInt(process.env.CH_NATIVE_PORT || '9000'),
  database: process.env.CH_DB || 'goteth_default',
  username: process.env.CH_USER || 'username',
  password: process.env.CH_PASSWORD || 'password',
  useSSL: process.env.CH_USE_SSL === 'true', // Add SSL toggle
}

// Create custom HTTPS agent for self-signed certificates
const httpsAgent = new https.Agent({
  rejectUnauthorized: false // Disable SSL verification for self-signed certs
})

// Create ClickHouse client
const clickhouse = createClient({
  url: `${clickhouseConfig.useSSL ? 'https' : 'http'}://${clickhouseConfig.host}:${clickhouseConfig.httpPort}`,
  username: clickhouseConfig.username,
  password: clickhouseConfig.password,
  database: clickhouseConfig.database,
  // Use custom agent only for HTTPS
  ...(clickhouseConfig.useSSL && { http_agent: httpsAgent })
})

const goteth = {
  /**
   * Get validator information by public keys or validator indexes
   * @param {string} network - Network name (mainnet, gnosis, prater)
   * @param {Array} validators - Array of validator public keys or indexes
   * @returns {Object} Response object compatible with beaconchain format
   */
  async getValidatorInfo(network, validators) {
    try {
      console.log(`Fetching validator info from ClickHouse for network: ${network}, validators: ${validators.length}`)
      
      // Build the WHERE clause based on validator type (pubkey vs index)
      const isIndexes = validators.every(v => typeof v === 'number' || /^\d+$/.test(v))
      const whereClause = isIndexes 
        ? `f_val_idx IN (${validators.join(',')})`
        : `f_public_key IN ('${validators.join("','")}')`
      
      const query = `
        SELECT 
          tvls.f_val_idx as validatorindex,
          tvls.f_public_key as pubkey,
          tvls.f_balance_eth as balance,
          tvls.f_status as status_number,
          tvls.f_slashed as slashed,
          tvls.f_activation_epoch as activation_epoch,
          tvls.f_exit_epoch as exit_epoch,
          ts.f_status as status
        FROM t_validator_last_status  tvls
        LEFT JOIN t_status ts ON tvls.f_status = ts.f_id
        WHERE ${whereClause}
        ORDER BY f_val_idx
      `
      
      const resultSet = await clickhouse.query({
        query: query,
        format: 'JSONEachRow'
      })
      
      const data = await resultSet.json()
      
      return {
        status: 'OK',
        data: data
      }
    } catch (error) {
      console.error('ClickHouse validator info query error:', error)
      throw error
    }
  },

  /**
   * Get validator attestations
   * @param {string} network - Network name
   * @param {Array} validators - Array of validator indexes
   * @returns {Object} Response object with attestation data
   */
  async getValidatorAttestations(network, validators) {
    try {
      console.log(`Fetching attestations from ClickHouse for network: ${network}, validators: ${validators.length}`)
      
      const lastFinalizedEpoch = await this.getLastFinalizedEpoch(network)
      if (lastFinalizedEpoch.status !== 'OK') {
        console.error('ClickHouse last finalized epoch query error:', lastFinalizedEpoch.error)
        throw new Error('ClickHouse last finalized epoch query error')
      }

      const query = `
        SELECT 
          f_val_idx as validatorindex,
          f_epoch as epoch,
          f_status as status,
          f_inclusion_delay as inclusion_delay,
          f_missing_source,
          f_missing_target,
          f_missing_head,
          f_reward,
          f_max_reward,
          -- Calculate if completely missed (all three missing)
          CASE 
            WHEN f_missing_source = true AND f_missing_target = true AND f_missing_head = true 
            THEN 0 
            ELSE 1 
          END as status_calculated
        FROM t_validator_rewards_summary 
        WHERE f_val_idx IN (${validators.join(',')})
        AND f_epoch > ${Number(lastFinalizedEpoch.data.epoch)}
        ORDER BY f_epoch DESC
      `
      
      const resultSet = await clickhouse.query({
        query: query,
        format: 'JSONEachRow'
      })
      
      const rawData = await resultSet.json()
      
      // Transform data to match beaconchain format
      const data = rawData.map(row => ({
        validatorindex: row.validatorindex,
        epoch: row.epoch -2, // takes into account the attestation to 2 epochs befor
        slot: row.slot,
        // Use calculated status: 0 = missed, 1 = successful
        status: row.status_calculated,
        inclusion_delay: row.inclusion_delay,
        committee_index: row.committee_index,
        // Additional goteth-specific fields for debugging
        missing_source: row.f_missing_source,
        missing_target: row.f_missing_target,
        missing_head: row.f_missing_head,
        reward: row.f_reward,
        max_reward: row.f_max_reward
      }))
      
      return {
        status: 'OK',
        data: data
      }
    } catch (error) {
      console.error('ClickHouse attestations query error:', error)
      throw error
    }
  },

  /**
   * Get validator block proposals
   * @param {string} network - Network name
   * @param {Array} validators - Array of validator indexes
   * @returns {Object} Response object with block data
   */
  async getValidatorBlocks(network, validators, indexesWithLastEpochCheckedArray, epoch) {
    try {
      console.log(`Fetching blocks from ClickHouse for network: ${network}, validators: ${validators.length}`)
      
      const query = `
        SELECT 
          tvls.f_status as status,
          bm.f_proposer_index as proposer,
          bm.f_slot as slot,
          bm.f_epoch as epoch,
          bm.f_graffiti as graffiti_text,
          bm.f_el_block_number as exec_block_number,
          bm.f_el_fee_recp as exec_fee_recipient,
          bm.f_el_gas_limit as exec_gas_limit,
          bm.f_el_gas_used as exec_gas_used,
          bm.f_el_transactions as exec_transactions_count
        FROM t_block_metrics bm
        INNER JOIN t_validator_last_status tvls ON bm.f_proposer_index = tvls.f_val_idx
        WHERE bm.f_proposer_index IN (${validators.join(',')})
        AND bm.f_epoch = ${epoch}
        ORDER BY bm.f_epoch DESC, bm.f_slot DESC
      `
      
      const resultSet = await clickhouse.query({
        query: query,
        format: 'JSONEachRow'
      })
      
      const data = await resultSet.json()
      
      return {
        status: 'OK',
        data: data
      }
    } catch (error) {
      console.error('ClickHouse blocks query error:', error)
      throw error
    }
  },

  async getValidatorBlocksFromEpoch(network, validatorEpochPairs) {
    try {
      console.log(`Fetching blocks from ClickHouse for network: ${network}, validators: ${validatorEpochPairs.length}`)

    // Create OR conditions: (validator = X AND epoch > Y) OR (validator = Z AND epoch > W) ...
    const conditions = validatorEpochPairs.map(pair => 
      `(bm.f_proposer_index = ${pair.validator_index} AND bm.f_epoch > ${pair.last_epoch_checked})`
    ).join(' OR ')
      
      const query = `
        SELECT 
          CASE WHEN bm.f_proposed = 1 THEN '1' ELSE '0' END as status,
          bm.f_proposer_index as proposer,
          bm.f_slot as slot,
          bm.f_epoch as epoch,
          bm.f_graffiti as graffiti_text,
          bm.f_el_block_number as exec_block_number,
          bm.f_el_fee_recp as exec_fee_recipient,
          bm.f_el_gas_limit as exec_gas_limit,
          bm.f_el_gas_used as exec_gas_used,
          bm.f_el_transactions as exec_transactions_count
        FROM t_block_metrics bm
        INNER JOIN t_validator_last_status tvls ON bm.f_proposer_index = tvls.f_val_idx
        WHERE ${conditions}
        ORDER BY bm.f_epoch DESC, bm.f_slot DESC
      `
      
      const resultSet = await clickhouse.query({
        query: query,
        format: 'JSONEachRow'
      })
      
      const data = await resultSet.json()
      
      // Check for orphaned blocks (f_el_block_number = "0") and fetch from t_orphans table
      const orphanedBlocks = data.filter(row => row.exec_block_number === "0")

      if (orphanedBlocks.length > 0) {
        console.log(`Found ${orphanedBlocks.length} orphaned blocks, fetching from t_orphans table`)

        // Create conditions for orphaned blocks query
        const orphanConditions = orphanedBlocks.map(block =>
          `(o.f_proposer_index = ${block.proposer} AND o.f_slot = ${block.slot})`
        ).join(' OR ')

        const orphanQuery = `
          SELECT
            2 as status,
            o.f_proposer_index as proposer,
            o.f_slot as slot,
            o.f_epoch as epoch,
            o.f_graffiti as graffiti_text,
            o.f_el_block_number as exec_block_number,
            o.f_el_fee_recp as exec_fee_recipient,
            o.f_el_gas_limit as exec_gas_limit,
            o.f_el_gas_used as exec_gas_used,
            o.f_el_transactions as exec_transactions_count
          FROM t_orphans o
          WHERE ${orphanConditions}
          ORDER BY o.f_epoch DESC, o.f_slot DESC
        `

        const orphanResultSet = await clickhouse.query({
          query: orphanQuery,
          format: 'JSONEachRow'
        })

        const orphanData = await orphanResultSet.json()

        // Replace orphaned blocks in original data with t_orphans data
        const updatedData = data.map(row => {
          if (row.exec_block_number === "0") {
            // Find matching orphan data by proposer and slot
            const orphanMatch = orphanData.find(orphan =>
              orphan.proposer === row.proposer && orphan.slot === row.slot
            )
            return orphanMatch || row  // Use orphan data if found, otherwise keep original
          }
          return row
        })

        console.log(`Replaced ${orphanedBlocks.length} orphaned blocks with t_orphans data`)

        return {
          status: 'OK',
          data: updatedData
        }
      }

      return {
        status: 'OK',
        data: data
      }
    } catch (error) {
      console.error('ClickHouse blocks query error:', error)
      throw error
    }
  },
  /**
   * Get validator rewards summary (alternative attestation source)
   * Uses t_validator_rewards_summary table for aggregated attestation data
   * @param {string} network - Network name
   * @param {Array} validators - Array of validator indexes
   * @param {number} epochStart - Start epoch (optional)
   * @param {number} epochEnd - End epoch (optional)
   * @returns {Object} Response object with rewards/attestation summary data
   */
  async getValidatorRewardsSummary(network, validators, epochStart = null, epochEnd = null) {
    try {
      console.log(`Fetching validator rewards summary from ClickHouse for network: ${network}, validators: ${validators.length}`)
      
      let epochFilter = ''
      if (epochStart !== null && epochEnd !== null) {
        epochFilter = `AND f_epoch BETWEEN ${epochStart} AND ${epochEnd}`
      } else if (epochStart !== null) {
        epochFilter = `AND f_epoch >= ${epochStart}`
      }
      
      const query = `
        SELECT 
          f_val_idx as validatorindex,
          f_epoch as epoch,
          f_missing_source,
          f_missing_target,
          f_missing_head,
          -- Calculate attestation status based on missing flags
          CASE 
            WHEN f_missing_source = true AND f_missing_target = true AND f_missing_head = true 
            THEN 0 
            ELSE 1 
          END as attestation_status,
          -- Count partial misses
          (CASE WHEN f_missing_source = true THEN 1 ELSE 0 END +
           CASE WHEN f_missing_target = true THEN 1 ELSE 0 END +
           CASE WHEN f_missing_head = true THEN 1 ELSE 0 END) as missing_count,
          f_att_reward,
          f_sync_reward,
          f_proposer_reward,
          f_total_reward
        FROM t_validator_rewards_summary 
        WHERE f_val_idx IN (${validators.join(',')})
        ${epochFilter}
        ORDER BY f_epoch ASC, f_val_idx
      `
      
      const resultSet = await clickhouse.query({
        query: query,
        format: 'JSONEachRow'
      })
      
      const data = await resultSet.json()
      
      return {
        status: 'OK',
        data: data
      }
    } catch (error) {
      console.error('ClickHouse validator rewards summary query error:', error)
      throw error
    }
  },

  /**
   * Get missed attestations specifically (completely missed ones)
   * @param {string} network - Network name
   * @param {Array} validators - Array of validator indexes
   * @param {number} epochStart - Start epoch (optional)
   * @param {number} epochEnd - End epoch (optional)
   * @returns {Object} Response object with missed attestations
   */
  async getMissedAttestations(network, validators, epochStart = null, epochEnd = null) {
    try {
      console.log(`Fetching missed attestations from ClickHouse for network: ${network}, validators: ${validators.length}`)
      
      let epochFilter = ''
      if (epochStart !== null && epochEnd !== null) {
        epochFilter = `AND f_epoch BETWEEN ${epochStart} AND ${epochEnd}`
      } else if (epochStart !== null) {
        epochFilter = `AND f_epoch >= ${epochStart}`
      }
      
      const query = `
        SELECT 
          f_val_idx as validatorindex,
          f_epoch as epoch,
          f_missing_source,
          f_missing_target,
          f_missing_head,
          0 as status  -- Mark as missed
        FROM t_validator_rewards_summary 
        WHERE f_val_idx IN (${validators.join(',')})
        ${epochFilter}
        -- Only get completely missed attestations
        AND f_missing_source = true 
        AND f_missing_target = true 
        AND f_missing_head = true
        ORDER BY f_epoch DESC, f_val_idx
        LIMIT 10000
      `
      
      const resultSet = await clickhouse.query({
        query: query,
        format: 'JSONEachRow'
      })
      
      const data = await resultSet.json()
      
      return {
        status: 'OK',
        data: data
      }
    } catch (error) {
      console.error('ClickHouse missed attestations query error:', error)
      throw error
    }
  },

  /**
   * Get sync committee information
   * @param {string} network - Network name
   * @param {string} period - 'latest' or 'next'
   * @returns {Object} Response object with sync committee data
   */
  async getSyncCommittee(network, period) {
    try {
      return null // not supported by goteth
      console.log(`Fetching sync committee (${period}) from ClickHouse for network: ${network}`)
      
      // Get current epoch first to determine which period to fetch
      const epochQuery = `
        SELECT MAX(f_epoch) as latest_epoch 
        FROM t_block
      `
      
      const epochResult = await clickhouse.query({
        query: epochQuery,
        format: 'JSONEachRow'
      })
      
      const epochData = await epochResult.json()
      const latestEpoch = epochData[0]?.latest_epoch || 0
      
      // Calculate sync committee period (each period is ~27 hours, 256 epochs)
      const epochsPerPeriod = 256
      const currentPeriod = Math.floor(latestEpoch / epochsPerPeriod)
      const targetPeriod = period === 'next' ? currentPeriod + 1 : currentPeriod
      
      const query = `
        SELECT 
          f_val_idx as validator_index,
          f_period as period,
          f_start_epoch as start_epoch,
          f_end_epoch as end_epoch
        FROM t_sync_committee 
        WHERE f_period = ${targetPeriod}
        ORDER BY f_val_idx
      `
      
      const resultSet = await clickhouse.query({
        query: query,
        format: 'JSONEachRow'
      })
      
      const data = await resultSet.json()
      
      return {
        status: 'OK',
        data: {
          validators: data.map(row => row.validator_index),
          start_epoch: data[0]?.start_epoch || targetPeriod * epochsPerPeriod,
          end_epoch: data[0]?.end_epoch || (targetPeriod + 1) * epochsPerPeriod - 1,
          period: targetPeriod
        }
      }
    } catch (error) {
      console.error('ClickHouse sync committee query error:', error)
      throw error
    }
  },

  /**
   * Get epoch information
   * @param {string} network - Network name
   * @param {string|number} epoch - Epoch number or 'latest'
   * @returns {Object} Response object with epoch data
   */
  async getEpoch(network, epoch) {
    try {
      console.log(`Fetching epoch ${epoch} from ClickHouse for network: ${network}`)
      
      let query
      if (epoch === 'latest') {
        query = `
          SELECT MAX(f_epoch) as epoch 
          FROM t_finalized_checkpoint
        `
      } else {
        query = `
          SELECT 
            f_epoch as epoch,
          FROM t_block_metrics 
          WHERE f_epoch = ${epoch}
        `
      }
      
      const resultSet = await clickhouse.query({
        query: query,
        format: 'JSONEachRow'
      })
      
      const data = await resultSet.json()
      
      return {
        status: 'OK',
        data: {
          epoch: data[0]?.epoch || parseInt(epoch)
        }
      }
    } catch (error) {
      console.error('ClickHouse epoch query error:', error)
      throw error
    }
  },

  /**
   * Get execution block information
   * @param {string} network - Network name
   * @param {number} blockNumber - Execution block number
   * @returns {Object} Response object with execution block data
   */
  async getExecutionBlock(network, blockNumber) {
    try {
      console.log(`Fetching execution block ${blockNumber} from ClickHouse for network: ${network}`)
      
      const query = `
        SELECT 
          bm.f_el_block_number as block_number,
          br.f_bid_commission as producerReward,
          bm.f_el_fee_recp as feeRecipient,
          bm.f_el_gas_limit as gas_limit,
          bm.f_el_gas_used as gas_used,
          bm.f_el_base_fee_per_gas as base_fee_per_gas,
          bm.f_el_transactions as transactions_count,
          CASE WHEN bm.f_proposed = true THEN 1 ELSE 0 END as status
        FROM t_block_metrics bm
        LEFT JOIN t_block_rewards br ON bm.f_slot = br.f_slot
        WHERE bm.f_el_block_number = ${blockNumber}
        LIMIT 1
      `
      
      const resultSet = await clickhouse.query({
        query: query,
        format: 'JSONEachRow'
      })
      
      const data = await resultSet.json()
      
      return {
        status: 'OK',
        data: data
      }
    } catch (error) {
      console.error('ClickHouse execution block query error:', error)
      throw error
    }
  },

  async getLastFinalizedEpoch(network) {
    try {
      console.log(`Fetching last finalized epoch from ClickHouse for network: ${network}`)

      const query = `
        SELECT MAX(f_epoch) as last_finalized_epoch
        FROM t_finalized_checkpoint
      `

      const resultSet = await clickhouse.query({
        query: query,
        format: 'JSONEachRow'
      })

      const data = await resultSet.json()

      return {
        status: 'OK',
        data: {
          epoch: data[0]?.last_finalized_epoch || 0
        }
      }
    } catch (error) {
      console.error('ClickHouse last finalized epoch query error:', error)
      throw error
    }
  },

  async getFirstSlotOfEpoch(network, epoch) {
    try {
      console.log(`Fetching first slot of epoch ${epoch} from ClickHouse for network: ${network}`)

      const query = `
        SELECT MIN(f_slot) as first_slot
        FROM t_block_metrics
        WHERE f_epoch = ${epoch}
      `

      const resultSet = await clickhouse.query({
        query: query,
        format: 'JSONEachRow'
      })

      const data = await resultSet.json()

      return {
        status: 'OK',
        data: {
          first_slot: data[0]?.first_slot || 0
        }
      }
    } catch (error) {
      console.error('ClickHouse last finalized epoch query error:', error)
      throw error
    }
  },

  async getMissingSyncCommittee(network, indexesWithLastEpochCheckedArray) {
    try {
      console.log(`Fetching missing sync committee from ClickHouse for network: ${network}, validators: ${indexesWithLastEpochCheckedArray.length}`)
      
      const targetSyncCommitteeParticipationsNumber = Number(process.env.SYNC_COMMITTEE_PARTICIPATIONS_NUMBER_TARGET || 30) // this could be changing in the futurte if the validator is bigger due to consolidations
      
      // Create OR conditions: (validator = X AND epoch > Y) OR (validator = Z AND epoch > W) ...
      const conditions = indexesWithLastEpochCheckedArray.map(pair => 
        `(f_val_idx = ${pair.validator_index} AND f_epoch > ${pair.last_epoch_checked})`
      ).join(' OR ')
      
      const query = `
        SELECT 
          f_val_idx as validator_index,
          f_epoch as epoch,
          f_sync_committee_participations_included as sync_committee_participations_included
        FROM t_validator_rewards_summary
        WHERE (${conditions})
        AND f_sync_committee_participations_included < ${targetSyncCommitteeParticipationsNumber} AND f_in_sync_committee = true 
        ORDER BY f_epoch ASC
      `

      const resultSet = await clickhouse.query({
        query: query,
        format: 'JSONEachRow'
      })

      const data = await resultSet.json()

      return {
        status: 'OK',
        data: data
      }
    } catch (error) {
      console.error('ClickHouse missing sync committee query error:', error)
      throw error
    }
  },

  async getValidatorConsolidationEvents(network, validatorPubkeys) {
    try {
      console.log(`Fetching validator consolidation events from ClickHouse for network: ${network}, validators: ${validatorPubkeys.length}`)
      
      const lastFinalizedEpoch = await this.getLastFinalizedEpoch(network)
      if (lastFinalizedEpoch.status !== 'OK') {
        console.error('ClickHouse last finalized epoch query error:', lastFinalizedEpoch.error)
        throw new Error('ClickHouse last finalized epoch query error')
      }
      const last100Epoch = Number(lastFinalizedEpoch.data.epoch) - 100
      const firstSlotOfEpoch = await this.getFirstSlotOfEpoch(network, last100Epoch)
      if (firstSlotOfEpoch.status !== 'OK') {
        console.error('ClickHouse first slot of epoch query error:', firstSlotOfEpoch.error)
        throw new Error('ClickHouse first slot of epoch query error')
      }

      const query = `
        SELECT 
          f_slot as slot,
          f_source_address as source_address,
          f_source_pubkey as source_pubkey,
          f_target_pubkey as target_pubkey,
          f_result as result
        FROM t_consolidation_requests
        WHERE (f_source_pubkey IN ('${validatorPubkeys.join("','")}') OR f_target_pubkey IN ('${validatorPubkeys.join("','")}')) AND 
        f_slot >= ${firstSlotOfEpoch.data.first_slot}
        ORDER BY f_slot ASC
      `

      const resultSet = await clickhouse.query({
        query: query,
        format: 'JSONEachRow'
      })

      const data = await resultSet.json()

      return {
        status: 'OK',
        data: data
      }
    } catch (error) {
      console.error('ClickHouse missing sync committee query error:', error)
      throw error
    }
  },
}

module.exports = goteth
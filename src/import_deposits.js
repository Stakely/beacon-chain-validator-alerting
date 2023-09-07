const path = require('path')
require('dotenv').config({ path: path.join(__dirname, '../.env') })
const fs = require('fs')
const db = require('./db')

const importPublicKeys = async (depositFile, protocol, vclocation) => {
  // Read deposit array from file
  const rawDepositData = fs.readFileSync(depositFile)
  const depositData = JSON.parse(rawDepositData)

  // Get the beacon chain network from the first element of the array
  const network = depositData[0].eth2_network_name || depositData[0].network_name

  // Extract just the public keys
  const publicKeys = depositData.map((key) => key.pubkey)

  // Get the current saved publicKeys in the db
  const savedPublicKeys = await db.query('SELECT public_key FROM beacon_chain_validators_monitoring WHERE network = ?', network)

  // Get just the public_key field from the returned array
  const savedPublicKeysMap = savedPublicKeys.map((key) => key.public_key)

  // Leave only the publicKeys that are not already saved
  const newPublicKeys = publicKeys.filter(value => !savedPublicKeysMap.includes(value))

  // Save the new public keys with its Beaconchain endpoint in the db
  for (const newPublicKey of newPublicKeys) {
    await db.query('INSERT INTO beacon_chain_validators_monitoring (public_key, network, protocol, vc_location) VALUES(?,?,?,?)',
      [newPublicKey, network, protocol, vclocation])
  }
  console.log('Process finished.', newPublicKeys.length, 'keys imported from', network)
}

// Usage: node src/import_deposits.js deposits/<deposit file> <protocol name> <optional vc identifier>
if (process.argv[2] && process.argv[3]) {
  importPublicKeys(process.argv[2], process.argv[3], process.argv[4])
} else {
  console.error('Example usage: node src/import_deposits.js deposits/deposit_data-xxx.json my-protocol-name my-vc-identifier')
}

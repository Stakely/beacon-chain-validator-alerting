const fs = require('fs')
const db = require('./db')

const importPublicKeys = async (depositFile, network) => {
  // Read deposit array from file
  const rawDepositData = fs.readFileSync(depositFile)
  const depositData = JSON.parse(rawDepositData)

  // Extract just the public keys
  const publicKeys = depositData.map((key) => key.pubkey)

  // Get the current saved publicKeys in the db
  const savedPublicKeys = await db.query('SELECT public_key FROM beacon_monitoring')

  // Leave only the publicKeys that are not already saved
  const newPublicKeys = publicKeys.filter(value => !savedPublicKeys.includes(value))

  // Save the new public keys with its Beaconchain endpoint in the db
  for (const newPublicKey of newPublicKeys) {
    await db.query('INSERT INTO beacon_monitoring (public_key, network) VALUES(?,?)', [newPublicKey, network])
  }
  console.log('Process finished', newPublicKeys.length, 'keys imported')
}

// Usage: node src/import_deposits.js deposits/<deposit file> <network>
if (process.argv[2] && process.argv[3]) {
  importPublicKeys(process.argv[2], process.argv[3])
} else {
  console.error('Example usage: node src/import_deposits.js deposits/deposit_data-xxx.json prater')
}

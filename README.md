# Beacon Chain Validator Alerting
Monitors multiple Beacon Chain validators using the free Beaconchain API.
- This tool has been developed for teams or individuals that manage hundreds or thousands of validators.
- Valid for any chain supported by Beaconchain, such as Ethereum Mainnet, Gnosis, and Prater.
- Uses Discord as the destination for the alerts, but it should be really easy to support any other platform.


Tool developed and maintained by [Stakely.io](https://stakely.io), a professional non-custodial staking service established as a new generation of node operators for blockchains of the Web3 ecosystem.

<br>

## Requirements
- Node.js 16 or higher
- An accesible MySQL server with the following schema:

Table name: ``beacon_chain_validators_monitoring``

Columns: ``beacon_chain_validator_monitoring_id (int) | public_key (varchar 98) | network (varchar 32) | balance (bigint) (null) | slashed (tinyint 1) (null) | status (varchar 32) (null) | created_at (timestamp) (current timestamp) | updated_at (timestamp) (on update current timestamp)``

<br>

## Setup
Copy the `env.example` file to `.env` and set your custom variables.

Install the Node.js dependencies with `npm install`.

<br>

## Run
### Load validator public keys
Place one or more deposit data json files in the `deposits/` folder and import the public keys
```
node src/import_deposits.js deposits/<file name> <network>
# Eg. node src/import_deposits.js deposits/deposit_data-1641143430.json gnosis
```

<br>

### Check validators
You can test if the script works properly with
```
node src/check_validators.js <network>
# Eg. node src/check_validators.js gnosis
```

Configure the Crontab to execute the check_validators.js script every five minutes
```
crontab -e
*/5 * * * * node /your/custom/path/beacon-chain-validator-alerting/src/check_validators.js mainnet
*/5 * * * * node /your/custom/path/beacon-chain-validator-alerting/src/check_validators.js gnosis
*/5 * * * * node /your/custom/path/beacon-chain-validator-alerting/src/check_validators.js prater
```

If you have many validators you may need to reduce the check period in order to not hit the Beaconchain API rate limit.

An alternative is to pay for the [Beaconchain Paid API](https://beaconcha.in/pricing).

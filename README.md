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

```
SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";

CREATE TABLE `beacon_chain_validators_monitoring` (
  `beacon_chain_validator_monitoring_id` int UNSIGNED NOT NULL,
  `public_key` varchar(96) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `validator_index` int UNSIGNED DEFAULT NULL,
  `network` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `balance` bigint DEFAULT NULL,
  `slashed` tinyint(1) DEFAULT NULL,
  `status` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_epoch_checked` int UNSIGNED DEFAULT NULL,
  `vc_name` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `protocol` varchar(100) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE `beacon_chain_validators_monitoring`
  ADD PRIMARY KEY (`beacon_chain_validator_monitoring_id`),
  ADD UNIQUE KEY `beacon_chain_validators_monitoring_network_public_key_unique` (`network`,`public_key`),
  ADD UNIQUE KEY `bcvmv_validator_index_network_unique` (`validator_index`,`network`);

ALTER TABLE `beacon_chain_validators_monitoring`
  MODIFY `beacon_chain_validator_monitoring_id` int UNSIGNED NOT NULL AUTO_INCREMENT;
COMMIT;
```

<br>

## Setup
Copy the `env.example` file to `.env` and set your custom variables.

Install the Node.js dependencies with `npm install`.

<br>

## Run
### Load validator public keys
Place one or more deposit data json files in the `deposits/` folder and import the public keys. The network will be auto detected.
```
node src/import_deposits.js deposits/<file name>
# Eg. node src/import_deposits.js deposits/deposit_data-1641143430.json
```

<br>

### Check validators
You can test if the script works properly with
```
node src/check_validators.js <network>
# Eg. node src/check_validators.js gnosis
```

Configure the Crontab to execute the check_validators.js script periodically. This example performs two checks per network per hour.
```
crontab -e
23,43 * * * * node /your/custom/path/beacon-chain-validator-alerting/src/check_validators.js mainnet  >> /your/custom/path/beacon-chain-validator-alerting/mainnet.log 2>&1
24,44 * * * * node /your/custom/path/beacon-chain-validator-alerting/src/check_validators.js gnosis  >> /your/custom/path/beacon-chain-validator-alerting/gnosis.log 2>&1
25,45 * * * * node /your/custom/path/beacon-chain-validator-alerting/src/check_validators.js prater  >> /your/custom/path/beacon-chain-validator-alerting/prater.log 2>&1
```

If you have many validators you may need to reduce the check period in order to not hit the Beaconchain API rate limit.

An alternative is to pay for the [Beaconchain Paid API](https://beaconcha.in/pricing).

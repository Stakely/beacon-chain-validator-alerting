const path = require('path')
require('dotenv').config({ path: path.join(__dirname, '../.env') })
const db = require('./db')

const fetchValidatorsByOperator = async(operatorId, network, page, itemsPerPage) => {
  let validators = [];
  let data;

  do {
    const response = await fetch(`https://api.ssv.network/api/v4/${network}/validators/in_operator/${operatorId}?page=${page}&perPage=${itemsPerPage}`);
    data = await response.json();
    if (data.validators && data.validators.length > 0) {
      // const formattedValidators = data.validators.map(validator => {
      //   return {
      //     public_key: validator.public_key,
      //     index: validator.validator_info.index,
      //     status: validator.validator_info.status,
      //   };
      // });
      validators.push(...data.validators);
    }
    page++;
  } while (data.pagination.pages >= page);

  return validators;
}

const updateDvtValidators = async (dvt, network, validators) => {
  let counter = {
    inserted: 0,
    updated: 0,
    deleted: 0
  };
  console.log('Validators obtained:', validators.length);
  // add new validators or update existing ones
  for (const validator of validators) {
    const existingValidator = await db.query('SELECT * FROM beacon_chain_validators_monitoring WHERE public_key = ? AND network = ? AND dvt_software = ?', [validator.public_key, network, dvt]);

    if (existingValidator.length === 0) {
      try {
        console.log('> Inserting new validator: ', validator.public_key);

        await db.query('INSERT INTO beacon_chain_validators_monitoring (public_key, network, dvt_software, vc_location, validator_share_ratio, protocol) VALUES(?,?,?,?,?,?)',
          [validator.public_key, network, dvt, validator.vc_location, validator.operatorPercentage,  validator.vc_location.includes('lido') ? 'lido' : 'ssv']);
        counter.inserted++;
      } catch (error) {
        console.log('Error inserting validator:', error);
        await db.query('UPDATE beacon_chain_validators_monitoring SET vc_location = ?, validator_share_ratio = ?, dvt_software = ? WHERE public_key = ? AND network = ?',
          [validator.vc_location, validator.operatorPercentage, dvt, validator.public_key, network]);
        counter.updated++;
      }

    } else {
      console.log('> Updating existing validator: ', validator.public_key);
      await db.query('UPDATE beacon_chain_validators_monitoring SET vc_location = ?, validator_share_ratio = ? WHERE public_key = ? AND network = ? AND dvt_software = ?',
        [validator.vc_location, validator.operatorPercentage, validator.public_key, network, dvt]);
      counter.updated++;
    }
  }

  // remove existing validators that are not in the new list
  const pubkeysArray = validators.map((validator) => validator.public_key);

  if (pubkeysArray.length > 0) {
    const placeholders = pubkeysArray.map(() => '?').join(', ');
    const deleteQuery = `
      DELETE FROM beacon_chain_validators_monitoring
      WHERE network = ?
        AND dvt_software = ?
        AND public_key NOT IN (${placeholders});
    `;
    const deleteResults = await db.query(deleteQuery, [network, dvt, ...pubkeysArray]);
    console.log('Deleted Rows affected:', deleteResults.affectedRows);
    counter.deleted = deleteResults.affectedRows;
  } else {
    console.log('No validators to process, skipping deletion of outdated records');
    // If no validators, we might want to delete all existing records for this network/dvt
    // Uncomment the following lines if you want to delete all existing records when no validators are found:
    // const deleteAllQuery = `DELETE FROM beacon_chain_validators_monitoring WHERE network = ? AND dvt_software = ?`;
    // const deleteResults = await db.query(deleteAllQuery, [network, dvt]);
    // console.log('Deleted all existing validators. Rows affected:', deleteResults.affectedRows);
    // counter.deleted = deleteResults.affectedRows;
  }

  console.log('=====================================');
  console.log('Inserted:', counter.inserted, 'Updated:', counter.updated, 'Deleted:', counter.deleted);
  console.log('=====================================');
}

const getSsvValidators = async (network) => {
  const ssvMapping = process.env.SSV_MAPPING;
  if (!ssvMapping || typeof ssvMapping !== 'string') {
    console.log('Please provide SSV_MAPPING in .env file');
    return;
  }

  const mappingJson = JSON.parse(ssvMapping);

  const OPERATOR_IDS = Object.keys(mappingJson).map(Number);
  if (!OPERATOR_IDS) {
    console.log('Please provide OPERATOR_IDS in .env file (comma separated)');
    return;
  }

  let result = [];

  for (const operatorId of OPERATOR_IDS) {
    const validators = await fetchValidatorsByOperator(operatorId, network, 1, 100);
    result.push(...validators);
  }

  result;
  const uniqueArray = removeDuplicates(result);

  const discardedValidators = [];
  const filteredValidators = uniqueArray.filter(validator => {
    const isValid = validator.is_deleted === false && validator.is_valid === true && validator.id_liquidated === false;
    if (!isValid) {
      discardedValidators.push(validator);
    }
    return isValid;
  });

  const finalResult = filteredValidators.map(validator => {
    const validatorWithMapping = calculateOperatorPercentage(validator, OPERATOR_IDS, mappingJson);
    return validatorWithMapping;
  });

  console.log('Discarded validators:', discardedValidators.length);
  console.log('Filtered validators:', filteredValidators.length);

  return finalResult;
}

function removeDuplicates(array) {
  return array.reduce((accumulator, current) => {
    const x = accumulator.find(item => item.public_key === current.public_key);
    if (!x) {
      return accumulator.concat([current]);
    } else {
      return accumulator;
    }
  }, []);
}

function calculateOperatorPercentage(validator, givenOperators, mappingJson) {
  const validatorOperators = new Set(validator.operators);
  const matchingOperators = givenOperators.filter(op => validatorOperators.has(op));

  const percentage = matchingOperators.length / validator.operators.length;
  let vc_location = '';
  matchingOperators.forEach((id, index) => {
    if (mappingJson[id]) {
        vc_location += `${mappingJson[id]} [${id}]`;

        // Add a comma and a space if it's not the last item
        if (index < matchingOperators.length - 1) {
            vc_location += process.env.SEPARATOR_CHAR || ', ';
        }
    }
  });

  return {
    ...validator,
    operatorPercentage: percentage,
    matchingOperators: matchingOperators,
    vc_location
  };
}

const getValidatorsAndUpdateDb = async (dvt, network) => {
  console.time('elapsed')
  console.log(new Date(), `Starting ${dvt} DVT get Validator and update network network`)

  if (!dvt || !network) {
    console.error('Please provide dvt and network')
    process.exit()
  }

  if (dvt === 'ssv' && network === 'mainnet') {
    const validators = await getSsvValidators(network)
    await updateDvtValidators(dvt, network, validators);
  } else{
    console.error('Please provide a valid dvt and network')
    process.exit()
  }

  console.log(new Date(), 'Finished')
  console.timeEnd('elapsed')
  process.exit()
}

// Usage: node src/dvt_validators_update.js <dvt> <network>
if (process.argv[2]) {
  getValidatorsAndUpdateDb(process.argv[2], process.argv[3])
} else {
  console.error('Example usage: node src/dvt_validators_update.js ssv')
};


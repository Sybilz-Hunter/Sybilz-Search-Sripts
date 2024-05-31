const fs = require('fs');
const csvParser = require('csv-parser');
const path = require('path');

const inputFilePath = '0x.csv'; // Replace with the path to your formatted file
const outputFilePath = 'filtered_output.csv'; // Output file

const startDateFirstTx = new Date('2023-05-01'); // Start date for FirstTx (inclusive)
const endDateFirstTx = new Date('2023-05-31'); // End date for FirstTx (inclusive)
const startDateLastTx = new Date('2024-02-21'); // Start date for LastTx (inclusive)
const endDateLastTx = new Date('2024-03-15'); // End date for LastTx (inclusive)
//Ethereum, Base, Arbitrum, Polygon, BNB Chain, Avalanche, Optimism, Gnosis, Celo Mainnet, Fantom, Moonbeam, Klaytn Mainnet Cypress
//const allowedChains = ['Ethereum', 'Base', 'Arbitrum', 'Polygon', 'BNB Chain', 'Avalanche', 'Optimism', 'Gnosis', 'Celo Mainnet', 'Fantom']; // Source chains to include in filtering
const allowedChainFromFirst = ['Avalanche']
const allowedChainToFirst = ['Polygon']
const allowedProtocolFirst = ['Stargate']
const allowedChainFromLast = ['Fantom']
const allowedChainToLast = ['Arbitrum']
const minNbTx = 10; // Minimum number of transactions
const maxNbTx = 350; // Maximum number of transactions
const allowedSybil = ['false', 'true']; // SYBIL values to include in filtering

const readStream = fs.createReadStream(inputFilePath);
const writeStream = fs.createWriteStream(outputFilePath);

// Write headers to the output file
const headers = [
  'ADDRESS',
  'NBTX',
  'SYBIL',
  'FIRST_SOURCE_CHAIN',
  'FIRST_DESTINATION_CHAIN',
  'FIRST_SOURCE_TIMESTAMP_UTC',
  'FIRST_PROJECT',
  'LAST_SOURCE_CHAIN',
  'LAST_DESTINATION_CHAIN',
  'LAST_SOURCE_TIMESTAMP_UTC',
  'LAST_PROJECT'
];
writeStream.write(headers.join(',') + '\n');

const logInterval = 10000; // Number of lines after which to display progress
let lineNumber = 0;

function isDateInRange(dateStr, startDate, endDate) {
  const date = new Date(dateStr.split(' ')[0]);
  return date >= startDate && date <= endDate;
}

function isChainAllowed(chain, allowance) {
  return allowance.includes(chain);
}

function isProtocolAllowed(protocol, allowance) {
  return allowance.includes(protocol);
}

function isNbTxInRange(nbTx) {
  return nbTx >= minNbTx && nbTx <= maxNbTx;
}

function isSybilAllowed(sybil) {
  return allowedSybil.includes(sybil);
}

readStream
  .pipe(csvParser())
  .on('data', (data) => {
    lineNumber++;
    if (lineNumber % logInterval === 0) {
      console.log(`Processed ${lineNumber} lines`);
    }

    const wallet = data['ADDRESS'];
    const firstTx = data['FIRST_SOURCE_TIMESTAMP_UTC'];
    const lastTx = data['LAST_SOURCE_TIMESTAMP_UTC'];
    const firstSourceChain = data['FIRST_SOURCE_CHAIN'];
    const firstDestinationChain = data['FIRST_DESTINATION_CHAIN'];
    const firstProtocol = data['FIRST_PROJECT'];
    const lastSourceChain = data['LAST_SOURCE_CHAIN'];
    const lastDestinationChain = data['LAST_DESTINATION_CHAIN'];
    const nbTx = parseInt(data['NBTX'], 10);
    const sybil = data['SYBIL'];

    if (
      wallet &&
      firstTx &&
      lastTx &&
      isDateInRange(firstTx, startDateFirstTx, endDateFirstTx) &&
      isDateInRange(lastTx, startDateLastTx, endDateLastTx) &&
      isChainAllowed(firstSourceChain, allowedChainFromFirst) &&
      isChainAllowed(firstDestinationChain, allowedChainToFirst) &&
      isProtocolAllowed(firstProtocol, allowedProtocolFirst) &&
      //isChainAllowed(lastSourceChain, allowedChainFromLast) &&
      //isChainAllowed(lastDestinationChain, allowedChainToLast) &&
      isNbTxInRange(nbTx) &&
      isSybilAllowed(sybil)
    ) {
      const outputLine = [
        wallet,
        nbTx,
        sybil,
        firstSourceChain,
        data['FIRST_DESTINATION_CHAIN'],
        firstTx,
        data['FIRST_PROJECT'],
        data['LAST_SOURCE_CHAIN'],
        data['LAST_DESTINATION_CHAIN'],
        lastTx,
        data['LAST_PROJECT']
      ].join(',') + '\n';

      writeStream.write(outputLine);
    }
  })
  .on('end', () => {
    console.log('Processing complete. Filtered data has been saved.');
    writeStream.end();
  })
  .on('error', (error) => {
    console.error(`Error reading file: ${error.message}`);
  });

process.on('exit', () => {
  writeStream.end();
});

process.on('SIGINT', () => {
  writeStream.end();
  process.exit();
});

process.on('uncaughtException', (err) => {
  console.error(err);
  writeStream.end();
  process.exit(1);
});
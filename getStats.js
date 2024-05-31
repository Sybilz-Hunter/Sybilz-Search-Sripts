const fs = require('fs');
const csvParser = require('csv-parser');
const path = require('path');

function readAddressesFromFile(filePath) {
  return new Promise((resolve, reject) => {
    const addresses = [];
    fs.createReadStream(filePath)
      .pipe(csvParser())
      .on('data', (row) => {
        addresses.push(row.address);
      })
      .on('end', () => {
        resolve(addresses);
      })
      .on('error', (error) => {
        reject(`Error reading file: ${error.message}`);
      });
  });
}

async function findAddressTransactions(address) {
  return new Promise((resolve, reject) => {
    let found = false;
    const inputFilePath = `sybil/${address.substr(2, 2)}.csv`;
    fs.createReadStream(inputFilePath)
      .pipe(csvParser({ headers: ['address'] }))
      .on('data', (data) => {
        if (data.address.toLowerCase() === address.toLowerCase()) {
          found = true;
          resolve(true);
        }
      })
      .on('end', () => {
        if (!found) {
          resolve(false);
        }
      })
      .on('error', (error) => {
        reject(`Error reading file: ${error.message}`);
      });
  });
}

const outputFilePath = 'user_statistics_output.csv'; // Output file for user statistics

const userTransactions = {}; // Store transactions by user

let usersProcessed = 0;
let totalUsers = 0;
let writeStream;

// Function to get the year and week number based on Monday
function getWeekNumber(date) {
  const day = date.getDay() || 7;
  if (day !== 1) {
    date.setHours(-24 * (day - 1));
  }
  const yearStart = new Date(date.getFullYear(), 0, 1);
  const weekNumber = Math.ceil((((date - yearStart) / 86400000) + 1) / 7);
  return `${date.getFullYear()}-W${weekNumber}`;
}

function processAddress(address) {
  return new Promise((resolve, reject) => {
    const prefix = address.slice(2, 6).toLowerCase(); // The first four letters after '0x'
    const inputFilePath = path.join(__dirname, 'fullDatabase', `${prefix}.csv`);

    if (!fs.existsSync(inputFilePath)) {
      console.log(`File not found for address prefix ${prefix}: ${inputFilePath}`);
      userTransactions[address.toLowerCase()] = []; // Initialize with an empty array if the file does not exist
      return resolve();
    }

    fs.createReadStream(inputFilePath)
      .pipe(csvParser())
      .on('data', (data) => {
        const wallet = data['SENDER_WALLET'];
        if (wallet.toLowerCase() === address.toLowerCase()) {
          if (!userTransactions[wallet]) {
            userTransactions[wallet] = [];
          }
          userTransactions[wallet].push({
            sourceTimestampUtc: new Date(data['SOURCE_TIMESTAMP_UTC']),
            sourceChain: data['SOURCE_CHAIN'],
            destinationChain: data['DESTINATION_CHAIN'],
            project: data['PROJECT'],
            nativeDropUsd: parseFloat(data['NATIVE_DROP_USD']) || 0,
            stargateSwapUsd: parseFloat(data['STARGATE_SWAP_USD']) || 0,
          });
        }
      })
      .on('end', () => {
        if (!userTransactions[address.toLowerCase()]) {
          userTransactions[address.toLowerCase()] = []; // Initialize with an empty array if no transactions are found
        }
        resolve();
      })
      .on('error', (error) => {
        reject(`Error reading file: ${error.message}`);
      });
  });
}

function calculateUserStatistics(transactions) {
  const nbTx = transactions.length;
  const nativeDropTotal = transactions.reduce((sum, tx) => sum + tx.nativeDropUsd, 0);
  const stargateSwapTotal = transactions.reduce((sum, tx) => sum + tx.stargateSwapUsd, 0);
  const uniqueSourceChains = new Set(transactions.map(tx => tx.sourceChain)).size;
  const uniqueDestinationChains = new Set(transactions.map(tx => tx.destinationChain)).size;
  const uniqueProjects = new Set(transactions.map(tx => tx.project)).size;

  const uniqueDays = new Set(transactions.map(tx => tx.sourceTimestampUtc.toISOString().split('T')[0])).size;
  const uniqueWeeks = new Set(transactions.map(tx => getWeekNumber(new Date(tx.sourceTimestampUtc)))).size;
  const uniqueMonths = new Set(transactions.map(tx => {
    const date = new Date(tx.sourceTimestampUtc);
    return `${date.getUTCFullYear()}-${date.getUTCMonth() + 1}`;
  })).size;

  const initialActiveDate = transactions[0]?.sourceTimestampUtc.toISOString().split('T')[0] || '';
  const lastActiveDate = transactions[transactions.length - 1]?.sourceTimestampUtc.toISOString().split('T')[0] || '';

  return {
    nbTx,
    nativeDropTotal,
    stargateSwapTotal,
    uniqueSourceChains,
    uniqueDestinationChains,
    uniqueProjects,
    uniqueDays,
    uniqueWeeks,
    uniqueMonths,
    initialActiveDate,
    lastActiveDate
  };
}

async function processAllAddresses() {
  const addressFilePath = 'addressToCheck.csv';
  const addresses = await readAddressesFromFile(addressFilePath);
  totalUsers = addresses.length;

  console.log(`Total users to process: ${totalUsers}`);

  for (const address of addresses) {
    await processAddress(address);
    usersProcessed++;
    console.log(`Processed ${usersProcessed} / ${totalUsers} users`);
  }

  writeStream = fs.createWriteStream(outputFilePath);
  writeStream.write('User Address,NB TX,NATIVE DROP,STARGATE SWAP,src | dest | contract,day | week | month,Initial Active Date,Last Active Date,isSybil\n');

  // Write user statistics
  for (const wallet of addresses) {
    const transactions = userTransactions[wallet.toLowerCase()] || [];
    transactions.sort((a, b) => a.sourceTimestampUtc - b.sourceTimestampUtc);

    const stats = calculateUserStatistics(transactions);
    const isSybil = await findAddressTransactions(wallet);

    const outputLine = [
      wallet,
      stats.nbTx,
      stats.nativeDropTotal.toFixed(2),
      stats.stargateSwapTotal.toFixed(2),
      `${stats.uniqueSourceChains} | ${stats.uniqueDestinationChains} | ${stats.uniqueProjects}`,
      `${stats.uniqueDays} | ${stats.uniqueWeeks} | ${stats.uniqueMonths}`,
      stats.initialActiveDate,
      stats.lastActiveDate,
      isSybil ? 'true' : 'false'
    ].join(',') + '\n';

    writeStream.write(outputLine);
  }

  console.log('User statistics calculation complete. Results have been saved.');
  writeStream.end();
}

processAllAddresses().catch(error => {
  console.error(error);
  if (writeStream) {
    writeStream.end();
  }
});

process.on('exit', () => {
  if (writeStream) {
    writeStream.end();
  }
});

process.on('SIGINT', () => {
  if (writeStream) {
    writeStream.end();
  }
  process.exit();
});

process.on('uncaughtException', (err) => {
  console.error(err);
  if (writeStream) {
    writeStream.end();
  }
  process.exit(1);
});

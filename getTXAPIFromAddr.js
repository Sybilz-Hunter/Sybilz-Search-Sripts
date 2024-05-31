
const axios = require('axios');
const fs = require('fs');
const csvParser = require('csv-parser');

// API configuration
const config = {
  "AVAX": {
    "API_KEY": "XXXXXXXX",
    "API_URL": "https://api.snowscan.xyz/"
  }
};

// Addresses to exclude from analysis
const excludedAddresses = new Set([
  '0x6a67d4a8ee91919a22444ef3784b3ecd94bc9b29'
]);

// Starting addresses for analysis
const startAddresses = [
  '0xb84c63e3598393a6d6c772fdf296757880501e2a'
];

// Maximum number of transactions to exclude an address
const maxTransactions = 100;
const maxLZ = 5;

// Function to get transactions of an address
async function getTransactions(address, chainID) {
  const { API_KEY, API_URL } = config[chainID];
  const url = `${API_URL}api?module=account&action=txlist&address=${address}&startblock=0&endblock=99999999&sort=asc&apikey=${API_KEY}`;
  try {
    const response = await axios.get(url);
    const data = response.data;

    if (data.status == '1') {
      const transactions = data.result.filter(tx => tx.isError === '0' && tx.methodId === '0x');
      if (transactions.length > maxTransactions) {
        console.log(`Too many transactions for address ${address} (${transactions.length}). Limit of ${maxTransactions} exceeded.`);
        excludedAddresses.add(address.toLowerCase());
        return [];
      }
      return transactions; // Return all successful transactions
    } else if (data.status === '0' && data.message !== 'No transactions found') {
      return null; // Indicates that a timeout is necessary
    } else {
      return [];
    }
  } catch (error) {
    console.error(`Error fetching transactions for address ${address}:`, error);
    return null;
  }
}

// Function to find transactions of an address in a local file
async function findAddressTransactions(address) {
  return new Promise((resolve, reject) => {
    let found = false;
    const inputFilePath = `database/databaseB/${address.substr(2, 2).toLowerCase()}.csv`;
    console.log(`Reading file: ${inputFilePath}`); // Added log to check the file being read
    fs.createReadStream(inputFilePath)
      .pipe(csvParser({ headers: ['address', 'transactionCount', 'firstTransaction', 'lastTransaction'] }))
      .on('data', (data) => {
        if (data.address.toLowerCase() == address.toLowerCase()) {
          console.log(`Address found in file: ${data.address}`); // Added log to check the data found
          found = true;
          resolve({
            transactionCount: data.transactionCount,
            firstTransaction: data.firstTransaction,
            lastTransaction: data.lastTransaction
          });
        }
      })
      .on('end', () => {
        if (!found) {
          console.log(`Address not found in file: ${address}`); // Added log for addresses not found
          resolve({
            transactionCount: '0',
            firstTransaction: null,
            lastTransaction: null
          });
        }
      })
      .on('error', (error) => {
        reject(`Error reading file: ${error.message}`);
      });
  });
}

// Function to check if an address is in the Sybil file
async function isAddressInSybil(address) {
  return new Promise((resolve, reject) => {
    let found = false;
    const inputFilePath = `sybil/${address.substr(2, 2)}.csv`;
    console.log(`Reading Sybil file: ${inputFilePath}`); // Added log to check the Sybil file being read
    fs.createReadStream(inputFilePath)
      .pipe(csvParser({ headers: ['address'] }))
      .on('data', (data) => {
        if (data.address.toLowerCase() == address.toLowerCase()) {
          console.log(`Address found in Sybil file: ${data.address}`); // Added log to check the data found
          found = true;
          resolve(true);
        }
      })
      .on('end', () => {
        if (!found) {
          console.log(`Address not found in Sybil file: ${address}`); // Added log for addresses not found
          resolve(false);
        }
      })
      .on('error', (error) => {
        reject(`Error reading file: ${error.message}`);
      });
  });
}

// Function to write a line to a CSV file
function writeLineToFile(filePath, line) {
  fs.appendFileSync(filePath, line + '\n', (err) => {
    if (err) {
      console.error('Error writing to file', err);
    }
  });
}

// Function to introduce a delay
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Function to analyze an address and its outgoing transfers recursively
async function analyzeAddress(address, chainID, analyzedAddresses, outputFilePath) {
  while (true) {
    if (excludedAddresses.has(address.toLowerCase()) || analyzedAddresses.has(address.toLowerCase())) {
      console.log(`Address excluded or already analyzed: ${address}`);
      return;
    }
    console.log(`Analyzing address: ${address}`);
    
    const transactions = await getTransactions(address, chainID);
    if (transactions === null) {
      console.log(`Timeout necessary for address ${address}. Waiting for 1 second.`);
      await sleep(1000); // One-second delay
      continue; // Retry the same address
    }
    
    analyzedAddresses.add(address.toLowerCase());

    const outTransactions = transactions.filter(tx => tx.from.toLowerCase() === address.toLowerCase());
    console.log(`Number of outgoing transactions for ${address}: ${outTransactions.length}`);

    const addressInfo = await findAddressTransactions(address);
    console.log(`Info for address ${address}: Transactions - ${addressInfo.transactionCount}, First - ${addressInfo.firstTransaction}, Last - ${addressInfo.lastTransaction}`);

    if (transactions.length > maxTransactions) {
      console.log(`Address ${address} has more than ${maxTransactions} transactions returned by the API. Adding to excludedAddresses.`);
      excludedAddresses.add(address.toLowerCase());
    }

    if (parseInt(addressInfo.transactionCount) > maxLZ) {
      const isSybil = await isAddressInSybil(address);
      console.log(`Is address ${address} Sybil? ${isSybil}`);
      writeLineToFile(outputFilePath, `${address},${addressInfo.transactionCount},${isSybil}`);
    }

    for (const tx of outTransactions) {
      await analyzeAddress(tx.to, chainID, analyzedAddresses, outputFilePath);
    }

    break; // Exit the while loop after successful analysis
  }
}

// Main function
(async () => {
  const chainID = "AVAX"; // Use AVAX as an example
  const outputFilePath = 'analyzed_addresses.csv';
  
  // Write the header to the file
  const header = 'Address,TransactionCount,IsSybil';
  writeLineToFile(outputFilePath, header);

  const analyzedAddresses = new Set();
  for (const startAddress of startAddresses) {
    await analyzeAddress(startAddress, chainID, analyzedAddresses, outputFilePath);
  }

  console.log('Analysis complete. Results saved in analyzed_addresses.csv');
})();
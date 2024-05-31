const axios = require('axios');
const fs = require('fs');
const csv = require('csv-parser');

// JSON containing configuration parameters
const config = {
  "ETH": {
    "API_KEY": "XXXXXXX",
    "API_URL": "https://api.etherscan.io/"
  },
  "FTM": {
    "API_KEY": "XXXXXXX",
    "API_URL": "https://api.ftmscan.com/"
  },
  "ARB": {
    "API_KEY": "XXXXXXX",
    "API_URL": "https://api.arbiscan.io/"
  },
  "AVAX": {
    "API_KEY": "XXXXXXX",
    "API_URL": "https://api.snowscan.xyz/"
  }
  
  // Add other chains and their corresponding parameters here
};

// Function to get transactions of an address
async function getTransactions(address, chainID) {
  const { API_KEY, API_URL } = config[chainID];
  const url = `${API_URL}api?module=account&action=txlist&address=${address}&startblock=0&endblock=99999999&sort=asc&apikey=${API_KEY}`;
  try {
    const response = await axios.get(url);
    const data = response.data;

    if (data.status === '1') {
      // Filter transactions to keep only those of type "Transfer"
      //console.log(data.result)
      return data.result.filter(tx => tx.isError === '0' && (tx.methodId === "0x" || tx.functionName.startsWith('Transfer'))).slice(0, 10);
    } else if (data.status === '0' && data.message != 'No transactions found') {
      return null; // Indicates that a timeout is necessary
    } else {
      return [];
    }
  } catch (error) {
    console.error(`Error fetching transactions for address ${address}:`, error);
    return null;
  }
}

// Function to convert timestamp to date
function convertTimestampToDate(timestamp) {
  return new Date(timestamp * 1000).toISOString();
}

// Function to read addresses from a CSV file
async function readAddressesFromFile(filePath) {
  return new Promise((resolve, reject) => {
    const addresses = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => addresses.push(row.address))
      .on('end', () => resolve(addresses))
      .on('error', (error) => reject(error));
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

// Main function
(async () => {
  const chainID = "AVAX"; // Example usage for AVAX (Avalanche)
  const addresses = await readAddressesFromFile('addressToCheck.csv');
  const totalAddresses = addresses.length;
  const outputFilePath = `first_transactions${chainID}.csv`;

  // Write the header to the file
  const header = 'Address,TransactionCount,'
               + 'Tx1_Method,Tx1_Date,Tx1_Partner,Tx2_Method,Tx2_Date,Tx2_Partner,'
               + 'Tx3_Method,Tx3_Date,Tx3_Partner,Tx4_Method,Tx4_Date,Tx4_Partner,'
               + 'Tx5_Method,Tx5_Date,Tx5_Partner,Tx6_Method,Tx6_Date,Tx6_Partner,'
               + 'Tx7_Method,Tx7_Date,Tx7_Partner,Tx8_Method,Tx8_Date,Tx8_Partner,'
               + 'Tx9_Method,Tx9_Date,Tx9_Partner,Tx10_Method,Tx10_Date,Tx10_Partner';
  writeLineToFile(outputFilePath, header);

  for (let i = 0; i < totalAddresses; i++) {
    const address = addresses[i];
    let transactions = await getTransactions(address, chainID);

    // If the request failed, delay and retry
    if (transactions === null) {
      await sleep(1000);
      i--; // Decrement i to retry the same address
      continue; // Move to the next iteration to retry the request
    }

    const result = [address, transactions.length];

    for (let j = 0; j < 10; j++) {
      if (transactions && transactions[j]) {
        const methodName = 'Transfer';
        const date = convertTimestampToDate(transactions[j].timeStamp);
        const partnerAddress = (transactions[j].from.toLowerCase() === address.toLowerCase()) ? transactions[j].to : transactions[j].from;
        result.push(methodName, date, partnerAddress);
      } else {
        // Add empty fields if less than 10 transactions
        result.push('', '', '');
      }
    }

    writeLineToFile(outputFilePath, result.join(','));
    console.log(`Progress: ${i + 1}/${totalAddresses}`);
  }

  console.log('Processing complete. Filtered data has been saved in first_transactions.csv');
})();
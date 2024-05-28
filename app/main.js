const net = require("net");
const PORT = process.argv[2] === "--port" ? process.argv[3] : 6379;
const fs = require("fs");
let server_info = {
  role: "master",
  master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
  master_repl_offset: "0",
};

let dir = process.argv[2] === "--dir" ? process.argv[3] : "";
let dbfilename = process.argv[4] === "--dbfilename" ? process.argv[5] : "";

const replicaList = [];
let offset = 0;
let ack_received = 0; // Total acks received by master from replica when getack is passed
let ack_needed = 0; // Acks need to be received.
let reply_wait = false;
let propogated_commands = 0;
let bytecount = 0;
let empty_rdb =
  "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

const pendingWaitCommands = [];
const db = {};

//establishes a conenction with replica
const handleHandshake = (host, port) => {
  const hsclient = net.createConnection({ host: host, port: port }, () => {
    console.log("connected to master", "Host: ", host, "Port: ", port);
    hsclient.write("*1\r\n$4\r\nPING\r\n");

    let repl1 = false;

    hsclient.on("data", (data) => {
      let commands = Buffer.from(data).toString().split("\r\n");
      console.log(`Command recieved by replica:`, commands);
      let queries = data.toString();
      while (queries.length > 0) {
        let index = queries.indexOf("*", 1);
        let query;
        if (index == -1) {
          query = queries;
          queries = "";
        } else {
          query = queries.substring(0, index);
          queries = queries.substring(index);
        }
        console.log(`Query formed:`, query);

        commands = Buffer.from(query).toString().split("\r\n");

        if (commands[0] == "+PONG") {
          hsclient.write(
            `*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n${PORT}\r\n`
          );
        } else if (commands[2] == "PING") {
          bytecount += 14;
        } else if (commands[0] == "+OK") {
          if (repl1 == false) {
            hsclient.write(
              `*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n`
            );
            repl1 = true;
          } else hsclient.write(`*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n`);
        } else if (commands[2] == "SET") {
          bytecount += query.length;
          const key = commands[4];
          const value = commands[6];
          db[key] = value;

          if (commands[8] == "px")
            setTimeout(() => {
              delete db[key];
            }, commands[10]);

          //   hsclient.write(
          //     `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$${
          //       bytecount.toString().length
          //     }\r\n${bytecount}\r\n`
          //   );
          //   bytecount += query.length + 3;
        } else if (commands[2] == "GET") {
          const answer = db[commands[4]];
          if (answer) {
            const l = answer.length;
            hsclient.write("$" + l + "\r\n" + answer + "\r\n");
          } else {
            hsclient.write("$-1\r\n");
          }
        } else if (commands[2] == "REPLCONF") {
          if (commands[4] == "GETACK") {
            hsclient.write(
              `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$${
                bytecount.toString().length
              }\r\n${bytecount}\r\n`
            );

            bytecount += query.length + 3;
            //+3 because when query is partitioned then if forms the query as "*3 replconf getack" instead of "*3 replconf getack *"
          }
        }
      }
    });
  });
};

//Propagates write commands to replicas
const propagateToReplicas = (command) => {
  if (server_info.role != "master" || replicaList.length == 0) return;

  for (const replicaCon of replicaList) {
    replicaCon.write(command);
    replicaCon.once("data", (data) => {
      const commands = Buffer.from(data).toString().split("\r\n");
      if (commands[4] == "ACK") {
        ack_received++;
        console.log(`ACK  Recieved`, commands);
      }
    });
  }
  propogated_commands++;
};

//Wait command implementation
const wait = (args, connection) => {
  // Parse arguments and reset acknowledgment tracking
  const noOfReplica = parseInt(args[0]);
  const delay = parseInt(args[1]);
  ack_received = 0;
  ack_needed = noOfReplica;
  reply_wait = false;

  // If no commands need propagation, reply immediately
  if (propogated_commands === 0) {
    reply_wait = true;
    connection.write(`:${replicaList.size}\r\n`);
  } else {
    // Request acknowledgment status from replicas
    propagateToReplicas("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
  }

  // Set a timeout to send a reply if the required acknowledgments aren't received
  setTimeout(() => {
    if (!reply_wait)
      connection.write(
        `:${ack_received > noOfReplica ? noOfReplica : ack_received}\r\n`
      );
  }, delay);
};

//Reads RDB File
const readRdbFile = () => {
  const opCodes = {
    resizeDb: "fb",
  };

  let i = 0;
  const dirName = dir;
  const fileName = dbfilename;
  const filePath = dirName + "/" + fileName;
  console.log(`DIr: ${dirName} ,Filenamde :${fileName}`);
  console.log(`Path`, filePath);
  let dataBuffer;
  console.log("Hex data:", dataBuffer.toString("hex"));
  try {
    dataBuffer = fs.readFileSync(filePath);
  } catch (e) {
    console.log("Error:", e);
    return;
  }

  const getNextNBytes = (n) => {
    let nextNBytes = Buffer.alloc(n);
    for (let k = 0; k < n; k++) {
      nextNBytes[k] = dataBuffer[i];
      i++;
    }
    return nextNBytes;
  };

  const getNextObjLength = () => {
    const firstByte = dataBuffer[i];
    const twoBits = firstByte >> 6;
    let length = 0;
    switch (twoBits) {
      case 0b00:
        length = firstByte ^ 0b00000000;
        i++;
        break;
    }
    return length;
  };

  const hashTable = () => {
    const nextObjLength = getNextObjLength();
    const nextNBytes = getNextNBytes(nextObjLength);
  };
  const expiryHashTable = () => {
    const nextObjLength = getNextObjLength();
    const nextNBytes = getNextNBytes(nextObjLength);
  };

  const getKeyValues = (n) => {
    for (let j = 0; j < n; j++) {
      const keyLength = getNextObjLength();
      const key = getNextNBytes(keyLength).toString();
      const valueLength = getNextObjLength();
      const value = getNextNBytes(valueLength).toString();
      console.log(`Setting ${key} to ${value}`);
      db[key] = value;
      i++; // 00 padding.
    }
  };

  const resizeDb = () => {
    // console.log("Inside resizedb");
    i++;
    // hashTable();
    // expiryHashTable();
    // const keyLength = getNextObjLength();
    // const key = getNextNBytes(keyLength);
    // const valueLength = getNextObjLength();
    // const value = getNextNBytes(valueLength);
    // console.log("Key:", key.toString(), "value:", value.toString());
    // db[key] = value;
    const totalKeyVal = getNextObjLength();
    const totalExpiry = getNextObjLength();
    i++; // There is 00 padding.
    getKeyValues(totalKeyVal);
  };

  while (i < dataBuffer.length) {
    const currentHexByte = dataBuffer[i].toString(16);
    if (currentHexByte === opCodes.resizeDb) resizeDb();
    i++;
  }
};

if (process.argv[4] == "--replicaof") {
  server_info.role = "slave";
  let replicaofArray = process.argv[5].split(" ");
  let masterhost = replicaofArray[0];
  let masterport = replicaofArray[1];

  if (masterhost && masterport) {
    handleHandshake(masterhost, masterport);
  }
}
if (process.argv[2] == "--dir" && process.argv[4] == "--dbfilename") {
  readRdbFile();
}

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
const server = net.createServer((connection) => {
  // Handle connection

  connection.on("data", (data) => {
    const commands = Buffer.from(data).toString().split("\r\n");
    // *2\r\n $5 \r\n ECHO \r\n $3 \r\n hey \r\n
    console.log(`Command:`, commands);
    if (commands.includes("PING")) {
      return connection.write("+PONG\r\n");
    } else if (commands[2] == "ECHO") {
      const str = commands[4];
      const l = str.length;
      return connection.write("$" + l + "\r\n" + str + "\r\n");
    } else if (commands[2] == "SET") {
      const key = commands[4];
      const value = commands[6];
      db[key] = value;

      if (commands[8] == "px")
        setTimeout(() => {
          delete db[key];
        }, commands[10]);

      propagateToReplicas(Buffer.from(data).toString());
      connection.write("+OK\r\n");
    } else if (commands[2] == "GET") {
      const answer = db[commands[4]];
      if (answer) {
        const l = answer.length;
        return connection.write("$" + l + "\r\n" + answer + "\r\n");
      } else {
        return connection.write("$-1\r\n");
      }
    } else if (commands[2] == "INFO") {
      let response = "";
      for (let key in server_info) {
        response += `${key}:${server_info[key]},`;
      }

      return connection.write(
        `$` + `${response.length}\r\n` + response + `\r\n`
      );
    } else if (commands[2] == "REPLCONF") {
      if (commands.includes("listening-port")) {
        return connection.write(`+OK\r\n`);
      } else {
        return connection.write(`+OK\r\n`);
      }
    } else if (commands[2] == "PSYNC") {
      if (commands[4] == "?" && commands[6] == "-1") {
        connection.write(`+FULLRESYNC ${server_info.master_replid} 0\r\n`);

        //Empty RBD send to replica
        const bufferRDB = Buffer.from(empty_rdb, "base64");
        const res = Buffer.concat([
          Buffer.from(`$${bufferRDB.length}\r\n`),
          bufferRDB,
        ]);
        console.log(res);
        connection.write(res);
        replicaList.push(connection);
      }
    } else if (commands[2] == "WAIT") {
      //   return connection.write(`:${replicaList.length}\r\n`);
      let args = [commands[4], commands[6]];
      wait(args, connection);
    } else if (commands[2] == "CONFIG") {
      if (commands[4] == "GET") {
        let command = commands[6];
        let response;
        switch (command) {
          case "dir":
            response = `*2\r\n$3\r\ndir\r\n$${dir.length}\r\n${dir}\r\n`;
            break;
          case "dbfilename":
            response = `*2\r\n$3\r\ndbfilename\r\n$${dbfilename.length}\r\n${dbfilename}\r\n`;
            break;
        }

        return connection.write(response);
      }
    } else if (commands[2] == "KEYS") {
      const keys = Object.keys(db);
      let response = "";
      for (let key of keys) {
        response += `$${key.length}\r\n${key}\r\n`;
      }
      connection.write(`*${keys.length}\r\n` + response);
    }
  });
});

server.listen(PORT, "127.0.0.1");

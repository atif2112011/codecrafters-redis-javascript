const net = require("net");
const PORT = process.argv[2] === "--port" ? process.argv[3] : 6379;

let server_info = {
  role: "master",
  master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
  master_repl_offset: "0",
};

const replicaList = [];
let bytecount = 0;
let empty_rdb =
  "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

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

            bytecount += query.length;
          }
        }
      }
    });
  });
};

const propagateToReplicas = (command) => {
  if (server_info.role != "master" || replicaList.length == 0) return;

  for (const replicaCon of replicaList) {
    replicaCon.write(command);
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

const db = {};
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

      connection.write("+OK\r\n");
      propagateToReplicas(data);
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
    }
  });
});

server.listen(PORT, "127.0.0.1");

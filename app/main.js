const net = require("net");
const PORT = process.argv[2] === "--port" ? process.argv[3] : 6379;

let server_info = {
  role: "master",
  master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
  master_repl_offset: "0",
};

const handleHandshake = (host, port) => {
  const hsclient = net.createConnection({ host: host, port: port }, () => {
    console.log("connected to master", "Host: ", host, "Port: ", port);
    hsclient.write("*1\r\n$4\r\nPING\r\n");

    hsclient.on("data", (data) => {
      const commands = Buffer.from(data).toString().split("\r\n");

      if (commands[0] == "+PONG") {
        hsclient.write(
          `*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n${port}\r\n`
        );
        hsclient.write(
          `*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n`
        );
      }
    });
  });
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

    if (commands[2] == "ECHO") {
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

      return connection.write("+OK\r\n");
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
    }

    connection.write("+PONG\r\n");
  });
});

server.listen(PORT, "127.0.0.1");

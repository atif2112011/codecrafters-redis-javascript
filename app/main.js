const net = require("net");

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

      return connection.write("+OK\r\n");
    } else if (commands[2] == "GET") {
      const answer = db[commands[4]];
      const l = answer.length;
      return connection.write("$" + l + "\r\n" + answer + "\r\n");
    }

    connection.write("+PONG\r\n");
  });
});

server.listen(6379, "127.0.0.1");

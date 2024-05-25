const net = require("net");

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
const server = net.createServer((connection) => {
  // Handle connection

  connection.on("data", (data) => {
    const commands = Buffer.from(data).toString().split("\r\n");
    // *2\r\n $5 \r\n ECHO \r\n $3 \r\n hey \r\n

    if (commands[2] == "ECHO") {
      const str = commands[3];
      const l = str.length;
      connection.write("$" + l + "\r\n" + str + "\r\n");
    }
  });
});

server.listen(6379, "127.0.0.1");

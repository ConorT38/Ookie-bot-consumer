const amqp = require("amqplib");
const mysql = require("mysql2/promise");

const queue = "sitesQueue";

// Set your config here...
let config = {
  protocol: "amqp",
  hostname: "192.168.0.22",
  port: 5672,
  username: "root",
  password: "Ae27!6CdJc1_thEQ9",
  locale: "en_US",
  frameMax: 0,
  heartbeat: 0,
  vhost: "/",
};

async function start() {
  try {
    const conn = await createConnection(config);
    console.log("Connected to AMQP server.");

    const dbconn = await mysql.createConnection({
      host: "192.168.0.21",
      user: "root",
      password: "raspberry",
      database: "ookie",
    });

    let channel = await conn.createChannel();
    await channel.assertQueue(queue, { durable: false });

    startPollingForMessages(channel, dbconn);
  } catch (err) {
    console.error("start: Connection error:", err.message);
  }
}

async function createConnection(config) {
  const conn = await amqp.connect(config);

  conn.on("error", function (err) {
    console.error("Connection error:", err.message);
  });

  conn.on("close", function () {
    console.error("Connection closed:", err.message);
  });

  return conn;
}

function startSendingMessages(channel) {
  const SEND_INTERVAL = 5000;
  setInterval(() => {
    sendMessage(
      channel,
      queue,
      JSON.stringify({
        timestamp: new Date().toISOString(),
        message: " Some message",
      })
    );
  }, SEND_INTERVAL);
}

async function sendMessage(channel, queue, messageContent) {
  console.log(`sendMessage: sending message: ${messageContent}...`);
  return channel.sendToQueue(queue, Buffer.from(messageContent));
}

function startPollingForMessages(ch, dbconn) {
  ch.consume(queue, (msg) => {
    onNewMessage(msg, dbconn);
    ch.ack(msg);
  });
}

async function onNewMessage(msg, dbconn) {
  const siteInfo = JSON.parse(msg.content.toString());

  let [rows, fields] = await dbconn.execute(
    "INSERT INTO sites (title, url) VALUES (?, ?) " +
      "ON DUPLICATE KEY UPDATE title = VALUES(title), url = VALUES(url)",
    [siteInfo["title"], siteInfo["url"]]
  );
  console.log("[INSERT] -- "+siteInfo["title"]);
  return rows;
}

start();

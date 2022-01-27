const amqp = require("amqplib");
const mysql = require("mysql2/promise");
require("dotenv").config();

const queue = "sitesQueue";

// Set your config here...
let amqpConfig = {
  protocol: process.env.AMQP_PROTOCOL,
  hostname: process.env.AMQP_HOST,
  port: process.env.AMQP_PORT,
  username: process.env.AMQP_USERNAME,
  password: process.env.AMQP_PASSWORD,
  locale: process.env.AMQP_LOCALE,
  frameMax: process.env.AMQP_FRAME_MAX,
  heartbeat: process.env.AMQP_HEARTBEAT,
  vhost: process.env.AMQP_VHOST,
};

let dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
};

async function start() {
  try {
    const conn = await createConnection(config);
    console.log("Connected to AMQP server.");

    const dbconn = await createDatabaseConnection();

    let channel = await conn.createChannel();
    await channel.assertQueue(queue, { durable: false });

    startPollingForMessages(channel, dbconn);
  } catch (err) {
    console.error("start: Connection error:", err);
    return await start();
  }
}

async function createDatabaseConnection() {
  return await mysql.createConnection(dbConfig);
}
async function createConnection(config) {
  const conn = await amqp.connect(config);

  conn.on("error", function (err) {
    console.error("Connection error:", err);
  });

  conn.on("close", async function () {
    console.error("Connection closed:", err);
    return await createConnection(config);
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
  if (
    siteInfo["url"] == null ||
    siteInfo["url"].length == 0 ||
    siteInfo["url"].length >= 255 ||
    siteInfo["title"] == null ||
    siteInfo["title"].length == 0 ||
    siteInfo["title"].length >= 255
  ) {
    return;
  }

  await saveSiteInfo(siteInfo, dbconn);
}

async function saveStemmedWords(msgJson, dbconn) {
  for (const word of msgJson["words"]) {
    // stemmedWord = word.stem();
    //
  }
}

async function saveSiteInfo(msgJson, dbconn) {
  try {
    let [rows, fields] = await dbconn.execute(
      "INSERT INTO sites (title, url) VALUES (?, ?) " +
        "ON DUPLICATE KEY UPDATE title = VALUES(title), url = VALUES(url)",
      [msgJson["title"], msgJson["url"]]
    );
    console.log("[INSERT] -- " + msgJson["title"]);
    return rows;
  } catch (err) {
    console.log(err);
    dbconn.close();
    const dbconn = await createDatabaseConnection();
    await onNewMessage(msg, dbconn);
  }
}

start();

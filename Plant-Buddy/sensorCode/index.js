require("dotenv").config();
const express = require("express");

// MQTT
const mqtt = require("mqtt");
const mqttClientId = "node-arthur";
const mqttclient = mqtt.connect(process.env.MQTT_HOST, {
  clientId: mqttClientId,
});
const topics = [
  "herbert/sensor/lichtintensitaet",
  "herbert/sensor/wasserstand",
  "herbert/sensor/feuchtigkeit",
  "herbert/sensor/lichtstatus",
];

//cors Problem
const cors = require("cors");

let dbReady = false;
let sensorDataHerbsCollection;

// DB
const { MongoClient, ServerApiVersion } = require("mongodb");
const uri = `mongodb+srv://${process.env.DB_USER}:${process.env.DB_PASS}@cluster0.jl57uj6.mongodb.net/Herbert?retryWrites=true&w=majority`;

// const uuid = require("uuid");
// const mqttId = "sensorvalueservice-" + uuid.v4();

mqttclient.on("connect", function () {
  console.log("Connected to MQTT broker!");
  mqttclient.subscribe(topics, function (err) {
    if (err) {
      console.error("Error subscribing to selected topic:", err);
    } else {
      console.log(`Subscribed to ${topics} topic!`);
    }
  });
});

mqttclient.on("message", function (topic, message) {
  const msg = {
    createdAt: new Date(),
  };

  if (topic === "herbert/sensor/lichtintensitaet") {
    msg.lightIntensity = JSON.parse(message.toString());
    lightIntensityCollection
      .insertOne(msg)
      .then(() => {
        console.log("Inserted message into 'lightIntensity' collection:", msg);
      })
      .catch((error) => {
        console.error(
          "Error inserting message into 'lightIntensity' collection:",
          error
        );
      });
  }

  if (topic === "herbert/sensor/wasserstand") {
    msg.waterLevel = parseFloat(message.toString());
    waterLevelCollection
      .insertOne(msg)
      .then(() => {
        console.log("Inserted message into 'waterLevel' collection:", msg);
      })
      .catch((error) => {
        console.error(
          "Error inserting message into 'waterLevel' collection:",
          error
        );
      });
  }

  if (topic === "herbert/sensor/feuchtigkeit") {
    msg.humidity = JSON.parse(message.toString());
    humidityCollection
      .insertOne(msg)
      .then(() => {
        console.log("Inserted message into 'humidity' collection:", msg);
      })
      .catch((error) => {
        console.error(
          "Error inserting message into 'humidity' collection:",
          error
        );
      });
  }

  if (topic === "herbert/sensor/lichtstatus") {
    msg.uvLightStatus = JSON.parse(message.toString());
    uvLightStatusCollection
      .insertOne(msg)
      .then(() => {
        console.log("Inserted message into 'uvLightStatus' collection:", msg);
      })
      .catch((error) => {
        console.error(
          "Error inserting message into 'uvLightStatus' collection:",
          error
        );
      });
  }
});

// DB
// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const dbClient = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  },
});

// DB
async function dbRun() {
  await dbClient.connect();
  const db = await dbClient.db("Herbert");

  // Create "humidity" collection
  humidityCollection = await db.createCollection("humidity");
  console.log("Created collection 'humidity'");

  // Create "waterLevel" collection
  waterLevelCollection = await db.createCollection("waterLevel");
  console.log("Created collection 'waterLevel'");

  // Create "lightIntensity" collection
  lightIntensityCollection = await db.createCollection("lightIntensity");
  console.log("Created collection 'lightIntensity'");

  // Create "uvLightStatus" collection
  uvLightStatusCollection = await db.createCollection("uvLightStatus");
  console.log("Created collection 'uvLightStatus'");

  console.log("Connected successfully to MongoDB");
  dbReady = true;
}

dbRun().catch(console.dir);

/***********
 * API
 * ********/

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());
const port = process.env.API_PORT;

app.listen(port, () => {
  console.log(`userService running on port ${port}`);
});

app.get("/", (req, res) => {
  res.status(200).send("Welcome 2!");
});

app.get("/api/light_intensity", async (req, res) => {
  const message = await getLightIntensity();
  res.status(200).send(message);
  console.log(message);
});

app.get("/api/water_level", async (req, res) => {
  const message = await getWaterLevel();
  res.status(200).send(message);
  console.log(message);
});

app.get("/api/humidity", async (req, res) => {
  const message = await getHumidity();
  res.status(200).send(message);
  console.log(message);
});

app.get("/api/uv_light_status", async (req, res) => {
  const message = await getUvLightStatus();
  res.status(200).send(message);
  console.log(message);
});

async function getLightIntensity() {
  // get the last message from the sensorDataHerbs collection by createdAt
  const lightIntensity = await lightIntensityCollection
    .find()
    .sort({ createdAt: -1 })
    .limit(1)
    .toArray();
  return lightIntensity;
}

async function getWaterLevel() {
  // get the last message from the sensorDataHerbs collection by createdAt
  const waterLevel = await waterLevelCollection
    .find()
    .sort({ createdAt: -1 })
    .limit(1)
    .toArray();
  return waterLevel;
}

async function getHumidity() {
  // get the last message from the sensorDataHerbs collection by createdAt
  const humidity = await humidityCollection
    .find()
    .sort({ createdAt: -1 })
    .limit(1)
    .toArray();
  return humidity;
}

async function getUvLightStatus() {
  // get the last message from the sensorDataHerbs collection by createdAt
  const uvLightStatus = await uvLightStatusCollection
    .find()
    .sort({ createdAt: -1 })
    .limit(1)
    .toArray();
  return uvLightStatus;
}

require("dotenv").config();
const bcrypt = require("bcryptjs");
const pepper = process.env.PEPPER;

try {
  checkEnvVars();
} catch (error) {
  console.log(error);
  process.exit(1);
}

function checkEnvVars() {
  const requiredEnvVars = [
    "DB_USER",
    "DB_PASSWORD",
    "DB_HOST",
    "DB_DATABASE",
    "API_PORT",
  ];

  const unsetEnvVars = requiredEnvVars.filter(
    (envVar) => !(envVar in process.env)
  );

  if (unsetEnvVars.length > 0) {
    console.error(
      `Fehler: Die folgenden Umgebungsvariablen sind nicht gesetzt: ${unsetEnvVars.join(
        ", "
      )}`
    );
    throw new Error("Umgebungsvariable nicht gesetzt.");
  }
}

const express = require("express");
const cors = require("cors");

const { MongoClient, ServerApiVersion } = require("mongodb");
const dbUri = `mongodb+srv://${process.env.DB_USER}:${process.env.DB_PASSWORD}@${process.env.DB_HOST}?retryWrites=true&w=majority`;
// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const dbClient = new MongoClient(dbUri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  },
});
let db, userCollection;
async function dbRun() {
  console.log("Inside dbRun");
  await dbClient.connect();
  db = await dbClient.db(process.env.DB_DATABASE);
  userCollection = await db.collection("user");
  console.log("connected to db");
}
dbRun().catch(console.dir);

async function deleteAllDocuments(collection) {
  const result = await collection.deleteMany({});
  console.log(`${result.deletedCount} Dokumente wurden gelöscht.`);
}

/*********
 * API
 *********/

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());
const port = process.env.API_PORT;

app.listen(port, () => {
  console.log(`userService running on port ${port}`);
});

app.get("/", (req, res) => {
  res.status(200).send("Welcome to the peaceful DP2 user service API!");
});

app.post("/api/register", async (req, res) => {
  if (userCollection) {
    const username = req.body.username;
    const email = req.body.email;

    const existingUser = await userCollection.findOne({ username: username });
    if (!existingUser) {
      const password = req.body.password;
      console.log(`Füge User ${username} zur Datenbank hinzu...`);
      let user = {};
      user.username = username;
      user.email = email;

      let hashedPassword;

      try {
        const saltRounds = 10;
        const salt = await bcrypt.genSalt(saltRounds);
        hashedPassword = await bcrypt.hash(password + pepper, salt);
        user.password = hashedPassword;
      } catch (error) {
        console.log("password couldn't be hashed.");
        res.status(500).send("password couldn't be hashed");
      }
      user.createdAt = new Date();
      user.modifiedAt = new Date();
      try {
        await saveDocument(user);
        res.status(200).send("Registration successful");
      } catch (error) {
        console.log(error);
        res.status(500).send("Error while saving userdata to database");
      }
    } else {
      console.log("username already taken.");
      res.status(409).send("username already taken.");
    }
  } else {
    res.status(500).send("Error connecting to database.");
  }
});

app.post("/api/login", async (req, res) => {
  if (userCollection) {
    const email = req.body.email;
    const password = req.body.password;

    const user = await userCollection.findOne({ email: email });
    if (user) {
      // Überprüfen, ob das Passwort korrekt ist
      const isValidPassword = await bcrypt.compare(
        password + pepper,
        user.password
      );

      if (isValidPassword) {
        res.status(200).send("Login successful");
        // router.push("/homescreen");
      } else {
        console.log("Invalid password.");
        res.status(401).send("Invalid password.");
      }
    } else {
      console.log("No user found with that username.");
      res.status(404).send("No user found with that username.");
    }
  } else {
    res.status(500).send("Error connecting to database.");
  }
});

async function saveDocument(msg) {
  msg.createdAt = new Date();
  try {
    await userCollection.insertOne(msg);
  } catch (error) {
    throw error;
  }
}

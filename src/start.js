import fs from "fs";
import path from "path";

const dotenvPath = path.resolve(process.cwd(), ".env");
const nodeEnv = (process.env.NODE_ENV ?? "").toLowerCase();

const shouldLoadDotenv =
  process.env.LOAD_DOTENV === "true" ||
  (process.env.LOAD_DOTENV !== "false" &&
    nodeEnv !== "production" &&
    fs.existsSync(dotenvPath));

if (shouldLoadDotenv) {
  const dotenv = await import("dotenv");
  dotenv.config({ path: dotenvPath });
}

await import("./server.js");


import { MongoClient, Db } from "mongodb";
import chalk from "chalk";
import "dotenv/config";

/** No default credentials: a missing password must stop startup, not fall back to a well-known one. */
export function mongoUri(env: NodeJS.ProcessEnv = process.env, dbName = env["MONGO_DATABASE"] || "kafka_mimic"): string {
  if (env["MONGO_URI"]) return env["MONGO_URI"];
  const user = env["MONGO_USER"], password = env["MONGO_PASSWORD"];
  if (!user || !password) throw new Error("Set MONGO_URI, or MONGO_USER and MONGO_PASSWORD, in .env");
  return `mongodb://${encodeURIComponent(user)}:${encodeURIComponent(password)}@${env["MONGO_HOST"] || "127.0.0.1"}:${env["MONGO_PORT"] || 27017}/${dbName}?authSource=admin`;
}

export class MongoConfig {
  // Created on connect(), so importing this module (tests, CLI help) needs no credentials.
  private client: MongoClient | null = null;
  private db: Db | null = null;
  private readonly dbName = process.env["MONGO_DATABASE"] || "kafka_mimic";

  async connect(): Promise<Db> {
    try {
      if (!this.db) {
        this.client ??= new MongoClient(mongoUri(process.env, this.dbName), { maxPoolSize: 10, minPoolSize: 2, serverSelectionTimeoutMS: 5000 });
        await this.client.connect();
        const db = this.client.db(this.dbName);
        await db.command({ ping: 1 });
        this.db = db;
        console.log(chalk.green("MongoDB Connected Successfully"));
      }
      return this.db;
    } catch (error) {
      console.error(chalk.red("MongoDB Connection Failed:"), error);
      throw error;
    }
  }

  getDb(): Db {
    if (!this.db) {
      throw new Error("MongoDB is not connected. Call connect() first.");
    }
    return this.db;
  }

  async disconnect(): Promise<void> {
    try {
      await this.client?.close();
      this.db = null;
      console.log(chalk.yellow("MongoDB Disconnected"));
    } catch (error) {
      console.error(chalk.red("MongoDB Disconnect Failed:"), error);
      throw error;
    }
  }
}

// Singleton instance to reuse across different modules
export const mongoConfig = new MongoConfig();
export const getMongoDb = (): Db => mongoConfig.getDb();


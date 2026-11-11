import { Redis } from "ioredis";
import chalk from "chalk";
import "dotenv/config";

export class RedisConfig {
  private client: Redis;

  constructor() {
    this.client = new Redis({
      host: process.env["REDIS_HOST"] || "127.0.0.1",
      port: Number(process.env["REDIS_PORT"] || 6379),
      password: process.env["REDIS_PASSWORD"],
      lazyConnect: true,
      retryStrategy: (times: number) => Math.min(times * 1000, 3000),
    });

    this.client.on("connect", () => {
      console.log(chalk.green("Redis Connected Successfully"));
    });

    this.client.on("error", (error: Error) => {
      console.error(chalk.red("Redis Error:"), error.message);
    });
  }

  async connect(): Promise<void> {
    // No default password: a missing one must stop startup, not fall back to a well-known one.
    if (!process.env["REDIS_PASSWORD"]) throw new Error("Set REDIS_PASSWORD in .env");
    try {
      if (this.client.status === "wait" || this.client.status === "close") {
        await this.client.connect();
      }
    } catch (error) {
      console.error("Redis Connection Failed:", error);
      throw error;
    }
  }

  async healthCheck(): Promise<boolean> {
    return await this.client.ping() === "PONG";
  }

  getClient(): Redis {
    return this.client;
  }

  async disconnect(): Promise<void> {
    try {
      if (this.client.status !== "end") {
        await this.client.quit();
        console.log(chalk.yellow("Redis Disconnected"));
      }
    } catch (error) {
      console.error(chalk.red("Redis Disconnect Failed:"), error);
      throw error;
    }
  }
}

// Singleton instance to reuse across different modules
export const redisConfig = new RedisConfig();
export const redisClient = redisConfig.getClient();

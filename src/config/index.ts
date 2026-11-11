import type { ManagedConnection } from "../ports/connection.js";
import type { MetricsConnection } from "../storage/metrics-store.js";
import chalk from "chalk";

export class DatabaseManager {
    constructor(
        private readonly storage: MetricsConnection,
        private readonly redis: ManagedConnection
    ) {}

    /**
     * Initializes all database connections (Redis & metrics storage) concurrently
     */
    public async connectAll(): Promise<void> {
        console.log(chalk.blue("🔌 Initializing database connections..."));
        try {
            await Promise.all([
                this.redis.connect(),
                this.storage.connect(),
            ]);
            console.log(chalk.green("✨ All databases connected successfully."));
        } catch (error) {
            console.error(chalk.red("💥 Failed to connect to databases:"), error);
            throw error;
        }
    }

    /**
     * Gracefully closes all active database connections
     */
    public async disconnectAll(): Promise<void> {
        console.log(chalk.yellow("🛑 Closing all database connections..."));
        const results = await Promise.allSettled([
            this.redis.disconnect(), this.storage.disconnect(),
        ]);
        const failures = results.filter((result) => result.status === "rejected");
        if (failures.length) throw new AggregateError(failures.map((result) => result.reason), "Database disconnect failed");
        console.log(chalk.yellow("👋 All database connections closed."));
    }

    /**
     * Verifies connectivity to all databases concurrently
     */
    public async healthCheck(): Promise<{ redis: boolean; storage: boolean; storageProvider: string }> {
        const [redisResult, storageResult] = await Promise.allSettled([
            this.redis.healthCheck(),
            this.storage.healthCheck(),
        ]);
        return {
            redis: redisResult.status === "fulfilled" && redisResult.value,
            storage: storageResult.status === "fulfilled" && storageResult.value,
            storageProvider: this.storage.name,
        };
    }

}


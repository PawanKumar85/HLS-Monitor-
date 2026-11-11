import "dotenv/config";
import { createApplicationRuntime } from "./application/create-runtime.js";
import { buildApp } from "./app.js";

const PORT = Number(process.env["PORT"] || 3000);
const HOST = process.env["HOST"] || "0.0.0.0";

async function main() {
  const runtime = createApplicationRuntime();
  const { dbManager } = runtime;

  try {
    // 1. Connect databases
    await dbManager.connectAll();

    // 2. Build Fastify application
    const app = await buildApp(runtime);

    // 3. Graceful shutdown handling
    let isShuttingDown = false;

    const shutdown = async (signal: string) => {
      if (isShuttingDown) return;
      isShuttingDown = true;

      app.log.info(`Received ${signal}. Shutting down gracefully...`);
      try {
        await app.close();
        process.exit(0);
      } catch (error) {
        app.log.error(error, "Error during graceful shutdown");
        process.exit(1);
      }
    };

    process.on("SIGINT", () => void shutdown("SIGINT"));
    process.on("SIGTERM", () => void shutdown("SIGTERM"));

    // 4. Start HTTP server
    await app.listen({ port: PORT, host: HOST });
  } catch (error) {
    console.error("💥 Server failed to start:", error);
    await dbManager.disconnectAll();
    process.exit(1);
  }
}

main();

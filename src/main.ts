// ====================================================================================================
//                                     PONTO DE ENTRADA
// ====================================================================================================
import { composeApplication, startBackgroundWork, type CompositionOverrides } from "./composition";
import { loadConfig } from "./config";
import { createHttpApp } from "./infrastructure/http/http-app";
import { logger } from "./observability/logger";

export async function startServer(overrides: CompositionOverrides = {}) {
  const config = loadConfig();
  const application = await composeApplication(config, overrides);
  const httpApp = await createHttpApp(application);
  await httpApp.listen(config.port);
  startBackgroundWork(application);
  logger.info({ event: "server_started", port: config.port }, `ouvindo na porta ${config.port}`);
  return { application, httpApp };
}

if (import.meta.main) {
  await startServer();
}

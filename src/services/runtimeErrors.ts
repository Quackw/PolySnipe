import type { AdminNotifier, AdminErrorSource } from "./adminNotifier.js";

export async function notifyFatalError(
  notifier: AdminNotifier,
  source: AdminErrorSource,
  error: unknown,
  log: (line: string) => void
): Promise<void> {
  try {
    await notifier.notifyError(source, error);
  } catch (notifyError) {
    log(`[notify:fatal] source=${source} error=${String(notifyError)}`);
  }
}

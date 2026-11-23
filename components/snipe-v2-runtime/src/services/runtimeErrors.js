export async function notifyFatalError(notifier, source, error, log) {
    try {
        await notifier.notifyError(source, error);
    }
    catch (notifyError) {
        log(`[notify:fatal] source=${source} error_type=${notifyError instanceof Error ? notifyError.name : "unknown"}`);
    }
}

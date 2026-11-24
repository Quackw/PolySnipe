export function createFastShutdown(intervals, stop) {
  let shutdownPromise;
  return function shutdown() {
    shutdownPromise ??= Promise.resolve().then(async () => {
      for (const timer of intervals) clearInterval(timer);
      await stop();
    });
    return shutdownPromise;
  };
}

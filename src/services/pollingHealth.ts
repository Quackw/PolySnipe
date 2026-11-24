// Empty successful polls are healthy: lack of user messages is not a failure.
export function pollingHealth(onStale: () => void, now = Date.now, staleMs = 180_000, checkMs = 10_000) {
  let lastSuccess = now();
  let tripped = false;
  const timer = setInterval(() => {
    if (!tripped && now() - lastSuccess >= staleMs) {
      tripped = true;
      onStale();
    }
  }, checkMs);
  timer.unref();
  return {
    success: () => {
      lastSuccess = now();
    },
    stop: () => clearInterval(timer)
  };
}

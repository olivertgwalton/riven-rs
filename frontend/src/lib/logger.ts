import { createConsola } from "consola";

/**
 * Browser console logger.
 *
 * Verbosity is deliberately not an environment variable. This is a static
 * bundle, so a `PUBLIC_LOG_LEVEL` would be fixed when the image was built and
 * could not be changed by whoever is actually looking at the console.
 * `localStorage` can be, from devtools, on the machine with the problem:
 *
 *     localStorage.setItem("riven:log-level", "4")   // debug
 *     localStorage.removeItem("riven:log-level")     // back to default
 *
 * Levels: 0 error · 1 warn · 2 log · 3 info · 4 debug · 5 trace · -999 silent.
 */
const STORAGE_KEY = "riven:log-level";
const DEFAULT_LEVEL = import.meta.env.DEV ? 4 : 3;

function resolveLevel(): number {
    if (typeof localStorage === "undefined") return DEFAULT_LEVEL;
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored === null) return DEFAULT_LEVEL;
    const level = Number(stored);
    return Number.isFinite(level) ? level : DEFAULT_LEVEL;
}

export const logger = createConsola({ level: resolveLevel() });

/**
 * Create a scoped logger with a specific tag.
 * Useful for categorizing logs by module/feature.
 *
 * @example
 * const authLogger = createScopedLogger('auth');
 * authLogger.info('User logged in');
 */
export function createScopedLogger(tag: string) {
    return logger.withTag(tag);
}

export default logger;

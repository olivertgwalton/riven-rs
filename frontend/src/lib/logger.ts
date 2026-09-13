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

const level = resolveLevel();

function makeLogger(tag?: string) {
	const prefix = tag ? [`[${tag}]`] : [];
	const at =
		(min: number, fn: (...args: unknown[]) => void) =>
		(...args: unknown[]) => {
			if (level >= min) fn(...prefix, ...args);
		};
	return {
		error: at(0, console.error),
		warn: at(1, console.warn),
		log: at(2, console.log),
		info: at(3, console.info),
		debug: at(4, console.debug),
		trace: at(5, console.trace),
	};
}

export const logger = makeLogger();

/** Create a logger whose output is prefixed with `[tag]`. */
export function createScopedLogger(tag: string) {
	return makeLogger(tag);
}

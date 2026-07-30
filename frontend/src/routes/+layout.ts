/**
 * App-wide rendering mode.
 *
 * `ssr = false` because there is no server to render on — this is a static
 * bundle served by a reverse proxy, and every load talks to the Rust backend
 * with the session cookie, which only exists in the browser.
 *
 * `prerender = false` for the same reason: pages depend on a session and on
 * live library data, so there is nothing meaningful to bake at build time.
 */
export const ssr = false;
export const prerender = false;
export const trailingSlash = "never";

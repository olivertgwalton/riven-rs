import tailwindcss from "@tailwindcss/vite";
import { sveltekit } from "@sveltejs/kit/vite";
import { defineConfig } from "vite";

// No dev proxy: riven serves this bundle from `RIVEN_STATIC_DIR`, so the
// frontend and the API are always one origin. To run a change, build and let
// riven serve `./build` — a `vite dev` server on its own port would be a second
// origin that production never has, and the session cookie and WebAuthn
// relying-party ID are both bound to the origin.
export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
});

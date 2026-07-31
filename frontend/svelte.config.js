import adapter from "@sveltejs/adapter-static";
import { vitePreprocess } from "@sveltejs/vite-plugin-svelte";

const config = {
	preprocess: [vitePreprocess()],
	kit: {
		// Static bundle: the Rust backend owns auth, data and media, so there
		// is no server here to run loads, actions or remote functions.
		// `fallback` makes it a single-page app, so deep links to dynamic
		// routes (/details/media/123/movie) resolve client-side.
		adapter: adapter({ fallback: "index.html", strict: false }),
	},
	compilerOptions: {
		experimental: {
			async: true,
		},
	},
	vitePlugin: {
		inspector: true,
	},
};

export default config;

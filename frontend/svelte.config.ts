import adapter from "@sveltejs/adapter-static";
import type { Config } from "@sveltejs/kit";
import { vitePreprocess } from "@sveltejs/vite-plugin-svelte";

const config: Config = {
	preprocess: [vitePreprocess()],
	kit: {
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

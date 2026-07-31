<script lang="ts">
    import "../app.css";
    // Every font the app uses is imported here, once. The per-weight files are
    // deliberate: the `latin.css` barrels pull every weight a family ships (8
    // for JetBrains Mono, 7 for Merriweather), and we use four and two. Adding
    // a `font-light`/`font-black` utility means adding the matching weight
    // here, or the browser synthesises it.
    // Oxanium is the UI font, so it keeps latin-ext — media titles are full of
    // Polish/Czech/Turkish characters that plain latin does not cover.
    import "@fontsource/oxanium/latin-400.css";
    import "@fontsource/oxanium/latin-500.css";
    import "@fontsource/oxanium/latin-600.css";
    import "@fontsource/oxanium/latin-700.css";
    import "@fontsource/oxanium/latin-ext-400.css";
    import "@fontsource/oxanium/latin-ext-500.css";
    import "@fontsource/oxanium/latin-ext-600.css";
    import "@fontsource/oxanium/latin-ext-700.css";
    import "@fontsource/jetbrains-mono/latin-400.css";
    import "@fontsource/jetbrains-mono/latin-500.css";
    import "@fontsource/jetbrains-mono/latin-600.css";
    import "@fontsource/jetbrains-mono/latin-700.css";
    import "@fontsource/merriweather/latin-400.css";
    import "@fontsource/merriweather/latin-500.css";
    import { ModeWatcher } from "mode-watcher";
    import type { Snippet } from "svelte";

    let { children }: { children: Snippet } = $props();
</script>

<ModeWatcher defaultMode="dark" defaultTheme="darkmatter" />
{@render children()}

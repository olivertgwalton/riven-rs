<script lang="ts">
    // Fonts are imported once, in the root layout.
    import oxanium400Woff2 from "@fontsource/oxanium/files/oxanium-latin-400-normal.woff2?url";

    import { onNavigate } from "$app/navigation";
    import { Toaster } from "$lib/components/ui/sonner/index.js";
    import type { LayoutProps } from "./$types";

    let { children }: LayoutProps = $props();

    onNavigate((navigation) => {
        if (!document.startViewTransition) return;

        return new Promise((resolve) => {
            document.startViewTransition(async () => {
                resolve();
                await navigation.complete;
            });
        });
    });
</script>

<svelte:head>
    <title>Riven</title>
    <link
        rel="preload"
        as="font"
        type="font/woff2"
        href={oxanium400Woff2}
        crossorigin="anonymous" />
</svelte:head>

<Toaster richColors closeButton />

<div class="bg-background h-screen overflow-hidden">
    {@render children?.()}
</div>

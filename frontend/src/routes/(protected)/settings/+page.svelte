<script lang="ts">
    import { can } from "#lib/permissions.js";
    import { toast } from "svelte-sonner";
    import { gqlClient } from "#lib/graphql-client.js";
    import PageShell from "#lib/components/page-shell.svelte";
    import * as Tabs from "#lib/components/ui/tabs/index.js";
    import GeneralTab from "#lib/components/settings/general-tab.svelte";
    import PluginsTab from "#lib/components/settings/plugins-tab.svelte";
    import RankingPanel from "#lib/components/settings/ranking-panel.svelte";
    import SystemTab from "#lib/components/settings/system-tab.svelte";
    import { page } from "$app/state";
    import type { PageData } from "./$types";
    import type { CustomProfile, SettingsSection } from "#lib/components/settings/types.js";
    import { UPDATE_SETTINGS } from "#lib/components/settings/operations.js";
    import { untrack } from "svelte";

    let { data }: { data: PageData } = $props();

    const canManageSettings = $derived(can(page.data.permissions, "MANAGE_SETTINGS"));

    let activeTab = $state("general");

    // Settings sections (general + plugins) — the unified, typed surface.
    let sections = $state<SettingsSection[]>(
        untrack(() => (data.sections ?? []).map((s) => ({ ...s, values: { ...s.values } })))
    );
    const generalSection = $derived(sections.find((s) => s.kind === "general") ?? null);
    const pluginSections = $derived(sections.filter((s) => s.kind === "plugin"));

    // Single write path for the general section and every plugin.
    async function saveSection(section: SettingsSection) {
        try {
            const result = await gqlClient<{ updateSettings: SettingsSection }>(UPDATE_SETTINGS, {
                section: section.id,
                values: section.values
            });
            const updated = result.updateSettings;
            const index = sections.findIndex((s) => s.id === updated.id);
            if (index >= 0) sections[index] = updated;
            toast.success(
                section.kind === "general" ? "General settings saved" : "Plugin settings saved"
            );
        } catch (error) {
            toast.error(error instanceof Error ? error.message : "Failed to save settings");
        }
    }

    // Ranking is a distinct domain (presets + custom profiles), kept separate.
    // Both panels share the one list of saved profiles and show their own kind.
    let customProfiles = $state<CustomProfile[]>(untrack(() => data.customProfiles ?? []));
</script>

<svelte:head>
    <title>Settings - Riven</title>
</svelte:head>

<PageShell>
    <div class="mx-auto w-full max-w-5xl">
        <div class="mb-8">
            <h1 class="text-3xl font-semibold tracking-tight">Settings</h1>
            <p class="text-muted-foreground mt-1 text-sm">
                Configure Riven, plugins, and ranking preferences.
            </p>
        </div>

        <Tabs.Root bind:value={activeTab}>
            <Tabs.List class="mb-6 w-full justify-start">
                <Tabs.Trigger value="general">General</Tabs.Trigger>
                <Tabs.Trigger value="plugins">Plugins</Tabs.Trigger>
                <Tabs.Trigger value="ranking">Ranking</Tabs.Trigger>
                <Tabs.Trigger value="music-ranking">Music ranking</Tabs.Trigger>
            </Tabs.List>

            <Tabs.Content value="general">
                {#if generalSection}
                    <GeneralTab section={generalSection} save={saveSection} />
                {/if}
                {#if canManageSettings}
                    <div class="mt-8">
                        <SystemTab />
                    </div>
                {/if}
            </Tabs.Content>

            <Tabs.Content value="plugins">
                <PluginsTab
                    sections={pluginSections}
                    groups={data.setupGroups}
                    save={saveSection} />
            </Tabs.Content>

            <Tabs.Content value="ranking">
                <RankingPanel
                    kind="video"
                    rankSchema={data.rankSettingsSchema}
                    qualityProfiles={data.qualityProfiles}
                    bind:customProfiles
                    initialSettings={data.rankSettings ?? {}}
                    initialProfileName={data.initialProfileName ?? null} />
            </Tabs.Content>

            <Tabs.Content value="music-ranking">
                <RankingPanel
                    kind="music"
                    rankSchema={data.musicRankSettingsSchema}
                    qualityProfiles={data.musicQualityProfiles}
                    bind:customProfiles
                    initialSettings={data.initialMusicProfile?.settings ?? {}}
                    initialProfileName={data.initialMusicProfile?.id ?? null} />
            </Tabs.Content>
        </Tabs.Root>
    </div>
</PageShell>

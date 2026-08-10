<script lang="ts">
    import { Badge } from "$lib/components/ui/badge/index.js";
    import { Button } from "$lib/components/ui/button/index.js";
    import { Separator } from "$lib/components/ui/separator/index.js";
    import SettingFieldEditor from "./setting-field-editor.svelte";
    import { pluginStatus, buildGeneralSections } from "./helpers";
    import { gqlClient } from "$lib/graphql-client";
    import { SEND_TEST_NOTIFICATION } from "./operations";
    import { toast } from "svelte-sonner";
    import type { SettingsSection, SetupGroup } from "./types";

    let {
        sections,
        groups,
        save
    }: {
        sections: SettingsSection[];
        groups: SetupGroup[];
        save: (section: SettingsSection) => Promise<void>;
    } = $props();

    let selectedId = $state<string | null>(null);
    let saving = $state(false);
    const selected = $derived(sections.find((s) => s.id === selectedId) ?? sections[0] ?? null);
    const groupedSchema = $derived(buildGeneralSections(selected?.schema ?? []));

    // The notifications plugin's movie/show template sections each get an
    // inline "send a test notification" action so a template can be
    // previewed without waiting for a real download. Keyed by section title
    // rather than a boolean per category, since a plugin could in principle
    // have more than two testable sections later.
    const TESTABLE_NOTIFICATION_SECTIONS: Record<string, "MOVIE" | "EPISODE"> = {
        "Movie notifications": "MOVIE",
        "TV show notifications": "EPISODE"
    };
    let testing = $state<Record<string, boolean>>({});

    async function sendTestNotification(sectionTitle: string) {
        const itemType = TESTABLE_NOTIFICATION_SECTIONS[sectionTitle];
        if (!itemType || testing[sectionTitle]) return;
        testing[sectionTitle] = true;
        try {
            await gqlClient<{ sendTestNotification: boolean }>(SEND_TEST_NOTIFICATION, {
                itemType
            });
            toast.success(`Test ${itemType === "MOVIE" ? "movie" : "show"} notification sent`);
        } catch (error) {
            toast.error(
                error instanceof Error ? error.message : "Failed to send test notification"
            );
        } finally {
            testing[sectionTitle] = false;
        }
    }

    // Group plugins by their backend category, in the backend-defined group
    // order (media → sources → services), each sorted by name. Anything with an
    // unknown/missing category lands in a trailing "Other" group.
    const grouped = $derived.by(() => {
        const byName = (a: SettingsSection, b: SettingsSection) => a.title.localeCompare(b.title);
        const knownIds = new Set(groups.map((group) => group.id));

        const result = groups
            .map((group) => ({
                title: group.title,
                sections: sections.filter((s) => s.category === group.id).sort(byName)
            }))
            .filter((group) => group.sections.length > 0);

        const other = sections.filter((s) => !s.category || !knownIds.has(s.category)).sort(byName);
        if (other.length > 0) result.push({ title: "Other", sections: other });

        return result;
    });

    async function savePlugin() {
        if (!selected) return;
        saving = true;
        await save(selected);
        saving = false;
    }
</script>

{#if sections.length === 0}
    <p class="text-muted-foreground text-sm">No plugins registered.</p>
{:else}
    <div class="flex gap-6">
        <aside class="w-48 shrink-0 space-y-4">
            {#each grouped as group (group.title)}
                <div class="space-y-1">
                    <p
                        class="text-muted-foreground px-3 pb-0.5 text-xs font-medium tracking-wide uppercase">
                        {group.title}
                    </p>
                    {#each group.sections as section (section.id)}
                        <button
                            type="button"
                            onclick={() => (selectedId = section.id)}
                            class="flex w-full items-center gap-2 rounded-md px-3 py-2 text-left text-sm transition-colors {selectedId ===
                            section.id
                                ? 'bg-accent text-accent-foreground'
                                : 'hover:bg-muted'}">
                            <span
                                class="h-2 w-2 shrink-0 rounded-full {section.enabled
                                    ? section.valid
                                        ? 'bg-green-500'
                                        : 'bg-amber-500'
                                    : 'bg-zinc-500'}">
                            </span>
                            {section.title}
                        </button>
                    {/each}
                </div>
            {/each}
        </aside>

        <Separator orientation="vertical" class="h-auto" />

        <div class="min-w-0 flex-1">
            {#if selected}
				<div class="mb-4 flex items-center gap-3">
					<h2 class="text-lg font-medium">{selected.title}</h2>
					<Badge variant={pluginStatus(selected).variant}>{pluginStatus(selected).label}</Badge>
                    {#if selected.version}
                        <span class="text-muted-foreground text-xs">v{selected.version}</span>
                    {/if}
                </div>

                <div class="space-y-8">
                    {#each groupedSchema as group, i (group.title || `__default-${i}`)}
                        <section class="space-y-4">
                            {#if group.title}
                                <h3 class="text-base font-semibold tracking-tight">{group.title}</h3>
                            {/if}
                            {#each group.fields as field (field.key)}
                                <SettingFieldEditor {field} bind:value={selected.values[field.key]} />
                            {/each}
                            {#if group.title in TESTABLE_NOTIFICATION_SECTIONS}
                                <Button
                                    type="button"
                                    variant="outline"
                                    size="sm"
                                    disabled={testing[group.title]}
                                    onclick={() => sendTestNotification(group.title)}>
                                    {testing[group.title] ? "Sending…" : "Send test notification"}
                                </Button>
                            {/if}
                        </section>
                    {/each}
                </div>

                <div class="mt-6">
                    <Button type="button" disabled={saving} onclick={savePlugin}>
                        {saving ? "Saving…" : "Save plugin settings"}
                    </Button>
                </div>
            {/if}
        </div>
    </div>
{/if}

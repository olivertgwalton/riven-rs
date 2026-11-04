<script lang="ts">
    import { toast } from "svelte-sonner";
    import { untrack } from "svelte";
    import { gqlClient } from "#lib/graphql-client.js";
    import RankingTab from "./ranking-tab.svelte";
    import {
        SAVE_CUSTOM_PROFILE,
        DELETE_CUSTOM_PROFILE,
        SET_PROFILE_ENABLED,
        UPDATE_PROFILE_SETTINGS,
        CUSTOM_PROFILES
    } from "./operations.js";
    import type { CustomProfile, ProfileKind, QualityProfile, SettingFieldDef } from "./types.js";

    let {
        kind,
        rankSchema,
        qualityProfiles,
        customProfiles = $bindable(),
        initialSettings,
        initialProfileName
    }: {
        kind: ProfileKind;
        rankSchema: SettingFieldDef[];
        qualityProfiles: QualityProfile[];
        /** Every profile of both kinds; this panel shows and edits its own. */
        customProfiles: CustomProfile[];
        initialSettings: Record<string, unknown>;
        initialProfileName: string | null;
    } = $props();

    const GRAPHQL_KIND: Record<ProfileKind, string> = { video: "VIDEO", music: "MUSIC" };

    let rank = $state<Record<string, unknown>>(
        untrack(() => JSON.parse(JSON.stringify(initialSettings)))
    );
    let activeProfileName = $state<string | null>(untrack(() => initialProfileName));
    let newProfileName = $state("");
    let savingProfile = $state(false);

    const profilesOfKind = $derived(customProfiles.filter((profile) => profile.kind === kind));

    function applyProfile(settings: Record<string, unknown>, profileName?: string) {
        activeProfileName = profileName ?? null;
        rank = JSON.parse(JSON.stringify(settings)) as Record<string, unknown>;
    }

    async function saveActiveProfileSettings() {
        if (!activeProfileName) return;
        try {
            await gqlClient<{ updateProfileSettings: unknown }>(UPDATE_PROFILE_SETTINGS, {
                name: activeProfileName,
                settings: rank
            });

            const settings = JSON.parse(JSON.stringify(rank)) as Record<string, unknown>;
            const customProfile = customProfiles.find(
                (profile) => profile.name === activeProfileName
            );
            if (customProfile) customProfile.settings = settings;
            const qualityProfile = qualityProfiles.find(
                (profile) => profile.id === activeProfileName
            );
            if (qualityProfile) qualityProfile.settings = settings;
        } catch {
            throw new Error("Failed to update active ranking profile");
        }
    }

    async function saveAsProfile() {
        const name = newProfileName.trim();
        if (!name) return;
        savingProfile = true;
        try {
            const result = await gqlClient<{ saveCustomProfile: CustomProfile }>(
                SAVE_CUSTOM_PROFILE,
                { id: null, name, settings: rank, kind: GRAPHQL_KIND[kind] }
            );
            const profile = result.saveCustomProfile;
            const idx = customProfiles.findIndex((p) => p.id === profile.id);
            if (idx >= 0) customProfiles[idx] = profile;
            else customProfiles.push(profile);
            newProfileName = "";
            toast.success(`Profile "${profile.name}" saved`);
        } catch {
            toast.error("Failed to save profile");
        } finally {
            savingProfile = false;
        }
    }

    async function toggleProfileEnabled(name: string, enabled: boolean) {
        try {
            await gqlClient<{ setProfileEnabled: unknown }>(SET_PROFILE_ENABLED, { name, enabled });
            const data = await gqlClient<{ customProfiles: CustomProfile[] }>(CUSTOM_PROFILES);
            customProfiles = data.customProfiles ?? customProfiles;
            toast.success(`Profile "${name}" ${enabled ? "enabled" : "disabled"}`);
        } catch {
            toast.error("Failed to update profile");
        }
    }

    async function deleteCustomProfile(id: number, name: string) {
        try {
            await gqlClient<{ deleteCustomProfile: unknown }>(DELETE_CUSTOM_PROFILE, { id });
            customProfiles = customProfiles.filter((p) => p.id !== id);
            toast.success(`Profile "${name}" deleted`);
        } catch {
            toast.error("Failed to delete profile");
        }
    }
</script>

<RankingTab
    bind:rank
    {rankSchema}
    {qualityProfiles}
    customProfiles={profilesOfKind}
    bind:activeProfileName
    bind:newProfileName
    bind:savingProfile
    {saveAsProfile}
    {toggleProfileEnabled}
    {applyProfile}
    {deleteCustomProfile}
    {saveActiveProfileSettings} />

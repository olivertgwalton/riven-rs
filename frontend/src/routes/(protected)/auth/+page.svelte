<script lang="ts">
    import { goto } from "$app/navigation";
    import { resolve } from "$app/paths";
    import { authClient } from "$lib/auth-client";
    import { Button } from "$lib/components/ui/button/index.js";
    import { Input } from "$lib/components/ui/input/index.js";
    import { Label } from "$lib/components/ui/label/index.js";
    import * as Avatar from "$lib/components/ui/avatar/index.js";
    import * as Card from "$lib/components/ui/card/index.js";
    import PageShell from "$lib/components/page-shell.svelte";
    import Passkeys from "$lib/components/auth/passkeys.svelte";
    import { getInitials } from "$lib/utils";
    import type { PageProps } from "./$types";

    // Everything here talks straight to the backend's /auth endpoints. The
    // superforms + server-action versions of these forms are gone: there is no
    // server in this bundle to run an action on.
    let { data }: PageProps = $props();

    let currentPassword = $state("");
    let newPassword = $state("");
    let confirmPassword = $state("");
    let passwordBusy = $state(false);
    let passwordMessage = $state<{ ok: boolean; text: string } | null>(null);

    let displayName = $state(data.user?.name ?? "");
    let profileBusy = $state(false);
    let profileMessage = $state<{ ok: boolean; text: string } | null>(null);

    async function changePassword(event: SubmitEvent) {
        event.preventDefault();
        passwordMessage = null;

        if (newPassword !== confirmPassword) {
            passwordMessage = { ok: false, text: "New password and confirmation do not match." };
            return;
        }

        passwordBusy = true;
        const { error } = await authClient.changePassword({
            currentPassword,
            newPassword,
            // Signing other sessions out on a password change is the safe
            // default: if the old password leaked, this is what evicts whoever
            // used it.
            revokeOtherSessions: true
        });
        passwordBusy = false;

        if (error) {
            passwordMessage = { ok: false, text: error.message };
            return;
        }
        currentPassword = newPassword = confirmPassword = "";
        passwordMessage = { ok: true, text: "Password updated." };
    }

    async function saveProfile(event: SubmitEvent) {
        event.preventDefault();
        profileBusy = true;
        profileMessage = null;

        const { error } = await authClient.updateUser({ name: displayName });
        profileBusy = false;

        profileMessage = error
            ? { ok: false, text: error.message }
            : { ok: true, text: "Profile updated." };
    }

    async function signOut() {
        await authClient.signOut();
        await goto(resolve("/auth/login"));
    }
</script>

<svelte:head><title>Profile · Riven</title></svelte:head>

<PageShell class="mx-auto w-full max-w-3xl">
    <section class="border-border/60 flex items-start gap-4 border-b pb-6">
        <Avatar.Root class="h-16 w-16 text-xl">
            {#if data.user?.image}
                <Avatar.Image src={data.user.image} alt={data.user?.name ?? ""} />
            {/if}
            <Avatar.Fallback class="bg-primary text-primary-foreground font-semibold">
                {getInitials(data.user?.name ?? data.user?.username ?? "?")}
            </Avatar.Fallback>
        </Avatar.Root>
        <div class="flex-1">
            <h1 class="text-xl font-semibold">{data.user?.name ?? data.user?.username}</h1>
            <p class="text-muted-foreground text-sm">{data.user?.email ?? ""}</p>
            {#if data.user?.role}
                <p class="text-muted-foreground mt-1 text-xs uppercase">{data.user.role}</p>
            {/if}
        </div>
        <Button variant="outline" onclick={signOut}>Sign out</Button>
    </section>

    <Card.Root class="mt-6">
        <Card.Header>
            <Card.Title>Profile</Card.Title>
        </Card.Header>
        <form onsubmit={saveProfile}>
            <Card.Content class="space-y-2">
                <Label for="displayName">Display name</Label>
                <Input id="displayName" bind:value={displayName} required />
                {#if profileMessage}
                    <p
                        class="text-sm {profileMessage.ok
                            ? 'text-muted-foreground'
                            : 'text-destructive'}">
                        {profileMessage.text}
                    </p>
                {/if}
            </Card.Content>
            <Card.Footer>
                <Button type="submit" disabled={profileBusy}>
                    {profileBusy ? "Saving…" : "Save"}
                </Button>
            </Card.Footer>
        </form>
    </Card.Root>

    <Card.Root class="mt-6">
        <Card.Header>
            <Card.Title>Change password</Card.Title>
            <Card.Description>Other sessions are signed out when you change it.</Card.Description>
        </Card.Header>
        <form onsubmit={changePassword}>
            <Card.Content class="space-y-4">
                <div class="space-y-2">
                    <Label for="currentPassword">Current password</Label>
                    <Input
                        id="currentPassword"
                        type="password"
                        autocomplete="current-password"
                        bind:value={currentPassword}
                        required />
                </div>
                <div class="space-y-2">
                    <Label for="newPassword">New password</Label>
                    <Input
                        id="newPassword"
                        type="password"
                        autocomplete="new-password"
                        bind:value={newPassword}
                        required />
                </div>
                <div class="space-y-2">
                    <Label for="confirmPassword">Confirm new password</Label>
                    <Input
                        id="confirmPassword"
                        type="password"
                        autocomplete="new-password"
                        bind:value={confirmPassword}
                        required />
                </div>
                {#if passwordMessage}
                    <p
                        class="text-sm {passwordMessage.ok
                            ? 'text-muted-foreground'
                            : 'text-destructive'}"
                        role="alert">
                        {passwordMessage.text}
                    </p>
                {/if}
            </Card.Content>
            <Card.Footer>
                <Button type="submit" disabled={passwordBusy}>
                    {passwordBusy ? "Updating…" : "Update password"}
                </Button>
            </Card.Footer>
        </form>
    </Card.Root>

    <Passkeys />
</PageShell>

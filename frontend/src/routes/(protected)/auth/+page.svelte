<script lang="ts">
    import type { PageProps } from "./$types";
    import { authClient } from "$lib/auth-client";
    import { goto } from "$app/navigation";
    import { resolve } from "$app/paths";
    import { Button } from "$lib/components/ui/button/index.js";
    import { Badge } from "$lib/components/ui/badge/index.js";
    import { Input } from "$lib/components/ui/input/index.js";
    import { Label } from "$lib/components/ui/label/index.js";
    import * as AlertDialog from "$lib/components/ui/alert-dialog/index.js";
    import * as Avatar from "$lib/components/ui/avatar/index.js";
    import Passkeys from "$lib/components/auth/passkeys.svelte";
    import PasswordChangeForm from "$lib/components/auth/password-change-form.svelte";
    import EmailChangeForm from "$lib/components/auth/email-change-form.svelte";
    import AccountLinks from "$lib/components/auth/account-links.svelte";
    import UpdateUserForm from "$lib/components/auth/update-user-form.svelte";
    import UserManagement from "$lib/components/auth/user-management.svelte";
    import * as dateUtils from "$lib/utils/date";
    import { getInitials } from "$lib/utils";
    import { toast } from "svelte-sonner";
    import PageShell from "$lib/components/page-shell.svelte";

    let { data }: PageProps = $props();

    const displayName = $derived(data.user?.name ?? data.user?.username ?? "");

    /**
     * Every riven account has a `credential` row unless it was created purely
     * through Plex, and there is no `/set-password` endpoint to recover from
     * that — so the password form is hidden rather than replaced.
     */
    const hasCredentialProvider = $derived(
        data.accounts.some((account) => account.providerId === "credential")
    );

    let deleteOpen = $state(false);
    let deletePassword = $state("");
    let deleting = $state(false);

    async function signOut() {
        await authClient.signOut();
        await goto(resolve("/auth/login"));
    }

    async function deleteAccount() {
        deleting = true;
        const { error } = await authClient.deleteUser({ password: deletePassword });
        deleting = false;

        if (error) {
            toast.error(error.message);
            return;
        }
        deleteOpen = false;
        await goto(resolve("/auth/login"));
    }
</script>

<svelte:head>
    <title>Profile - Riven</title>
</svelte:head>

<PageShell class="mx-auto w-full max-w-5xl">
    <section
        class="border-border/60 flex flex-col gap-5 border-b pb-6 lg:flex-row lg:items-start lg:justify-between">
        <div class="flex items-start gap-4">
            <Avatar.Root class="h-16 w-16 text-xl">
                {#if data.user?.image}
                    <Avatar.Image src={data.user.image} alt={displayName} />
                {/if}
                <Avatar.Fallback class="bg-primary text-primary-foreground font-semibold">
                    {getInitials(displayName)}
                </Avatar.Fallback>
            </Avatar.Root>

            <div class="min-w-0">
                <div class="flex flex-wrap items-center gap-2">
                    <h1 class="text-3xl font-bold tracking-tight">{displayName}'s Profile</h1>
                    {#if data.user?.role}
                        <Badge variant="secondary" class="capitalize">
                            Role: {data.user.role}
                        </Badge>
                    {/if}
                </div>
                <p class="text-muted-foreground mt-1 text-sm break-all">{data.user?.email ?? ""}</p>

                <dl class="text-muted-foreground mt-3 grid gap-x-6 gap-y-1 text-sm sm:grid-cols-2">
                    <div class="flex gap-2">
                        <dt>Member since</dt>
                        <dd class="text-foreground">
                            {dateUtils.formatDate(data.user?.createdAt) ?? "Unknown"}
                        </dd>
                    </div>
                    <div class="flex gap-2">
                        <dt>Last updated</dt>
                        <dd class="text-foreground">
                            {dateUtils.formatDate(data.user?.updatedAt) ?? "Unknown"}
                        </dd>
                    </div>
                </dl>
            </div>
        </div>

        <div class="flex flex-col gap-2 sm:flex-row">
            <Button variant="secondary" class="w-full sm:w-auto" onclick={signOut}>Logout</Button>

            <AlertDialog.Root bind:open={deleteOpen}>
                <AlertDialog.Trigger>
                    {#snippet child({ props })}
                        <Button variant="destructive" class="w-full sm:w-auto" {...props}>
                            Delete Account
                        </Button>
                    {/snippet}
                </AlertDialog.Trigger>
                <AlertDialog.Content>
                    <AlertDialog.Header>
                        <AlertDialog.Title>Delete your account?</AlertDialog.Title>
                        <AlertDialog.Description>
                            This removes your user, its sessions and its passkeys. Library content
                            is untouched. This cannot be undone.
                        </AlertDialog.Description>
                    </AlertDialog.Header>
                    <div class="space-y-2">
                        <Label for="deletePassword">Confirm with your password</Label>
                        <Input
                            id="deletePassword"
                            type="password"
                            autocomplete="current-password"
                            bind:value={deletePassword} />
                    </div>
                    <AlertDialog.Footer>
                        <AlertDialog.Cancel disabled={deleting}>Cancel</AlertDialog.Cancel>
                        <AlertDialog.Action
                            disabled={deleting || deletePassword.length === 0}
                            onclick={deleteAccount}>
                            {deleting ? "Deleting…" : "Delete account"}
                        </AlertDialog.Action>
                    </AlertDialog.Footer>
                </AlertDialog.Content>
            </AlertDialog.Root>
        </div>
    </section>

    <div>
        {#if hasCredentialProvider}
            <PasswordChangeForm data={data.passwordChangeForm} />
        {/if}
        <EmailChangeForm data={data.emailChangeForm} />

        <UpdateUserForm data={data.changeUserDataForm} />
    </div>

    {#if data.canManageUsers}
        <UserManagement
            formData={data.createUserForm}
            users={data.managedUsers}
            currentUserId={data.user?.id ?? ""} />
    {/if}

    <AccountLinks accounts={data.accounts} />
    <Passkeys />
</PageShell>

<script lang="ts">
    import Check from "@lucide/svelte/icons/check";
    import Fingerprint from "@lucide/svelte/icons/fingerprint";
    import Pencil from "@lucide/svelte/icons/pencil";
    import X from "@lucide/svelte/icons/x";
    import { onMount } from "svelte";
    import { toast } from "svelte-sonner";

    import { authClient, type Passkey } from "$lib/auth-client";
    import { createScopedLogger } from "$lib/logger";
    import { browserSupportsPasskeys, PasskeyCancelledError } from "$lib/passkeys";
    import { Button } from "$lib/components/ui/button";
    import { Input } from "$lib/components/ui/input";
    import * as Card from "$lib/components/ui/card";

    const logger = createScopedLogger("passkeys");

    let passkeys = $state<Passkey[]>([]);
    let loading = $state(true);
    let registering = $state(false);
    let editingId = $state<string | null>(null);
    let editingName = $state("");
    let saving = $state(false);

    const supported = browserSupportsPasskeys();

    async function load() {
        loading = true;
        const { data, error } = await authClient.passkey.list();
        if (error) {
            logger.error("failed to list passkeys", error.message);
            passkeys = [];
        } else {
            passkeys = data ?? [];
        }
        loading = false;
    }

    onMount(() => {
        if (supported) void load();
        else loading = false;
    });

    async function addPasskey() {
        registering = true;
        try {
            const { error } = await authClient.passkey.add({ name: defaultPasskeyName() });
            if (error) {
                toast.error(error.message);
                return;
            }
            toast.success("Passkey registered.");
            await load();
        } catch (error) {
            if (!(error instanceof PasskeyCancelledError)) {
                logger.error("passkey registration failed", error);
                toast.error("Could not register that passkey.");
            }
        } finally {
            registering = false;
        }
    }

    /** A best guess at the device, so the list is not a column of "Unnamed". */
    function defaultPasskeyName() {
        const platform = navigator.userAgent.match(
            /(iPhone|iPad|Android|Macintosh|Windows|Linux)/
        )?.[1];
        return platform ? `${platform} passkey` : "Passkey";
    }

    function startEditing(passkey: Passkey) {
        editingId = passkey.id;
        editingName = passkey.name ?? "";
    }

    async function saveName(id: string) {
        const name = editingName.trim();
        if (!name) {
            toast.error("A passkey needs a name.");
            return;
        }
        saving = true;
        const { error } = await authClient.passkey.rename({ id, name });
        saving = false;
        if (error) {
            toast.error(error.message);
            return;
        }
        editingId = null;
        await load();
    }

    async function removePasskey(id: string) {
        const { error } = await authClient.passkey.remove({ id });
        if (error) {
            toast.error(error.message);
            return;
        }
        toast.success("Passkey removed.");
        await load();
    }
</script>

<Card.Root class="mt-6">
    <Card.Header>
        <Card.Title>Passkeys</Card.Title>
        <Card.Description>
            Sign in with your device instead of a password. Passkeys are bound to the hostname you
            registered them on.
        </Card.Description>
    </Card.Header>
    <Card.Content>
        {#if !supported}
            <p class="text-muted-foreground text-sm">This browser does not support passkeys.</p>
        {:else}
            {#if loading}
                <p class="text-muted-foreground text-sm">Loading…</p>
            {:else if passkeys.length === 0}
                <p class="text-muted-foreground text-sm">No passkeys registered yet.</p>
            {:else}
                <ul class="border-border/60 mb-4 border-t">
                    {#each passkeys as passkey (passkey.id)}
                        <li
                            class="border-border/60 flex items-center justify-between gap-3 border-b py-3">
                            <Fingerprint class="text-muted-foreground size-5 shrink-0" />
                            <div class="min-w-0 flex-1">
                                {#if editingId === passkey.id}
                                    <Input
                                        bind:value={editingName}
                                        disabled={saving}
                                        class="h-8"
                                        aria-label="Passkey name" />
                                {:else}
                                    <p class="truncate text-sm font-medium">
                                        {passkey.name || "Unnamed passkey"}
                                    </p>
                                    <p class="text-muted-foreground text-xs">
                                        Added {new Date(passkey.created_at).toLocaleDateString()}
                                    </p>
                                {/if}
                            </div>
                            <div class="flex shrink-0 items-center gap-2">
                                {#if editingId === passkey.id}
                                    <Button
                                        size="icon"
                                        variant="ghost"
                                        class="size-8"
                                        disabled={saving}
                                        aria-label="Save name"
                                        onclick={() => saveName(passkey.id)}>
                                        <Check class="size-4" />
                                    </Button>
                                    <Button
                                        size="icon"
                                        variant="ghost"
                                        class="size-8"
                                        disabled={saving}
                                        aria-label="Cancel"
                                        onclick={() => (editingId = null)}>
                                        <X class="size-4" />
                                    </Button>
                                {:else}
                                    <Button
                                        size="icon"
                                        variant="ghost"
                                        class="size-8"
                                        aria-label="Rename passkey"
                                        onclick={() => startEditing(passkey)}>
                                        <Pencil class="size-4" />
                                    </Button>
                                    <Button
                                        size="sm"
                                        variant="destructive"
                                        onclick={() => removePasskey(passkey.id)}>
                                        Remove
                                    </Button>
                                {/if}
                            </div>
                        </li>
                    {/each}
                </ul>
            {/if}

            <Button variant="outline" disabled={registering} onclick={addPasskey}>
                <Fingerprint class="mr-2 size-4" />
                {registering ? "Waiting for your device…" : "Add a passkey"}
            </Button>
        {/if}
    </Card.Content>
</Card.Root>

<script lang="ts">
    import { Button } from "$lib/components/ui/button/index.js";
    import Link2 from "@lucide/svelte/icons/link-2";
    import Link2Off from "@lucide/svelte/icons/link-2-off";
    import { authClient, type LinkedAccount } from "$lib/auth-client";
    import { toast } from "svelte-sonner";
    import { invalidateAll } from "$app/navigation";

    let { accounts }: { accounts: LinkedAccount[] } = $props();

    /**
     * The providers riven can actually authenticate against. It is a fixed list
     * rather than server-supplied config because riven registers no generic
     * OAuth providers: `credential` is better-auth's email/password account, and
     * Plex is riven's own PIN flow mounted outside better-auth's router.
     */
    const providers = [
        { id: "credential", name: "Password", linkable: false },
        { id: "plex", name: "Plex", linkable: true }
    ];

    let busy = $state<string | null>(null);

    function linked(providerId: string) {
        return accounts.find((account) => account.providerId === providerId);
    }

    async function unlink(providerId: string) {
        busy = providerId;
        const { error } = await authClient.unlinkAccount({ providerId });
        busy = null;

        if (error) {
            toast.error(error.message);
            return;
        }
        toast.success(`${providerId} unlinked successfully.`);
        await invalidateAll();
    }

    /**
     * The same PIN dance the login page runs. Linking is by confirmed email on
     * riven's side, so this attaches Plex to whichever local account shares the
     * address — it never creates one.
     */
    async function linkPlex() {
        // Opened before the await: a popup opened afterwards is blocked, because
        // by then the click is no longer what caused it.
        const plexTab = window.open("", "_blank");
        busy = "plex";

        const { data, error } = await authClient.plex.start();
        if (error || !data) {
            plexTab?.close();
            busy = null;
            toast.error(error?.message ?? "Could not start Plex sign-in");
            return;
        }
        if (!plexTab) {
            busy = null;
            toast.info("Allow pop-ups to link Plex, then try again.");
            return;
        }
        plexTab.location.href = data.auth_url;

        const deadline = Date.now() + 3 * 60 * 1000;
        while (Date.now() < deadline) {
            await new Promise((resolve) => setTimeout(resolve, 2000));
            const { data: status, error: pollError } = await authClient.plex.poll(data.id);
            if (pollError) {
                busy = null;
                toast.error(pollError.message);
                return;
            }
            if (status && !status.pending) {
                plexTab.close();
                busy = null;
                toast.success("Plex linked successfully.");
                await invalidateAll();
                return;
            }
        }

        plexTab.close();
        busy = null;
        toast.error("Plex linking timed out");
    }
</script>

<section class="border-border/60 grid gap-4 border-b py-6 md:grid-cols-[12rem_minmax(0,1fr)]">
    <div>
        <h2 class="text-base font-semibold">Account Links</h2>
        <p class="text-muted-foreground mt-1 text-sm">
            Manage your linked authentication providers.
        </p>
    </div>

    <div class="flex min-w-0 flex-col">
        {#each providers as provider (provider.id)}
            <div class="border-border/60 flex items-center justify-between border-t py-3">
                <div class="flex items-center gap-2">
                    <span>{provider.name}</span>
                    {#if !linked(provider.id)}
                        <span class="text-muted-foreground text-sm">Not linked</span>
                    {/if}
                </div>

                {#if linked(provider.id)}
                    {#if provider.linkable}
                        <Button
                            variant="destructive"
                            size="sm"
                            disabled={busy === provider.id}
                            onclick={() => unlink(provider.id)}>
                            <Link2Off class="mr-2 h-4 w-4" />
                            Unlink
                        </Button>
                    {:else}
                        <span class="text-muted-foreground text-sm">Linked</span>
                    {/if}
                {:else if provider.linkable}
                    <Button size="sm" disabled={busy === provider.id} onclick={linkPlex}>
                        <Link2 class="mr-2 h-4 w-4" />
                        {busy === provider.id ? "Waiting for Plex…" : "Link"}
                    </Button>
                {/if}
            </div>
        {/each}
    </div>
</section>

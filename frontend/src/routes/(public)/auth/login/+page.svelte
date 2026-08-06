<script lang="ts">
    import { onMount } from "svelte";
    import { goto } from "$app/navigation";
    import { resolve } from "$app/paths";
    import Fingerprint from "@lucide/svelte/icons/fingerprint";
    import KeyRound from "@lucide/svelte/icons/key-round";
    import Mountain from "@lucide/svelte/icons/mountain";
    import { toast } from "svelte-sonner";

    import { authClient, type OidcProviderSummary } from "$lib/auth-client";
    import { createScopedLogger } from "$lib/logger";
    import {
        browserSupportsPasskeys,
        supportsConditionalMediation,
        PasskeyCancelledError
    } from "$lib/passkeys";
    import { Button } from "$lib/components/ui/button";
    import { Input } from "$lib/components/ui/input";
    import { Label } from "$lib/components/ui/label";
    import * as Card from "$lib/components/ui/card";
    import * as Tabs from "$lib/components/ui/tabs";

    const logger = createScopedLogger("login");

    let username = $state("");
    let password = $state("");
    let submitting = $state(false);
    let errorMessage = $state<string | null>(null);

    // Sign-up is offered only while the instance has no accounts; the first one
    // becomes the admin. The backend enforces this — this flag just decides
    // whether to render the tab.
    let signUpAvailable = $state(false);
    let activeTab = $state("login");
    let signUpUsername = $state("");
    let signUpEmail = $state("");
    let signUpPassword = $state("");
    let signUpConfirm = $state("");

    let passkeysAvailable = $state(false);
    let passkeyBusy = $state(false);
    let plexBusy = $state(false);
    let oidcProviders = $state<OidcProviderSummary[]>([]);
    let oidcBusyId = $state<string | null>(null);

    // Only one `credentials.get()` may be outstanding, so the autofill request
    // has to be cancelled before the button opens a modal one.
    let autofillAbort: AbortController | null = null;

    function finishSignIn() {
        return goto(resolve("/"), { invalidateAll: true });
    }

    async function signIn(event: SubmitEvent) {
        event.preventDefault();
        submitting = true;
        errorMessage = null;

        const { error } = await authClient.signIn.username({ username, password });

        submitting = false;
        if (error) {
            // Not distinguishing "no such user" from "wrong password": that
            // difference is a user-enumeration oracle.
            errorMessage = error.message ?? "Sign in failed";
            return;
        }
        await finishSignIn();
    }

    async function signUp(event: SubmitEvent) {
        event.preventDefault();
        errorMessage = null;

        if (signUpPassword !== signUpConfirm) {
            errorMessage = "Passwords do not match.";
            return;
        }

        submitting = true;
        const { error } = await authClient.signUp.email({
            username: signUpUsername,
            email: signUpEmail,
            password: signUpPassword
        });
        submitting = false;

        if (error) {
            errorMessage = error.message;
            return;
        }
        toast.success("Admin account created.");
        await finishSignIn();
    }

    onMount(() => {
        passkeysAvailable = browserSupportsPasskeys();

        void authClient.firstUserAvailable().then(({ data }) => {
            signUpAvailable = data?.available === true;
            if (signUpAvailable) activeTab = "signup";
        });

        void authClient.oidcProviders().then(({ data }) => {
            oidcProviders = data ?? [];
        });

        if (!passkeysAvailable) return;

        // Conditional mediation offers passkeys from inside the username field.
        // Failures stay in the log: the user did not ask for this, and the form
        // behind it still works.
        let cancelled = false;
        void (async () => {
            if (!(await supportsConditionalMediation()) || cancelled) return;
            autofillAbort = new AbortController();
            try {
                const { error } = await authClient.signIn.passkey({
                    conditional: true,
                    signal: autofillAbort.signal
                });
                if (error) {
                    logger.debug("passkey autofill rejected", error.message);
                    return;
                }
                await finishSignIn();
            } catch (error) {
                if (!(error instanceof PasskeyCancelledError)) {
                    logger.debug("passkey autofill failed", error);
                }
            }
        })();

        return () => {
            cancelled = true;
            autofillAbort?.abort();
        };
    });

    async function signInWithPasskey() {
        autofillAbort?.abort();
        autofillAbort = null;

        passkeyBusy = true;
        errorMessage = null;
        try {
            const { error } = await authClient.signIn.passkey();
            if (error) {
                errorMessage = error.message;
                return;
            }
            await finishSignIn();
        } catch (error) {
            if (!(error instanceof PasskeyCancelledError)) {
                errorMessage = "Passkey authentication failed";
                logger.error("passkey sign-in failed", error);
            }
        } finally {
            passkeyBusy = false;
        }
    }

    async function signInWithPlex() {
        plexBusy = true;
        errorMessage = null;

        // Opened before the await: a popup opened afterwards is blocked, because
        // by then the click is no longer what caused it.
        const plexTab = window.open("", "_blank");
        const { data, error } = await authClient.plex.start();
        if (error || !data) {
            plexTab?.close();
            plexBusy = false;
            errorMessage = error?.message ?? "Could not start Plex sign-in";
            return;
        }

        if (!plexTab) {
            plexBusy = false;
            toast.info("Allow pop-ups to sign in with Plex, then try again.");
            return;
        }
        plexTab.location.href = data.auth_url;

        // Plex PINs live for 15 minutes; give up well before that so a forgotten
        // tab does not poll forever.
        const deadline = Date.now() + 3 * 60 * 1000;
        while (Date.now() < deadline) {
            await new Promise((resolve) => setTimeout(resolve, 2000));
            const { data: status, error: pollError } = await authClient.plex.poll(data.handle);
            if (pollError) {
                plexBusy = false;
                errorMessage = pollError.message;
                return;
            }
            if (status && !status.pending) {
                plexTab.close();
                plexBusy = false;
                await finishSignIn();
                return;
            }
        }

        plexBusy = false;
        errorMessage = "Plex sign-in timed out";
    }

    async function signInWithOidc(id: string) {
        oidcBusyId = id;
        errorMessage = null;

        const { data, error } = await authClient.signIn.social({
            provider: id,
            callbackURL: window.location.origin + resolve("/")
        });
        if (error || !data?.url) {
            oidcBusyId = null;
            errorMessage = error?.message ?? "Could not start sign-in";
            return;
        }
        // Navigates away — no need to reset oidcBusyId, the provider's
        // authorization page replaces this one.
        window.location.href = data.url;
    }
</script>

<svelte:head><title>Sign in · Riven</title></svelte:head>

<div class="grid min-h-svh lg:grid-cols-2">
    <div class="flex flex-col gap-4 overflow-y-auto p-6 md:p-10">
        <div class="flex justify-center gap-2 md:justify-start">
            <span class="flex items-center gap-2 font-medium">
                <span
                    class="bg-primary text-primary-foreground flex size-6 items-center justify-center rounded-md">
                    <Mountain class="size-4" />
                </span>
                Riven Media
            </span>
        </div>

        <div class="flex flex-1 items-center justify-center">
            <Tabs.Root bind:value={activeTab} class="w-full max-w-md">
                {#if signUpAvailable}
                    <Tabs.List class="w-full">
                        <Tabs.Trigger value="signup">Create account</Tabs.Trigger>
                        <Tabs.Trigger value="login">Sign in</Tabs.Trigger>
                    </Tabs.List>
                {/if}

                <Tabs.Content value="login">
                    <Card.Root>
                        <Card.Header>
                            <Card.Title class="text-2xl">Sign in</Card.Title>
                            <Card.Description>Sign in to your Riven library.</Card.Description>
                        </Card.Header>
                        <Card.Content>
                            <form onsubmit={signIn} class="space-y-4">
                                <div class="space-y-2">
                                    <Label for="username">Username</Label>
                                    <Input
                                        id="username"
                                        name="username"
                                        autocomplete="username webauthn"
                                        bind:value={username}
                                        required />
                                </div>
                                <div class="space-y-2">
                                    <Label for="password">Password</Label>
                                    <Input
                                        id="password"
                                        name="password"
                                        type="password"
                                        autocomplete="current-password webauthn"
                                        bind:value={password}
                                        required />
                                </div>
                                {#if errorMessage && activeTab === "login"}
                                    <p class="text-destructive text-sm" role="alert">
                                        {errorMessage}
                                    </p>
                                {/if}
                                <Button type="submit" class="w-full" disabled={submitting}>
                                    {submitting ? "Signing in…" : "Sign in"}
                                </Button>
                            </form>

                            <div class="relative my-6">
                                <div class="absolute inset-0 flex items-center" aria-hidden="true">
                                    <span class="w-full border-t"></span>
                                </div>
                                <div class="relative flex justify-center text-xs uppercase">
                                    <span class="bg-card text-muted-foreground px-2">
                                        Or continue with
                                    </span>
                                </div>
                            </div>

                            <div class="flex flex-col gap-2">
                                {#if passkeysAvailable}
                                    <Button
                                        variant="outline"
                                        class="w-full"
                                        type="button"
                                        disabled={passkeyBusy}
                                        onclick={signInWithPasskey}>
                                        <Fingerprint class="mr-2 size-4" />
                                        {passkeyBusy
                                            ? "Waiting for your device…"
                                            : "Sign in with a passkey"}
                                    </Button>
                                {/if}
                                <Button
                                    variant="outline"
                                    class="w-full"
                                    type="button"
                                    disabled={plexBusy}
                                    onclick={signInWithPlex}>
                                    <svg
                                        xmlns="http://www.w3.org/2000/svg"
                                        viewBox="0 0 512 512"
                                        aria-hidden="true"
                                        class="mr-2 size-4">
                                        <path
                                            d="M256 70H148l108 186-108 186h108l108-186z"
                                            fill="currentColor" />
                                    </svg>
                                    {plexBusy ? "Waiting for Plex…" : "Sign in with Plex"}
                                </Button>
                                {#each oidcProviders as provider (provider.id)}
                                    <Button
                                        variant="outline"
                                        class="w-full"
                                        type="button"
                                        disabled={oidcBusyId !== null}
                                        onclick={() => signInWithOidc(provider.id)}>
                                        <KeyRound class="mr-2 size-4" />
                                        {oidcBusyId === provider.id
                                            ? "Redirecting…"
                                            : `Sign in with ${provider.name}`}
                                    </Button>
                                {/each}
                            </div>
                        </Card.Content>
                    </Card.Root>
                </Tabs.Content>

                {#if signUpAvailable}
                    <Tabs.Content value="signup">
                        <Card.Root>
                            <Card.Header>
                                <Card.Title class="text-2xl">Create your account</Card.Title>
                                <Card.Description>
                                    This instance has no users yet. The account you create here is
                                    the administrator.
                                </Card.Description>
                            </Card.Header>
                            <Card.Content>
                                <form onsubmit={signUp} class="space-y-4">
                                    <div class="space-y-2">
                                        <Label for="signUpUsername">Username</Label>
                                        <Input
                                            id="signUpUsername"
                                            autocomplete="username"
                                            bind:value={signUpUsername}
                                            required />
                                    </div>
                                    <div class="space-y-2">
                                        <Label for="signUpEmail">Email</Label>
                                        <Input
                                            id="signUpEmail"
                                            type="email"
                                            autocomplete="email"
                                            bind:value={signUpEmail}
                                            required />
                                    </div>
                                    <div class="space-y-2">
                                        <Label for="signUpPassword">Password</Label>
                                        <Input
                                            id="signUpPassword"
                                            type="password"
                                            autocomplete="new-password"
                                            minlength={8}
                                            bind:value={signUpPassword}
                                            required />
                                    </div>
                                    <div class="space-y-2">
                                        <Label for="signUpConfirm">Confirm password</Label>
                                        <Input
                                            id="signUpConfirm"
                                            type="password"
                                            autocomplete="new-password"
                                            bind:value={signUpConfirm}
                                            required />
                                    </div>
                                    {#if errorMessage && activeTab === "signup"}
                                        <p class="text-destructive text-sm" role="alert">
                                            {errorMessage}
                                        </p>
                                    {/if}
                                    <Button type="submit" class="w-full" disabled={submitting}>
                                        {submitting ? "Creating…" : "Create admin account"}
                                    </Button>
                                </form>
                            </Card.Content>
                        </Card.Root>
                    </Tabs.Content>
                {/if}
            </Tabs.Root>
        </div>
    </div>

    <div
        class="from-primary/25 via-background to-background relative hidden overflow-hidden bg-gradient-to-br lg:block"
        aria-hidden="true">
        <div class="bg-primary/20 absolute -top-24 -right-24 size-[28rem] rounded-full blur-3xl">
        </div>
        <div class="bg-accent/20 absolute -bottom-32 -left-16 size-[24rem] rounded-full blur-3xl">
        </div>
        <img
            src="/images/login-hero.webp"
            alt=""
            fetchpriority="high"
            class="absolute inset-0 size-full object-cover" />
        <div class="from-background absolute inset-0 bg-gradient-to-r to-transparent"></div>
        <div
            class="from-primary/25 absolute inset-0 bg-gradient-to-br to-transparent mix-blend-overlay">
        </div>
    </div>
</div>

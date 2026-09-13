<script lang="ts">
    import { emailChangeSchema } from "$lib/schemas/auth";
    import { Button } from "$lib/components/ui/button/index.js";
    import { Input } from "$lib/components/ui/input/index.js";
    import { Label } from "$lib/components/ui/label/index.js";
    import { toast } from "svelte-sonner";
    import LoaderCircle from "@lucide/svelte/icons/loader-circle";
    import { invalidateAll } from "$app/navigation";
    import { authClient } from "$lib/auth-client";
    import FormBase from "./form-base.svelte";
    import { validateForm } from "./validate";

    let errors = $state<Record<string, string>>({});
    let submitting = $state(false);

    async function onsubmit(event: SubmitEvent & { currentTarget: HTMLFormElement }) {
        event.preventDefault();
        const result = validateForm(emailChangeSchema, event.currentTarget);
        errors = result.errors ?? {};
        if (!result.data) return;

        submitting = true;
        const { error } = await authClient.changeEmail({
            new_email: result.data.newEmail,
            current_password: result.data.currentPassword || undefined
        });
        submitting = false;

        if (error) {
            toast.error(error.message);
            return;
        }

        // riven sends no confirmation mail, so the address is already live —
        // reload so the header stops showing the old one.
        await invalidateAll();
        toast.success("Email changed successfully.");
    }
</script>

<FormBase
    title="Change Email"
    description="Manage your email address associated with your account.">
    {#snippet content()}
        <form id="email-change-form" novalidate {onsubmit}>
            <div class="space-y-2">
                <Label for="newEmail" class={errors.newEmail && "text-destructive"}>New Email</Label>
                <Input
                    id="newEmail"
                    name="newEmail"
                    type="email"
                    placeholder="Your new email address"
                    aria-invalid={!!errors.newEmail} />
                {#if errors.newEmail}
                    <p class="text-destructive text-sm font-medium">{errors.newEmail}</p>
                {/if}
            </div>
            <div class="space-y-2">
                <Label for="currentPassword">Current Password</Label>
                <Input
                    id="currentPassword"
                    name="currentPassword"
                    type="password"
                    placeholder="Leave blank if you sign in without a password" />
            </div>
        </form>
    {/snippet}

    {#snippet footer()}
        <Button type="submit" form="email-change-form" variant="secondary" size="sm" disabled={submitting}>
            {#if submitting}
                <LoaderCircle class="mr-2 h-5 w-5 animate-spin" />
            {/if}
            Change Email
        </Button>
    {/snippet}
</FormBase>

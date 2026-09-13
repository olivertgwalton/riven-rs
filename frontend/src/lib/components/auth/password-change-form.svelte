<script lang="ts">
    import * as ButtonGroup from "$lib/components/ui/button-group/index.js";
    import { Button } from "$lib/components/ui/button/index.js";
    import { passwordChangeSchema } from "$lib/schemas/auth";
    import { Input } from "$lib/components/ui/input/index.js";
    import { Label } from "$lib/components/ui/label/index.js";
    import Eye from "@lucide/svelte/icons/eye";
    import EyeOff from "@lucide/svelte/icons/eye-off";
    import { Switch } from "$lib/components/ui/switch/index.js";
    import { toast } from "svelte-sonner";
    import LoaderCircle from "@lucide/svelte/icons/loader-circle";
    import { authClient } from "$lib/auth-client";
    import FormBase from "./form-base.svelte";
    import { validateForm } from "./validate";

    type PasswordFieldName = "oldPassword" | "newPassword" | "confirmNewPassword";

    let errors = $state<Record<string, string>>({});
    let submitting = $state(false);
    const passwordVisibility = $state({
        oldPassword: false,
        newPassword: false,
        confirmNewPassword: false
    });

    async function onsubmit(event: SubmitEvent & { currentTarget: HTMLFormElement }) {
        event.preventDefault();
        const form = event.currentTarget;
        const result = validateForm(passwordChangeSchema, form);
        errors = result.errors ?? {};
        if (!result.data) return;

        if (result.data.oldPassword === result.data.newPassword) {
            errors = { newPassword: "New password must be different from old password." };
            return;
        }

        submitting = true;
        const { error } = await authClient.changePassword({
            current_password: result.data.oldPassword,
            new_password: result.data.newPassword,
            revoke_other_sessions: result.data.revokeSessions
        });
        submitting = false;

        if (error) {
            toast.error(error.message);
            return;
        }

        form.reset();
        toast.success("Password changed successfully.");
    }
</script>

{#snippet passwordFormField(name: PasswordFieldName, title: string)}
    <div class="space-y-2">
        <Label for={name} class={errors[name] && "text-destructive"}>{title}</Label>
        <ButtonGroup.Root class="w-full">
            <Input
                id={name}
                {name}
                type={passwordVisibility[name] ? "text" : "password"}
                autocomplete={name === "oldPassword" ? "current-password" : "new-password"}
                aria-invalid={!!errors[name]} />
            <Button
                type="button"
                onclick={() => (passwordVisibility[name] = !passwordVisibility[name])}
                variant="outline"
                size="icon"
                aria-label="toggle password visibility">
                {#if passwordVisibility[name]}
                    <EyeOff />
                {:else}
                    <Eye />
                {/if}
            </Button>
        </ButtonGroup.Root>
        {#if errors[name]}
            <p class="text-destructive text-sm font-medium">{errors[name]}</p>
        {/if}
    </div>
{/snippet}

<FormBase
    title="Change Password"
    description="Update your account password to keep your account secure.">
    {#snippet content()}
        <form id="password-change-form" novalidate {onsubmit}>
            {@render passwordFormField("oldPassword", "Current Password")}
            {@render passwordFormField("newPassword", "New Password")}
            {@render passwordFormField("confirmNewPassword", "Confirm New Password")}

            <div class="mt-4 flex items-center gap-2">
                <Switch id="revokeSessions" name="revokeSessions" />
                <Label for="revokeSessions">Revoke all other sessions</Label>
            </div>
        </form>
    {/snippet}

    {#snippet footer()}
        <Button
            type="submit"
            form="password-change-form"
            variant="secondary"
            size="sm"
            disabled={submitting}>
            {#if submitting}
                <LoaderCircle class="mr-2 h-5 w-5 animate-spin" />
            {/if}
            Change Password
        </Button>
    {/snippet}
</FormBase>

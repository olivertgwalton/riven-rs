<script lang="ts">
    import * as Form from "$lib/components/ui/form/index.js";
    import * as ButtonGroup from "$lib/components/ui/button-group/index.js";
    import { Button } from "$lib/components/ui/button/index.js";
    import type { SuperValidated } from "sveltekit-superforms";
    import { passwordChangeSchema, type PasswordChangeSchema } from "$lib/schemas/auth";
    import { setError, superForm } from "sveltekit-superforms";
    import { zod4Client } from "sveltekit-superforms/adapters";
    import { Input } from "$lib/components/ui/input/index.js";
    import Eye from "@lucide/svelte/icons/eye";
    import EyeOff from "@lucide/svelte/icons/eye-off";
    import { Switch } from "$lib/components/ui/switch/index.js";
    import { toast } from "svelte-sonner";
    import type { FsSuperForm } from "formsnap";
    import LoaderCircle from "@lucide/svelte/icons/loader-circle";
    import { authClient } from "$lib/auth-client";
    import FormBase from "./form-base.svelte";

    let {
        data
    }: {
        data: SuperValidated<PasswordChangeSchema>;
    } = $props();

    // SPA mode: there is no server action behind this bundle, so `onUpdate` is
    // where the request happens once the client-side validators have passed.
    // svelte-ignore state_referenced_locally
    const form = superForm(data, {
        SPA: true,
        validators: zod4Client(passwordChangeSchema),
        resetForm: true,
        onUpdate: async ({ form }) => {
            if (!form.valid) return;

            if (form.data.oldPassword === form.data.newPassword) {
                setError(form, "newPassword", "New password must be different from old password.");
                return;
            }

            const { error } = await authClient.changePassword({
                currentPassword: form.data.oldPassword,
                newPassword: form.data.newPassword,
                revokeOtherSessions: form.data.revokeSessions
            });

            if (error) {
                form.valid = false;
                toast.error(error.message);
                return;
            }

            toast.success("Password changed successfully.");
        }
    });

    const { form: formData, enhance, delayed } = form;

    const passwordVisibility = $state({
        oldPassword: false,
        newPassword: false,
        confirmNewPassword: false
    });

    type PasswordFieldName = "oldPassword" | "newPassword" | "confirmNewPassword";

    function togglePasswordVisibility(field: keyof typeof passwordVisibility) {
        passwordVisibility[field] = !passwordVisibility[field];
    }
</script>

{#snippet passwordFormField(
    form: FsSuperForm<PasswordChangeSchema>,
    name: PasswordFieldName,
    title: string
)}
    <Form.Field {form} {name}>
        <Form.Control>
            {#snippet children({ props })}
                <Form.Label>{title}</Form.Label>
                <ButtonGroup.Root class="w-full">
                    <Input
                        type={passwordVisibility[name] ? "text" : "password"}
                        autocomplete={name === "oldPassword" ? "current-password" : "new-password"}
                        {...props}
                        bind:value={$formData[name]} />
                    <Button
                        type="button"
                        onclick={() => togglePasswordVisibility(name)}
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
            {/snippet}
        </Form.Control>
        <Form.FieldErrors />
    </Form.Field>
{/snippet}

<FormBase
    title="Change Password"
    description="Update your account password to keep your account secure.">
    {#snippet content()}
        <form method="POST" use:enhance>
            {@render passwordFormField(form, "oldPassword", "Current Password")}
            {@render passwordFormField(form, "newPassword", "New Password")}
            {@render passwordFormField(form, "confirmNewPassword", "Confirm New Password")}

            <Form.Field {form} name="revokeSessions" class="mt-4">
                <Form.Control>
                    {#snippet children({ props })}
                        <div class="flex items-center gap-2">
                            <Switch {...props} bind:checked={$formData.revokeSessions} />
                            <Form.Label for="revokeSessions">Revoke all other sessions</Form.Label>
                        </div>
                    {/snippet}
                </Form.Control>
                <Form.FieldErrors />
            </Form.Field>
        </form>
    {/snippet}

    {#snippet footer()}
        <Form.Button
            variant="secondary"
            size="sm"
            disabled={$delayed}
            onclick={() => {
                form.submit();
            }}>
            {#if $delayed}
                <LoaderCircle class="mr-2 h-5 w-5 animate-spin" />
            {/if}
            Change Password
        </Form.Button>
    {/snippet}
</FormBase>

<script lang="ts">
    import * as Form from "$lib/components/ui/form/index.js";
    import type { SuperValidated } from "sveltekit-superforms";
    import { emailChangeSchema, type EmailChangeSchema } from "$lib/schemas/auth";
    import { superForm } from "sveltekit-superforms";
    import { zod4Client } from "sveltekit-superforms/adapters";
    import { Input } from "$lib/components/ui/input/index.js";
    import { toast } from "svelte-sonner";
    import LoaderCircle from "@lucide/svelte/icons/loader-circle";
    import { invalidateAll } from "$app/navigation";
    import { authClient } from "$lib/auth-client";
    import FormBase from "./form-base.svelte";

    let {
        data
    }: {
        data: SuperValidated<EmailChangeSchema>;
    } = $props();

    // svelte-ignore state_referenced_locally
    const form = superForm(data, {
        SPA: true,
        validators: zod4Client(emailChangeSchema),
        onUpdate: async ({ form }) => {
            if (!form.valid) return;

            const { error } = await authClient.changeEmail({
                new_email: form.data.newEmail,
                current_password: form.data.currentPassword || undefined
            });

            if (error) {
                form.valid = false;
                toast.error(error.message);
                return;
            }

            // riven sends no confirmation mail, so the address is already live —
            // reload so the header stops showing the old one.
            await invalidateAll();
            toast.success("Email changed successfully.");
        }
    });

    const { form: formData, enhance, delayed } = form;
</script>

<FormBase
    title="Change Email"
    description="Manage your email address associated with your account.">
    {#snippet content()}
        <form method="POST" use:enhance>
            <Form.Field {form} name="newEmail">
                <Form.Control>
                    {#snippet children({ props })}
                        <Form.Label for="newEmail">New Email</Form.Label>
                        <Input
                            type="email"
                            placeholder="Your new email address"
                            {...props}
                            bind:value={$formData.newEmail} />
                    {/snippet}
                </Form.Control>
                <Form.FieldErrors />
            </Form.Field>
            <Form.Field {form} name="currentPassword">
                <Form.Control>
                    {#snippet children({ props })}
                        <Form.Label for="currentPassword">Current Password</Form.Label>
                        <Input
                            type="password"
                            placeholder="Leave blank if you sign in without a password"
                            {...props}
                            bind:value={$formData.currentPassword} />
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
            Change Email
        </Form.Button>
    {/snippet}
</FormBase>

<script lang="ts">
    import * as Form from "$lib/components/ui/form/index.js";
    import type { SuperValidated } from "sveltekit-superforms";
    import { changeUserDataSchema, type ChangeUserDataSchema } from "$lib/schemas/auth";
    import { superForm } from "sveltekit-superforms";
    import { untrack } from "svelte";
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
        data: SuperValidated<ChangeUserDataSchema>;
    } = $props();

    const form = untrack(() =>
        superForm(data, {
            SPA: true,
            validators: zod4Client(changeUserDataSchema),
            onUpdate: async ({ form }) => {
                if (!form.valid) return;

                // Blank means "leave this alone", so the payload is built from
                // the filled fields only. `/update-user` rejects an empty body.
                const payload: { username?: string; name?: string; image?: string } = {};
                if (form.data.newUsername.trim()) payload.username = form.data.newUsername.trim();
                if (form.data.newName.trim()) payload.name = form.data.newName.trim();
                if (form.data.newAvatar.trim()) payload.image = form.data.newAvatar.trim();

                if (Object.keys(payload).length === 0) {
                    form.valid = false;
                    toast.error("Fill in at least one field.");
                    return;
                }

                const { error } = await authClient.updateUser(payload);

                if (error) {
                    form.valid = false;
                    toast.error(error.message);
                    return;
                }

                await invalidateAll();
                toast.success("User data updated successfully.");
            }
        })
    );

    const { form: formData, enhance, delayed } = form;
</script>

<FormBase
    title="Update Profile"
    description="Update your user profile information including username, name, and avatar.">
    {#snippet content()}
        <form method="POST" use:enhance>
            <Form.Field {form} name="newUsername">
                <Form.Control>
                    {#snippet children({ props })}
                        <Form.Label for="newUsername">Username</Form.Label>
                        <Input
                            placeholder="Your new username"
                            {...props}
                            bind:value={$formData.newUsername} />
                    {/snippet}
                </Form.Control>
                <Form.FieldErrors />
            </Form.Field>

            <Form.Field {form} name="newName">
                <Form.Control>
                    {#snippet children({ props })}
                        <Form.Label for="newName">Name</Form.Label>
                        <Input
                            placeholder="Your new name"
                            {...props}
                            bind:value={$formData.newName} />
                    {/snippet}
                </Form.Control>
                <Form.FieldErrors />
            </Form.Field>

            <Form.Field {form} name="newAvatar">
                <Form.Control>
                    {#snippet children({ props })}
                        <Form.Label for="newAvatar">Avatar</Form.Label>
                        <Input
                            placeholder="Your new avatar URL"
                            {...props}
                            bind:value={$formData.newAvatar} />
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
            Update profile
        </Form.Button>
    {/snippet}
</FormBase>

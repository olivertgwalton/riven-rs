<script lang="ts">
    import { changeUserDataSchema } from "$lib/schemas/auth";
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
        const result = validateForm(changeUserDataSchema, event.currentTarget);
        errors = result.errors ?? {};
        if (!result.data) return;

        // Blank means "leave this alone", so the payload is built from
        // the filled fields only. `/update-user` rejects an empty body.
        const payload: { username?: string; image?: string } = {};
        if (result.data.newUsername.trim()) payload.username = result.data.newUsername.trim();
        if (result.data.newAvatar.trim()) payload.image = result.data.newAvatar.trim();

        if (Object.keys(payload).length === 0) {
            toast.error("Fill in at least one field.");
            return;
        }

        submitting = true;
        const { error } = await authClient.updateUser(payload);
        submitting = false;

        if (error) {
            toast.error(error.message);
            return;
        }

        await invalidateAll();
        toast.success("User data updated successfully.");
    }
</script>

{#snippet field(name: "newUsername" | "newAvatar", label: string, placeholder: string)}
    <div class="space-y-2">
        <Label for={name} class={errors[name] && "text-destructive"}>{label}</Label>
        <Input id={name} {name} {placeholder} aria-invalid={!!errors[name]} />
        {#if errors[name]}
            <p class="text-destructive text-sm font-medium">{errors[name]}</p>
        {/if}
    </div>
{/snippet}

<FormBase title="Update Profile" description="Update your username and avatar.">
    {#snippet content()}
        <form id="update-user-form" novalidate {onsubmit}>
            {@render field("newUsername", "Username", "Your new username")}
            {@render field("newAvatar", "Avatar", "Your new avatar URL")}
        </form>
    {/snippet}

    {#snippet footer()}
        <Button type="submit" form="update-user-form" variant="secondary" size="sm" disabled={submitting}>
            {#if submitting}
                <LoaderCircle class="mr-2 h-5 w-5 animate-spin" />
            {/if}
            Update profile
        </Button>
    {/snippet}
</FormBase>

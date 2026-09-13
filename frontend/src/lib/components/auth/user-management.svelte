<script lang="ts">
    import { Input } from "$lib/components/ui/input/index.js";
    import { Label } from "$lib/components/ui/label/index.js";
    import { Button } from "$lib/components/ui/button/index.js";
    import * as Table from "$lib/components/ui/table/index.js";
    import { Badge } from "$lib/components/ui/badge/index.js";
    import { toast } from "svelte-sonner";
    import LoaderCircle from "@lucide/svelte/icons/loader-circle";
    import { invalidateAll } from "$app/navigation";
    import { authClient } from "$lib/auth-client";
    import { createUserSchema } from "$lib/schemas/auth";
    import * as dateUtils from "$lib/utils/date";
    import FormBase from "./form-base.svelte";
    import { validateForm } from "./validate";

    type ManagedUser = {
        id: string;
        email?: string | null;
        username?: string | null;
        display_username?: string | null;
        role?: string | null;
        created_at?: string | null;
    };

    let {
        users,
        currentUserId
    }: {
        users: ManagedUser[];
        currentUserId: string;
    } = $props();

    let errors = $state<Record<string, string>>({});
    let submitting = $state(false);

    async function onsubmit(event: SubmitEvent & { currentTarget: HTMLFormElement }) {
        event.preventDefault();
        const form = event.currentTarget;
        const result = validateForm(createUserSchema, form);
        errors = result.errors ?? {};
        if (!result.data) return;

        submitting = true;
        const { error } = await authClient.admin.createUser({
            username: result.data.username,
            email: result.data.email,
            password: result.data.password,
            role: result.data.role
        });
        submitting = false;

        if (error) {
            toast.error(error.message);
            return;
        }

        form.reset();
        await invalidateAll();
        toast.success("User created successfully.");
    }

    let deletingId = $state<string | null>(null);
    let changingRoleId = $state<string | null>(null);

    function formatCreatedAt(value: ManagedUser["created_at"]) {
        if (!value) return "Unknown";
        return dateUtils.formatDate(value) ?? "Unknown";
    }

    async function changeRole(user: ManagedUser, role: string) {
        if (role === (user.role ?? "user")) return;

        changingRoleId = user.id;
        const { error } = await authClient.admin.updateUserRole({ user_id: user.id, role });
        changingRoleId = null;

        if (error) {
            toast.error(error.message);
            await invalidateAll();
            return;
        }

        await invalidateAll();
        toast.success("User role updated.");
    }

    async function deleteUser(user: ManagedUser) {
        const label = user.display_username ?? user.username ?? user.email ?? user.id;
        if (!confirm(`Delete ${label}? This cannot be undone.`)) return;

        deletingId = user.id;
        const { error } = await authClient.admin.removeUser({ user_id: user.id });
        deletingId = null;

        if (error) {
            toast.error(error.message);
            return;
        }

        await invalidateAll();
        toast.success("User deleted successfully.");
    }
</script>

{#snippet field(
    name: "username" | "email" | "password" | "confirmPassword",
    label: string,
    attrs: { type?: string; placeholder?: string; autocomplete?: "new-password" }
)}
    <div class="space-y-2">
        <Label for={name} class={errors[name] && "text-destructive"}>{label}</Label>
        <Input id={name} {name} {...attrs} aria-invalid={!!errors[name]} />
        {#if errors[name]}
            <p class="text-destructive text-sm font-medium">{errors[name]}</p>
        {/if}
    </div>
{/snippet}

<FormBase
    title="User Management"
    description="Create local credential users and choose their access role."
    class="pb-8 md:grid-cols-[12rem_minmax(0,1fr)]">
    {#snippet content()}
        <form
            id="create-user-form"
            novalidate
            {onsubmit}
            class="grid max-w-2xl gap-4 md:grid-cols-2">
            {@render field("username", "Username", { placeholder: "new_user" })}
            {@render field("email", "Email", { type: "email", placeholder: "user@example.com" })}
            {@render field("password", "Password", { type: "password", autocomplete: "new-password" })}
            {@render field("confirmPassword", "Confirm Password", {
                type: "password",
                autocomplete: "new-password"
            })}

            <div class="space-y-2 md:col-span-2">
                <Label for="role" class={errors.role && "text-destructive"}>Role</Label>
                <select
                    id="role"
                    name="role"
                    aria-describedby="role-description"
                    class="border-input bg-background ring-offset-background placeholder:text-muted-foreground focus-visible:ring-ring flex h-9 w-full max-w-48 rounded-md border px-3 py-2 text-sm focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:outline-none disabled:cursor-not-allowed disabled:opacity-50">
                    <option value="user">User</option>
                    <option value="manager">Manager</option>
                    <option value="admin">Admin</option>
                </select>
                <p id="role-description" class="text-muted-foreground text-sm">
                    Managers can maintain the library. Admins can also access settings and users.
                </p>
                {#if errors.role}
                    <p class="text-destructive text-sm font-medium">{errors.role}</p>
                {/if}
            </div>
        </form>

        <div class="border-border/60 mt-6 overflow-x-auto border-y">
            <Table.Root>
                <Table.Header>
                    <Table.Row>
                        <Table.Head>User</Table.Head>
                        <Table.Head>Role</Table.Head>
                        <Table.Head>Created</Table.Head>
                        <Table.Head class="text-right">Actions</Table.Head>
                    </Table.Row>
                </Table.Header>
                <Table.Body>
                    {#each users as user (user.id)}
                        <Table.Row>
                            <Table.Cell>
                                <div class="font-medium">{user.display_username ?? user.username}</div>
                                <div class="text-muted-foreground text-xs">{user.email}</div>
                            </Table.Cell>
                            <Table.Cell>
                                {#if user.id === currentUserId}
                                    <Badge variant={user.role === "admin" ? "default" : "secondary"}>
                                        {user.role ?? "user"}
                                    </Badge>
                                {:else}
                                    <select
                                        value={user.role ?? "user"}
                                        disabled={changingRoleId === user.id}
                                        onchange={(event) =>
                                            changeRole(user, event.currentTarget.value)}
                                        class="border-input bg-background ring-offset-background focus-visible:ring-ring h-8 rounded-md border px-2 py-1 text-sm focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:outline-none disabled:cursor-not-allowed disabled:opacity-50">
                                        <option value="user">User</option>
                                        <option value="manager">Manager</option>
                                        <option value="admin">Admin</option>
                                    </select>
                                {/if}
                            </Table.Cell>
                            <Table.Cell class="text-muted-foreground text-sm">
                                {formatCreatedAt(user.created_at)}
                            </Table.Cell>
                            <Table.Cell class="text-right">
                                <Button
                                    variant="destructive"
                                    size="sm"
                                    disabled={user.id === currentUserId || deletingId === user.id}
                                    onclick={() => deleteUser(user)}>
                                    Delete
                                </Button>
                            </Table.Cell>
                        </Table.Row>
                    {:else}
                        <Table.Row>
                            <Table.Cell colspan={4} class="text-muted-foreground text-center">
                                No users found.
                            </Table.Cell>
                        </Table.Row>
                    {/each}
                </Table.Body>
            </Table.Root>
        </div>
    {/snippet}

    {#snippet footer()}
        <Button
            type="submit"
            form="create-user-form"
            variant="secondary"
            size="sm"
            disabled={submitting}>
            {#if submitting}
                <LoaderCircle class="mr-2 h-5 w-5 animate-spin" />
            {/if}
            Create user
        </Button>
    {/snippet}
</FormBase>

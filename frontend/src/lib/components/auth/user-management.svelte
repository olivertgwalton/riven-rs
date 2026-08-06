<script lang="ts">
    import * as Form from "$lib/components/ui/form/index.js";
    import { Input } from "$lib/components/ui/input/index.js";
    import { Button } from "$lib/components/ui/button/index.js";
    import * as Table from "$lib/components/ui/table/index.js";
    import { Badge } from "$lib/components/ui/badge/index.js";
    import type { SuperValidated } from "sveltekit-superforms";
    import { superForm } from "sveltekit-superforms";
    import { untrack } from "svelte";
    import { zod4Client } from "sveltekit-superforms/adapters";
    import { toast } from "svelte-sonner";
    import LoaderCircle from "@lucide/svelte/icons/loader-circle";
    import { invalidateAll } from "$app/navigation";
    import { authClient } from "$lib/auth-client";
    import { createUserSchema, type CreateUserSchema } from "$lib/schemas/auth";
    import * as dateUtils from "$lib/utils/date";
    import FormBase from "./form-base.svelte";

    type ManagedUser = {
        id: string;
        email?: string | null;
        username?: string | null;
        display_username?: string | null;
        role?: string | null;
        created_at?: string | null;
    };

    let {
        formData: initialForm,
        users,
        currentUserId
    }: {
        formData: SuperValidated<CreateUserSchema>;
        users: ManagedUser[];
        currentUserId: string;
    } = $props();

    const form = untrack(() =>
        superForm(initialForm, {
            SPA: true,
            validators: zod4Client(createUserSchema),
            resetForm: true,
            onUpdate: async ({ form }) => {
                if (!form.valid) return;

                const { error } = await authClient.admin.createUser({
                    username: form.data.username,
                    email: form.data.email,
                    password: form.data.password,
                    role: form.data.role
                });

                if (error) {
                    form.valid = false;
                    toast.error(error.message);
                    return;
                }

                await invalidateAll();
                toast.success("User created successfully.");
            }
        })
    );

    const { form: formData, enhance, delayed } = form;
    let deletingId = $state<string | null>(null);

    function formatCreatedAt(value: ManagedUser["created_at"]) {
        if (!value) return "Unknown";
        return dateUtils.formatDate(value) ?? "Unknown";
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

<FormBase
    title="User Management"
    description="Create local credential users and choose their access role."
    class="pb-8 md:grid-cols-[12rem_minmax(0,1fr)]">
    {#snippet content()}
        <form method="POST" use:enhance class="grid max-w-2xl gap-4 md:grid-cols-2">
            <Form.Field {form} name="username">
                <Form.Control>
                    {#snippet children({ props })}
                        <Form.Label for="username">Username</Form.Label>
                        <Input placeholder="new_user" {...props} bind:value={$formData.username} />
                    {/snippet}
                </Form.Control>
                <Form.FieldErrors />
            </Form.Field>

            <Form.Field {form} name="email">
                <Form.Control>
                    {#snippet children({ props })}
                        <Form.Label for="email">Email</Form.Label>
                        <Input
                            type="email"
                            placeholder="user@example.com"
                            {...props}
                            bind:value={$formData.email} />
                    {/snippet}
                </Form.Control>
                <Form.FieldErrors />
            </Form.Field>

            <Form.Field {form} name="password">
                <Form.Control>
                    {#snippet children({ props })}
                        <Form.Label for="password">Password</Form.Label>
                        <Input
                            type="password"
                            autocomplete="new-password"
                            {...props}
                            bind:value={$formData.password} />
                    {/snippet}
                </Form.Control>
                <Form.FieldErrors />
            </Form.Field>

            <Form.Field {form} name="confirmPassword">
                <Form.Control>
                    {#snippet children({ props })}
                        <Form.Label for="confirmPassword">Confirm Password</Form.Label>
                        <Input
                            type="password"
                            autocomplete="new-password"
                            {...props}
                            bind:value={$formData.confirmPassword} />
                    {/snippet}
                </Form.Control>
                <Form.FieldErrors />
            </Form.Field>

            <Form.Field {form} name="role" class="md:col-span-2">
                <Form.Control>
                    {#snippet children({ props })}
                        <Form.Label for="role">Role</Form.Label>
                        <select
                            {...props}
                            bind:value={$formData.role}
                            class="border-input bg-background ring-offset-background placeholder:text-muted-foreground focus-visible:ring-ring flex h-9 w-full max-w-48 rounded-md border px-3 py-2 text-sm focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:outline-none disabled:cursor-not-allowed disabled:opacity-50">
                            <option value="user">User</option>
                            <option value="manager">Manager</option>
                            <option value="admin">Admin</option>
                        </select>
                    {/snippet}
                </Form.Control>
                <Form.Description>
                    Managers can maintain the library. Admins can also access settings and users.
                </Form.Description>
                <Form.FieldErrors />
            </Form.Field>
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
                                <Badge variant={user.role === "admin" ? "default" : "secondary"}>
                                    {user.role ?? "user"}
                                </Badge>
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
            Create user
        </Form.Button>
    {/snippet}
</FormBase>

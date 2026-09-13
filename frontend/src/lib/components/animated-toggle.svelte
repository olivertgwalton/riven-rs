<script lang="ts">
    import { Button } from "$lib/components/ui/button/index.js";
    import { cn } from "$lib/utils";

    type Option = {
        label: string;
        value: string;
    };

    let {
        options,
        value,
        onchange
    }: {
        options: Option[];
        value: string | undefined;
        onchange: (value: string) => void;
    } = $props();
</script>

<div
    class="relative flex w-fit items-center gap-1 rounded-xl border border-white/10 bg-black/20 p-1 shadow-inner backdrop-blur-md">
    {#each options as option (option.value)}
        {@const active = option.value === value}
        <Button
            variant="ghost"
            size="sm"
            aria-pressed={active}
            class={cn(
                "h-7 flex-1 rounded-lg px-4 text-xs font-bold transition-colors duration-300",
                active
                    ? "bg-primary text-primary-foreground hover:bg-primary hover:text-primary-foreground shadow-lg"
                    : "text-muted-foreground hover:text-foreground hover:bg-transparent"
            )}
            onclick={() => onchange(option.value)}>
            {option.label}
        </Button>
    {/each}
</div>

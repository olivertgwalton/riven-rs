<script lang="ts">
    import { Input } from "$lib/components/ui/input/index.js";

    // `value` is a "YYYY-MM-DD" string ("" when empty) — the native date input's own format.
    let { value = $bindable(""), ...restProps }: { value?: string; "aria-label"?: string } =
        $props();
</script>

<Input type="date" bind:value min="1900-01-01" max="2100-12-31" class="w-full text-sm" {...restProps} />

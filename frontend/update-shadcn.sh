#!/usr/bin/env bash
set -uo pipefail

echo "Reading component folders from src/lib/components/ui..."

if [ ! -d src/lib/components/ui ]; then
    echo "Error: src/lib/components/ui directory does not exist"
    exit 1
fi

components=()
while IFS= read -r dir; do
    components+=("$(basename "$dir")")
done < <(find src/lib/components/ui -mindepth 1 -maxdepth 1 -type d -not -name extras | sort)

if [ ${#components[@]} -eq 0 ]; then
    echo "No component directories found in src/lib/components/ui"
    exit 1
fi

echo "Found ${#components[@]} components to install"

total=${#components[@]}
current=0
successful=0
failed=0

for component in "${components[@]}"; do
    current=$((current + 1))
    echo ""
    echo "[$current/$total] Installing component: $component..."

    if pnpm dlx shadcn-svelte@latest add "$component" --yes --overwrite; then
        echo "✓ Successfully installed $component"
        successful=$((successful + 1))
    else
        echo "✗ Failed to install $component"
        failed=$((failed + 1))
    fi
done

echo ""
echo "Installation complete!"
echo "✓ Successfully installed: $successful components"
if [ "$failed" -gt 0 ]; then
    echo "✗ Failed to install: $failed components"
fi

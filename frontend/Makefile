.PHONY: help install build check lint format codegen clean

# Docker targets are gone: the frontend is no longer published as its own image.
# The root Dockerfile builds `frontend/` directly, so there is nothing here to
# tag or push. From the repo root these are also available as `make frontend-*`.

help:
	@echo "Riven frontend"
	@echo "-------------------------------------------------------------------------"
	@echo "install : Install packages with pnpm"
	@echo "build   : Build the static bundle into ./build (riven serves it)"
	@echo "check   : Typecheck with svelte-check"
	@echo "lint    : Biome checks"
	@echo "format  : Format the code"
	@echo "codegen : Regenerate src/lib/gql from ../schema.graphql"
	@echo "clean   : Remove node_modules, .svelte-kit and build"
	@echo "-------------------------------------------------------------------------"

install:
	@pnpm install

build:
	@pnpm run build

check:
	@pnpm run check

lint:
	@pnpm run lint

format:
	@pnpm run format

codegen:
	@pnpm run codegen

clean:
	@rm -rf node_modules .svelte-kit build

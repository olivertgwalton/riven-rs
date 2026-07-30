<div align="center">
  <a href="https://github.com/rivenmedia/riven">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/rivenmedia/riven/main/assets/riven-light.png">
      <img alt="riven" src="https://raw.githubusercontent.com/rivenmedia/riven/main/assets/riven-dark.png">
    </picture>
  </a>
</div>

<div align="center">
  <a href="https://github.com/rivenmedia/riven/stargazers"><img alt="GitHub Repo stars" src="https://img.shields.io/github/stars/rivenmedia/riven?label=Riven+Backend"></a>
    <a href="https://github.com/rivenmedia/riven-frontend/stargazers"><img alt="GitHub Repo stars" src="https://img.shields.io/github/stars/rivenmedia/riven-frontend?label=Riven+Frontend"></a>
  <a href="https://github.com/rivenmedia/riven/issues"><img alt="Issues" src="https://img.shields.io/github/issues/rivenmedia/riven-frontend" /></a>
  <a href="https://github.com/rivenmedia/riven/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/github/license/rivenmedia/riven-frontend"></a>
  <a href="https://github.com/rivenmedia/riven/graphs/contributors-frontend"><img alt="Contributors" src="https://img.shields.io/github/contributors/rivenmedia/riven-frontend" /></a>
  <a href="https://discord.riven.tv"><img alt="Discord" src="https://img.shields.io/badge/Join%20discord-8A2BE2" /></a>
</div>

## Riven Frontend

This repository contains the frontend for Riven. It is build with [SvelteKit](https://kit.svelte.dev/).

---

## Table Of Contents

- [Running the frontend](#running-the-frontend)
    - [Building the bundle](#building-the-bundle)
    - [Using the Docker image](#using-the-docker-image)
    - [Environment variables](#environment-variables)
- [Developing](#developing)
    - [Architecture](#architecture)
- [Contributing](#contributing)
    - [Submitting Changes](#submitting-changes)
    - [Code Formatting](#code-formatting)
- [Contributors](#contributors)
- [Star History](#star-history)

## Running the frontend

The frontend is a **static bundle**. It has no server of its own: the Riven
backend owns authentication, sessions and all data, and also serves these files.
You need the backend running — find it [here](https://github.com/rivenmedia/riven).

### Building the bundle

Make sure you have pnpm installed on your system.

```bash
pnpm install && pnpm run build
```

The bundle is written to `build/`. Point the backend at that directory with
`RIVEN_STATIC_DIR`, or serve it with any static file server — just make sure
`/graphql` and `/auth` reach the backend on the same origin, so the session
cookie stays first-party.

Because it is a single-page app, unknown paths must fall back to `index.html`
for deep links like `/details/media/123/movie` to resolve.

### Using the Docker image

The image is an artifact carrier built `FROM scratch` — it contains the bundle
at `/dist` and nothing else, so there is no container to run. Copy the bundle
into your own image:

```dockerfile
COPY --from=spoked/riven-frontend:latest /dist /app/static
```

Or extract it locally:

```bash
make extract
```

### Configuration

There is none, and no `.env`. Riven serves this bundle itself, so the API is
always on the same origin and nothing here points at it; auth providers,
passkeys, API keys and CORS are configured on the backend, in riven's own
environment.

The one browser-side knob is console verbosity, set from devtools at runtime
rather than baked into the build:

```js
localStorage.setItem("riven:log-level", "4"); // 0 error … 5 trace, -999 silent
```

---

### Developing

First install dependencies with `pnpm install`. Then build the bundle and point
riven's `RIVEN_STATIC_DIR` at `./build` — there is no dev server. The session
cookie and the WebAuthn relying-party ID are both bound to the origin, so a
`vite dev` server on its own port would be a second origin that production never
has, and passkeys registered against it would not work once deployed.

> [!NOTE]
> It is recommended to use the latest LTS version of Node.js. If using `pnpm` you can run `pnpm env use --global lts` to switch to the latest LTS version.

```bash
pnpm run build
```

#### Architecture

Everything the UI renders comes from the backend's GraphQL API:

- [`src/lib/graphql-client.ts`](./src/lib/graphql-client.ts) — queries and mutations over HTTP, subscriptions over a single shared WebSocket.
- [`src/lib/auth-client.ts`](./src/lib/auth-client.ts) — a thin HTTP client for the backend's `better-auth` routes. Deliberately no `better-auth` package dependency.
- [`src/lib/metadata/parser.ts`](./src/lib/metadata/parser.ts) — local parsing/mapping helpers for TMDB/TVDB detail payloads the backend proxies. Third-party metadata integrations themselves live in the backend.

There is no database, no ORM and no server-side code in this repository. Route
`load` functions are universal (`+page.ts`), not server (`+page.server.ts`), and
there are no form actions or remote functions.

---

## Contributing

We welcome contributions from the community! To ensure a smooth collaboration, please follow these guidelines:

### Submitting Changes

- Open an Issue: For major changes, start by opening an issue to discuss your proposed modifications. This helps us understand your intentions and provide feedback early in the process.

- Pull Requests: Once your changes are ready, submit a pull request. Ensure your code adheres to our coding standards and passes all tests.

### Code Formatting

- **Frontend**: We use [Prettier](https://prettier.io/) for code formatting. Run prettier on your code before submitting. You can use the following command:

```bash
pnpm run format
```

- **Line Endings**: Use CRLF line endings unless the file is a shell script or another format that requires LF line endings.

---

## Contributors

Thanks goes to these wonderful people

<a href="https://github.com/rivenmedia/riven-frontend/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=rivenmedia/riven-frontend" />
</a>

---

## Star History

<a href="https://www.star-history.com/#rivenmedia/riven&rivenmedia/riven-frontend&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=rivenmedia/riven,rivenmedia/riven-frontend&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=rivenmedia/riven,rivenmedia/riven-frontend&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=rivenmedia/riven,rivenmedia/riven-frontend&type=date&legend=top-left" />
 </picture>
</a>

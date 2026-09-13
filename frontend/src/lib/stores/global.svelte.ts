import { MediaQuery } from "svelte/reactivity";

export const sidebar = $state({ open: false });

export const isMobile = new MediaQuery("max-width: 767px");

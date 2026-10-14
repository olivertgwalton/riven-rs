import { redirect } from "@sveltejs/kit";
import { resolve } from "$app/paths";
import type { PageLoad } from "./$types";

export const load: PageLoad = async ({ params }) => {
	redirect(
		301,
		resolve("/(protected)/details/entity/[id]/[type]", {
			id: params.id,
			type: "company",
		}),
	);
};

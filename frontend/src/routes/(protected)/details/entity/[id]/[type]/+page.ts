import type { PageLoad } from "./$types";
import { error } from "@sveltejs/kit";
import {
	fetchCompanyDetails,
	fetchPersonDetails,
} from "$lib/services/media-details";

function isNotFound(err: unknown) {
	return err instanceof Error && err.message.includes("status 404 Not Found");
}

export const load: PageLoad = async ({ params, url }) => {
	const { id, type } = params;

	if (!id || Number.isNaN(Number(id))) {
		error(400, "Invalid ID");
	}
	if (type !== "person" && type !== "company") {
		error(404, "Invalid entity type");
	}

	try {
		const entity =
			type === "person"
				? await fetchPersonDetails(
						Number(id),
						url.searchParams.get("indexer") ?? undefined,
					)
				: await fetchCompanyDetails(Number(id));

		return { entity };
	} catch (err) {
		if (isNotFound(err)) {
			error(404, type === "person" ? "Person not found" : "Company not found");
		}
		throw err;
	}
};

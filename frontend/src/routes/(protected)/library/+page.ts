import type { PageLoad } from "./$types";
import { gqlClient } from "$lib/graphql-client";
import { createScopedLogger } from "$lib/logger";
import {
	ITEMS_QUERY,
	itemsQueryVariables,
	parseLibrarySearch,
	transformItems,
	type GqlItemsPage,
} from "./items";

const logger = createScopedLogger("library-page-server");
const LIBRARY_ITEMS_DEPENDENCY = "riven:library-items";

interface FilterOption {
	value: string;
	label: string;
}

const FILTER_ENUMS_QUERY = `
    query LibraryFilterEnums {
        mediaItemType: __type(name: "MediaItemType") {
            enumValues {
                name
            }
        }
        mediaItemState: __type(name: "MediaItemState") {
            enumValues {
                name
            }
        }
    }
`;

function labelFromEnum(value: string): string {
	return value
		.replace(/_/g, " ")
		.replace(/([a-z])([A-Z])/g, "$1 $2")
		.replace(
			/\w\S*/g,
			(word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase(),
		);
}

function typeValueFromEnum(value: string): string {
	return value.toLowerCase();
}

export const load: PageLoad = async (event) => {
	// Auth gating lives in the protected layout.
	event.depends(LIBRARY_ITEMS_DEPENDENCY);

	const search = parseLibrarySearch(event.url.searchParams);

	// Both queries start in parallel. Bundling into a single streaming Promise
	// means navigation is instant — the page renders immediately and content fills in.
	const filterEnumsTask = gqlClient<{
		mediaItemType?: { enumValues?: { name: string }[] } | null;
		mediaItemState?: { enumValues?: { name: string }[] } | null;
	}>(FILTER_ENUMS_QUERY, undefined);

	const itemsTask = gqlClient<{ items: GqlItemsPage }>(
		ITEMS_QUERY,
		itemsQueryVariables(search),
	);

	const pageData = Promise.all([filterEnumsTask, itemsTask])
		.then(([filterData, itemsData]) => {
			const typeEnums =
				filterData.mediaItemType?.enumValues?.map((e) => e.name) ?? [];
			const stateEnums =
				filterData.mediaItemState?.enumValues?.map((e) => e.name) ?? [];

			const typeOptions: FilterOption[] = typeEnums.map((value) => ({
				value: typeValueFromEnum(value),
				label: labelFromEnum(value),
			}));
			const stateOptions: FilterOption[] = [
				{ value: "All", label: "All" },
				...stateEnums.map((value) => ({ value, label: labelFromEnum(value) })),
			];

			return {
				items: transformItems(itemsData.items.items),
				page: itemsData.items.page,
				totalPages: itemsData.items.totalPages,
				limit: itemsData.items.limit,
				totalItems: itemsData.items.totalItems,
				typeOptions,
				stateOptions,
			};
		})
		.catch((err) => {
			logger.error("Failed to fetch library data:", err);
			return null;
		});

	return {
		search,
		pageData,
	};
};

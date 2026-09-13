import type {
	CustomProfile,
	SettingFieldDef,
	SettingsSection,
	SetupData,
	SetupGeneralSection,
	SetupGroup,
	SetupPluginSection,
	Step,
} from "./types";

export function pluginStatus(section: {
	enabled?: boolean | null;
	valid?: boolean | null;
}): {
	label: string;
	variant: "default" | "secondary";
} {
	if (!section.enabled) return { label: "Inactive", variant: "secondary" };
	if (section.valid) return { label: "Active", variant: "default" };
	return { label: "Invalid", variant: "secondary" };
}

export const settingsSwitchClass =
	"data-[state=checked]:bg-primary data-[state=unchecked]:bg-input dark:data-[state=unchecked]:bg-input/80 [&_[data-slot=switch-thumb]]:translate-x-0 [&_[data-state=checked][data-slot=switch-thumb]]:translate-x-[calc(100%-2px)] rtl:[&_[data-state=checked][data-slot=switch-thumb]]:-translate-x-[calc(100%-2px)] dark:[&_[data-state=unchecked][data-slot=switch-thumb]]:bg-foreground dark:[&_[data-state=checked][data-slot=switch-thumb]]:bg-primary-foreground";

// Presentational chrome for the non-plugin setup steps; the plugin-group steps
// are driven entirely by the backend `setupGroups` query.
const setupStepMeta = {
	welcome: {
		label: "Welcome",
		description: "Quick overview before you connect providers.",
	},
	quality: {
		label: "Quality",
		description: "Choose profiles and instance defaults.",
	},
	finish: {
		label: "Review",
		description: "Check readiness and finish setup.",
	},
} satisfies Record<"welcome" | "quality" | "finish", Omit<Step, "id">>;

type SetupState = {
	general: SettingsSection | null;
	plugins: SettingsSection[];
	customProfiles: CustomProfile[];
};

/** Split the backend sections into the general section + plugin sections, deep-copying values for local editing. */
export function createSetupState(data: SetupData): SetupState {
	const sections = (data.sections ?? []).map((section) => ({
		...section,
		values: { ...(section.values as Record<string, unknown>) },
	}));
	return {
		general: sections.find((section) => section.kind === "general") ?? null,
		plugins: sections.filter((section) => section.kind === "plugin"),
		customProfiles: (data.customProfiles ?? []).map((profile) => ({
			...profile,
		})),
	};
}

/**
 * Group plugin sections by their backend `category`, in the backend-defined
 * `setupGroups` order, each sorted by title. Sections with an unknown/missing
 * category land in a trailing "Other" group; empty groups are dropped.
 */
export function groupPluginsByCategory(
	plugins: SettingsSection[],
	groups: SetupGroup[],
): (SetupGroup & { sections: SettingsSection[] })[] {
	const byTitle = (a: SettingsSection, b: SettingsSection) =>
		a.title.localeCompare(b.title);
	const knownIds = new Set(groups.map((group) => group.id));
	const other = plugins.filter((s) => !s.category || !knownIds.has(s.category));

	return [
		...groups.map((group) => ({
			...group,
			sections: plugins.filter((s) => s.category === group.id),
		})),
		{
			id: "other",
			title: "Other",
			description: "Additional plugins.",
			sections: other,
		},
	]
		.map((group) => ({ ...group, sections: group.sections.sort(byTitle) }))
		.filter((group) => group.sections.length > 0);
}

/** Plugin groups as setup steps, each plugin carrying its status badge + saving flag. */
export function buildPluginSections(
	plugins: SettingsSection[],
	savingMap: Record<string, boolean>,
	groups: SetupGroup[],
): SetupPluginSection[] {
	return groupPluginsByCategory(plugins, groups).map(
		({ sections, ...group }) => ({
			...group,
			plugins: sections.map((section) => ({
				section,
				badge: pluginStatus(section),
				saving: savingMap[section.id] ?? false,
			})),
		}),
	);
}

/** Group general-settings fields by their backend-provided `section` label. */
export function buildGeneralSections(
	schema: SettingFieldDef[],
): SetupGeneralSection[] {
	const sections = new Map<string, SetupGeneralSection>();

	for (const field of schema) {
		const title = field.section ?? "General";
		const existing = sections.get(title);

		if (existing) {
			existing.fields.push(field);
			continue;
		}

		sections.set(title, { title, description: "", fields: [field] });
	}

	return [...sections.values()];
}

export function buildSetupSteps(pluginSections: SetupPluginSection[]): Step[] {
	return [
		{ id: "welcome", ...setupStepMeta.welcome },
		...pluginSections.map((section) => ({
			id: section.id,
			label: section.title,
			description: section.description,
		})),
		{ id: "quality", ...setupStepMeta.quality },
		{ id: "finish", ...setupStepMeta.finish },
	];
}

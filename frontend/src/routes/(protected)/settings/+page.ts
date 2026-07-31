import type { PageLoad } from "./$types";
import { error } from "@sveltejs/kit";
import { gqlClient } from "$lib/graphql-client";
import { requireCapability } from "$lib/permissions";
import type {
	CustomProfile,
	InstanceStatus,
	QualityProfile,
	SettingFieldDef,
	SettingsSection,
	SetupGroup,
} from "$lib/components/settings/types";

// Initial data is loaded server-side (SSR); every mutation runs client-side via
// `gqlClient` (see operations.ts), matching the rest of the app.
const RANK_PROFILES_QUERY = `query { qualityProfiles customProfiles }`;
const RANK_SETTINGS_SCHEMA_QUERY = `query { rankSettingsSchema }`;
const DEFAULT_RANK_PROFILE_QUERY = `query { defaultRankProfile }`;
const SETUP_GROUPS_QUERY = `query { setupGroups { id title description } }`;
const INSTANCE_STATUS_QUERY = `
    query {
        instanceStatus {
            setupCompleted
            readyToComplete
            enabledValidPluginCount
            enabledProfileCount
            blockers
        }
    }
`;
const SETTINGS_SECTIONS_QUERY = `
    query {
        settingsSections {
            id
            title
            kind
            schema
            values
            category
            enabled
            valid
            configured
            missingRequiredFields
            version
        }
    }
`;

export const load: PageLoad = async ({ parent }) => {
	// Capabilities come from the protected layout, which resolved them from
	// the backend.
	const { permissions } = await parent();
	requireCapability(permissions, "MANAGE_SETTINGS");

	try {
		const [
			sectionsData,
			rankProfilesData,
			rankSchemaData,
			setupGroupsData,
			instanceStatusData,
			defaultProfileData,
		] = await Promise.all([
			gqlClient<{ settingsSections: SettingsSection[] }>(
				SETTINGS_SECTIONS_QUERY,
				{},
			).catch(() => ({ settingsSections: [] })),
			gqlClient<{
				qualityProfiles: QualityProfile[];
				customProfiles: CustomProfile[];
			}>(RANK_PROFILES_QUERY, {}).catch(() => ({
				qualityProfiles: [],
				customProfiles: [],
			})),
			gqlClient<{ rankSettingsSchema: SettingFieldDef[] }>(
				RANK_SETTINGS_SCHEMA_QUERY,
				{},
			).catch(() => ({ rankSettingsSchema: [] })),
			gqlClient<{ setupGroups: SetupGroup[] }>(SETUP_GROUPS_QUERY, {}).catch(
				() => ({
					setupGroups: [],
				}),
			),
			gqlClient<{ instanceStatus: InstanceStatus }>(
				INSTANCE_STATUS_QUERY,
				{},
			).catch(() => ({
				instanceStatus: {
					setupCompleted: false,
					readyToComplete: false,
					enabledValidPluginCount: 0,
					enabledProfileCount: 0,
					blockers: [],
				},
			})),
			gqlClient<{
				defaultRankProfile: {
					name: string | null;
					settings: Record<string, unknown>;
				};
			}>(DEFAULT_RANK_PROFILE_QUERY, {}).catch(() => ({
				defaultRankProfile: { name: null, settings: {} },
			})),
		]);

		return {
			sections: sectionsData.settingsSections,
			rankSettings: defaultProfileData.defaultRankProfile.settings,
			rankSettingsSchema: rankSchemaData.rankSettingsSchema,
			initialProfileName: defaultProfileData.defaultRankProfile.name,
			qualityProfiles: rankProfilesData.qualityProfiles ?? [],
			customProfiles: rankProfilesData.customProfiles ?? [],
			setupGroups: setupGroupsData.setupGroups,
			instanceStatus: instanceStatusData.instanceStatus,
		};
	} catch {
		error(500, "Failed to load settings");
	}
};

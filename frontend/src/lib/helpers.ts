/**
 * Small presentation helpers with no better home.
 *
 * Date handling lives in `$lib/utils/date` — import it directly rather than
 * re-exporting through here.
 */

export const formatBytes = (bytes: number | null | undefined): string => {
	if (bytes === null || bytes === undefined) return "N/A";
	if (bytes === 0) return "0 B";
	const k = 1024;
	const sizes = ["B", "KB", "MB", "GB", "TB"];
	const i = Math.floor(Math.log(bytes) / Math.log(k));
	return `${parseFloat((bytes / k ** i).toFixed(2))} ${sizes[i]}`;
};

export const getServiceDisplayName = (service: string): string => {
	switch (service.toLowerCase()) {
		case "realdebrid":
			return "Real-Debrid";
		case "torbox":
			return "TorBox";
		case "alldebrid":
			return "AllDebrid";
		default:
			return service;
	}
};

/** Pick a bucket colour for `value` on a 0..max scale. */
export const getColor = (colors: string[], max: number, value: number) => {
	if (!value) return colors[0];
	const p = (value / max) * (colors.length - 1);
	return colors[Math.ceil(p)];
};

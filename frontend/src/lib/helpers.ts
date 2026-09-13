/**
 * Small presentation helpers with no better home.
 *
 * Date handling lives in `$lib/utils/date` — import it directly rather than
 * re-exporting through here.
 */

export const formatBytes = (bytes: number | null | undefined): string => {
	if (bytes === null || bytes === undefined) return "N/A";
	if (bytes <= 0) return "0 B";
	const sizes = ["B", "KB", "MB", "GB", "TB", "PB"];
	const i = Math.min(
		sizes.length - 1,
		Math.floor(Math.log(bytes) / Math.log(1024)),
	);
	return `${parseFloat((bytes / 1024 ** i).toFixed(2))} ${sizes[i]}`;
};

const rtf = new Intl.RelativeTimeFormat("en", { numeric: "auto" });
const RELATIVE_UNITS: [Intl.RelativeTimeFormatUnit, number][] = [
	["year", 31_536_000],
	["month", 2_592_000],
	["week", 604_800],
	["day", 86_400],
	["hour", 3_600],
	["minute", 60],
];

/** "5 minutes ago" / "in 2 hours" / "now" for a Date or epoch-ms timestamp. */
export function relativeTime(date: Date | number | string): string {
	const secs = (new Date(date).getTime() - Date.now()) / 1000;
	for (const [unit, size] of RELATIVE_UNITS) {
		if (Math.abs(secs) >= size)
			return rtf.format(Math.trunc(secs / size), unit);
	}
	return rtf.format(0, "second");
}

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

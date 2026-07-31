declare global {
	namespace App {}

	// Navigator User-Agent Client Hints API
	interface NavigatorUABrandVersion {
		readonly brand: string;
		readonly version: string;
	}

	interface NavigatorUAData {
		readonly platform: string;
		readonly mobile: boolean;
		readonly brands: ReadonlyArray<NavigatorUABrandVersion>;
	}

	interface Navigator {
		readonly userAgentData?: NavigatorUAData;
	}
}

export {};

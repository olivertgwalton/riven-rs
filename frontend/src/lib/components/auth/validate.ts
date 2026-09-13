import type { z } from "zod";

/**
 * Validate a submitted form's fields against `schema`. On failure, `errors`
 * holds the first message per top-level field name.
 */
export function validateForm<T extends z.ZodType>(
	schema: T,
	form: HTMLFormElement,
):
	| { data: z.infer<T>; errors: null }
	| { data: null; errors: Record<string, string> } {
	const result = schema.safeParse(Object.fromEntries(new FormData(form)));
	if (result.success) return { data: result.data, errors: null };

	const errors: Record<string, string> = {};
	for (const issue of result.error.issues) {
		errors[String(issue.path[0])] ??= issue.message;
	}
	return { data: null, errors };
}

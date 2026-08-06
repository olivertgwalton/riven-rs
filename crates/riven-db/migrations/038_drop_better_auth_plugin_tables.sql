-- Auth moved from the better-auth-rs library to a native implementation.
-- These tables belonged to better-auth plugins riven never exposed in its UI
-- (API keys, TOTP two-factor, organizations, OAuth device flow); nothing reads
-- them any more. The five tables the native implementation uses —
-- auth_users, auth_sessions, auth_accounts, auth_verifications, passkeys —
-- are untouched.
DROP TABLE IF EXISTS "device_code" CASCADE;
DROP TABLE IF EXISTS "invitation" CASCADE;
DROP TABLE IF EXISTS "member" CASCADE;
DROP TABLE IF EXISTS "organization" CASCADE;
DROP TABLE IF EXISTS "two_factor" CASCADE;
DROP TABLE IF EXISTS "api_keys" CASCADE;

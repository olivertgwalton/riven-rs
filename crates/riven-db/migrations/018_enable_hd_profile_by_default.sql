-- Enable the Ultra HD built-in profile by default so new installs work out of the box.
UPDATE ranking_profiles SET enabled = true WHERE name = 'ultra_hd' AND is_builtin = true AND enabled = false;

use async_graphql::{Context, Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum UserRole {
    User,
    Manager,
    Admin,
}

#[derive(Clone, Debug)]
pub struct RequestAuth {
    pub role: UserRole,
}

impl RequestAuth {
    pub fn trusted_api_key() -> Self {
        Self {
            role: UserRole::Admin,
        }
    }
}

fn get_request_auth<'ctx>(ctx: &'ctx Context<'_>) -> Result<&'ctx RequestAuth> {
    ctx.data::<RequestAuth>()
        .map_err(|_| Error::new("Missing request auth context"))
}

fn require_role(ctx: &Context<'_>, minimum_role: UserRole) -> Result<()> {
    let auth = get_request_auth(ctx)?;

    if auth.role < minimum_role {
        return Err(Error::new("Forbidden"));
    }

    Ok(())
}

pub fn require_request_access(ctx: &Context<'_>) -> Result<()> {
    require_role(ctx, UserRole::User)
}

pub fn require_library_access(ctx: &Context<'_>) -> Result<()> {
    require_role(ctx, UserRole::Manager)
}

pub fn require_settings_access(ctx: &Context<'_>) -> Result<()> {
    require_role(ctx, UserRole::Admin)
}

//! Bearer-token dispatch per plan §5
//!
//! Phase 2 will implement the full token-based routing logic.
//! This module is currently a stub.

use http::header::HeaderMap;

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum TokenKind {
    Client,
    Admin,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct AuthContext {
    pub token_kind: TokenKind,
    pub token: Option<String>,
}

#[allow(dead_code)]
pub fn classify_token(headers: &HeaderMap) -> Option<AuthContext> {
    let auth_header = headers.get("authorization")?.to_str().ok()?;
    let token = auth_header.strip_prefix("Bearer ")?;

    Some(AuthContext {
        token_kind: TokenKind::Client,
        token: Some(token.to_string()),
    })
}

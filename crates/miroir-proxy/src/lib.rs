// Allow camelCase field names to match Meilisearch API format
#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]
#![allow(unused_variables)]
#![allow(dead_code)]
#![cfg_attr(test, allow(clippy::useless_vec))]
#![cfg_attr(test, allow(clippy::too_many_arguments))]

pub mod admin_session;
pub mod admin_ui;
pub mod auth;
pub mod client;
pub mod error_response;
pub mod middleware;
pub mod otel;
pub mod routes;
pub mod scoped_key_rotation;

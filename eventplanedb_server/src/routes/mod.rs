use eventplanedb_thread_worker::job_context::JobContext;
use tracing::Span;

pub mod delete;
pub mod disable_client;
pub mod disable_share;
pub mod disable_user;
pub mod read;
pub mod share;
pub mod subscribe;
pub mod write;

pub fn record_span_fields(context: &JobContext) {
    let span = Span::current();
    span.record("client_id", context.current_client_id);
    span.record("user_id", context.current_user_id.as_deref().unwrap_or("no_user_id"));
    span.record("org_id", context.current_org_id.as_deref().unwrap_or("no_org_id"));
    span.record("server_time", context.server_time);
}

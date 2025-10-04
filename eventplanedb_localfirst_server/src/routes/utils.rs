use tracing::Span;

use crate::job_context::JobContext;

pub fn record_span_fields(context: &JobContext) {
    let span = Span::current();
    span.record("client_id", context.client_id);
    span.record("user_id", context.user_id);
    span.record("org_id", context.org_id);
    span.record("server_time", context.server_time);
}

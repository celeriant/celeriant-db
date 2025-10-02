#[derive(Clone)]
pub struct JobContext {
    pub org_id: u128,
    pub aggregate_type_id: u128,
    pub aggregate_id: u128,
    pub client_id: u128,
    pub user_id: Option<u128>,
    pub server_time: u64,
}

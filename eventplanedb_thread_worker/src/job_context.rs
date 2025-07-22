

#[derive(Clone)]
pub struct JobContext {
    pub file_path: String,
    pub current_client_id: u128,
    pub current_user_id: Option<String>,
    pub server_time: u64,
}
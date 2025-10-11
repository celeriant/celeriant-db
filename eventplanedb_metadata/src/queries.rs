use crate::{MetadataError, store::MetadataStore};
use std::time::{SystemTime, UNIX_EPOCH};

impl MetadataStore {
    pub async fn use_share_if_required(
        &self,
        client_id: u128,
        user_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        share_id: Option<u128>,
        required_access_level: u8,
    ) -> Result<bool, MetadataError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let conn_aggregate = self
            .open_aggregate_connection(org_id, aggregate_type_id, aggregate_id)
            .await?;

        // First, check existing access for user or client
        let mut existing_access_level: Option<u8> = None;

        if let Some(user_id) = user_id {
            // Check if user has direct access
            let mut stmt = conn_aggregate
                .prepare("SELECT access_level FROM users_and_clients WHERE id = ? AND is_user = 1")
                .await?;
            let mut rows = stmt.query([user_id.to_le_bytes().as_slice()]).await?;

            if let Some(row) = rows.next().await? {
                existing_access_level = Some(row.get::<i64>(0)? as u8);
            }
        } else {
            // Check if client has direct access
            let mut stmt = conn_aggregate
                .prepare("SELECT access_level FROM users_and_clients WHERE id = ? AND is_user = 0")
                .await?;
            let mut rows = stmt.query([client_id.to_le_bytes().as_slice()]).await?;

            if let Some(row) = rows.next().await? {
                existing_access_level = Some(row.get::<i64>(0)? as u8);
            }
        }

        // Check if existing access is sufficient (lower number = higher access)
        if let Some(access_level) = existing_access_level {
            if access_level <= required_access_level {
                return Ok(true);
            }
        }

        // No sufficient access found, check if we have a share_id to try
        let Some(share_id) = share_id else {
            return Ok(false);
        };

        // Validate the share link
        let mut stmt = conn_aggregate
            .prepare(
                "SELECT access_level, expires_at, is_single_use, use_count, disabled_at 
             FROM share_links 
             WHERE id = ?",
            )
            .await?;
        let mut rows = stmt.query([share_id.to_le_bytes().as_slice()]).await?;

        let Some(row) = rows.next().await? else {
            return Ok(false); // Share link doesn't exist
        };

        let share_access_level = row.get::<i64>(0)? as u8;
        let expires_at: Option<i64> = row.get(1)?;
        let is_single_use: bool = row.get::<i64>(2)? != 0;
        let use_count: i64 = row.get(3)?;
        let disabled_at: Option<i64> = row.get(4)?;

        // Check if share link is valid
        if disabled_at.is_some() {
            return Ok(false); // Share link is disabled
        }

        if let Some(expires_at) = expires_at {
            if now > expires_at {
                return Ok(false); // Share link has expired
            }
        }

        if is_single_use && use_count > 0 {
            return Ok(false); // Single-use share link already used
        }

        if share_access_level > required_access_level {
            return Ok(false); // Share link doesn't provide sufficient access
        }

        // Share link is valid and provides sufficient access, use it
        // Update usage count
        let mut stmt = conn_aggregate
            .prepare("UPDATE share_links SET use_count = use_count + 1 WHERE id = ?")
            .await?;
        stmt.execute([share_id.to_le_bytes().as_slice()]).await?;

        // Grant access to the user/client
        let is_user = user_id.is_some();
        let entity_id = user_id.unwrap_or(client_id);

        let mut stmt = conn_aggregate
            .prepare(
                "INSERT INTO users_and_clients 
             (id, is_user, access_level, created_at, modified_at, granted_from_share_id)
             VALUES (?, ?, ?, ?, ?, ?)",
            )
            .await?;
        stmt.execute((
            entity_id.to_le_bytes().as_slice(),
            is_user,
            share_access_level,
            now,
            now,
            share_id.to_le_bytes().as_slice(),
        ))
        .await?;

        // If user is logged in, also update user and org databases
        if let Some(user_id) = user_id {
            // Update user database
            let conn_user = self.open_user_connection(user_id).await?;
            let mut stmt = conn_user.prepare(
                "INSERT INTO user_aggregate_access 
                 (org_id, aggregate_type_id, aggregate_id, access_level, created_at, modified_at, granted_from_share_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?)"
            ).await?;
            stmt.execute((
                org_id.to_le_bytes().as_slice(),
                aggregate_type_id.to_le_bytes().as_slice(),
                aggregate_id.to_le_bytes().as_slice(),
                share_access_level,
                now,
                now,
                share_id.to_le_bytes().as_slice(),
            ))
            .await?;

            // Update org database
            let conn_org = self.open_org_connection(org_id).await?;
            let mut stmt = conn_org.prepare(
                "INSERT INTO user_aggregate_access 
                 (user_id, aggregate_type_id, aggregate_id, access_level, created_at, modified_at, granted_from_share_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?)"
            ).await?;
            stmt.execute((
                user_id.to_le_bytes().as_slice(),
                aggregate_type_id.to_le_bytes().as_slice(),
                aggregate_id.to_le_bytes().as_slice(),
                share_access_level,
                now,
                now,
                share_id.to_le_bytes().as_slice(),
            ))
            .await?;
        }

        Ok(true)
    }

    pub async fn disable_share_link(
        &self,
        client_id: u128,
        user_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        share_id: u128,
    ) -> Result<bool, MetadataError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let conn_aggregate = self
            .open_aggregate_connection(org_id, aggregate_type_id, aggregate_id)
            .await?;

        // Check if share link exists and is not already disabled
        let mut stmt = conn_aggregate
            .prepare("SELECT id FROM share_links WHERE id = ? AND disabled_at IS NULL")
            .await?;
        let mut rows = stmt.query([share_id.to_le_bytes().as_slice()]).await?;

        if rows.next().await?.is_none() {
            return Ok(false); // Share link doesn't exist or is already disabled
        }

        // Disable the share link
        let mut stmt = conn_aggregate
            .prepare(
                "UPDATE share_links 
                 SET disabled_at = ?, disabled_by_client_id = ?, disabled_by_user_id = ?
                 WHERE id = ?",
            )
            .await?;

        stmt.execute((
            now,
            client_id.to_le_bytes().as_slice(),
            user_id.map(|id| id.to_le_bytes().to_vec()),
            share_id.to_le_bytes().as_slice(),
        ))
        .await?;

        Ok(true)
    }

    pub async fn disable_client(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        for_client_id: u128,
    ) -> Result<bool, MetadataError> {
        let conn_aggregate = self
            .open_aggregate_connection(org_id, aggregate_type_id, aggregate_id)
            .await?;

        // Check if client exists and is not a user
        let mut stmt = conn_aggregate
            .prepare("SELECT id FROM users_and_clients WHERE id = ? AND is_user = 0")
            .await?;
        let mut rows = stmt.query([for_client_id.to_le_bytes().as_slice()]).await?;

        if rows.next().await?.is_none() {
            return Ok(false); // Client doesn't exist
        }

        // Remove the client's access by deleting the record
        let mut stmt = conn_aggregate
            .prepare("DELETE FROM users_and_clients WHERE id = ? AND is_user = 0")
            .await?;

        let rows_affected = stmt
            .execute([for_client_id.to_le_bytes().as_slice()])
            .await?;

        Ok(rows_affected > 0)
    }

    pub async fn disable_user(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        for_user_id: u128,
    ) -> Result<bool, MetadataError> {
        let conn_aggregate = self
            .open_aggregate_connection(org_id, aggregate_type_id, aggregate_id)
            .await?;

        // Check if user exists
        let mut stmt = conn_aggregate
            .prepare("SELECT id FROM users_and_clients WHERE id = ? AND is_user = 1")
            .await?;
        let mut rows = stmt.query([for_user_id.to_le_bytes().as_slice()]).await?;

        if rows.next().await?.is_none() {
            return Ok(false); // User doesn't exist in this aggregate
        }

        // Remove the user's access from the aggregate database
        let mut stmt = conn_aggregate
            .prepare("DELETE FROM users_and_clients WHERE id = ? AND is_user = 1")
            .await?;
        let rows_affected = stmt.execute([for_user_id.to_le_bytes().as_slice()]).await?;

        if rows_affected == 0 {
            return Ok(false);
        }

        // Also remove from user database if it exists
        if let Ok(conn_user) = self.open_user_connection(for_user_id).await {
            let mut stmt = conn_user
                .prepare(
                    "DELETE FROM user_aggregate_access 
                 WHERE org_id = ? AND aggregate_type_id = ? AND aggregate_id = ?",
                )
                .await?;
            stmt.execute((
                org_id.to_le_bytes().as_slice(),
                aggregate_type_id.to_le_bytes().as_slice(),
                aggregate_id.to_le_bytes().as_slice(),
            ))
            .await?;
        }

        // Also remove from org database
        let conn_org = self.open_org_connection(org_id).await?;
        let mut stmt = conn_org
            .prepare(
                "DELETE FROM user_aggregate_access 
             WHERE user_id = ? AND aggregate_type_id = ? AND aggregate_id = ?",
            )
            .await?;
        stmt.execute((
            for_user_id.to_le_bytes().as_slice(),
            aggregate_type_id.to_le_bytes().as_slice(),
            aggregate_id.to_le_bytes().as_slice(),
        ))
        .await?;

        Ok(true)
    }

    pub async fn create_share_link(
        &self,
        client_id: u128,
        user_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
        share_id: u128,
        access_level: u8,
        expires_on: Option<u64>,
        is_single_use: bool,
    ) -> Result<(), MetadataError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let conn_aggregate = self
            .open_aggregate_connection(org_id, aggregate_type_id, aggregate_id)
            .await?;

        let expires_at = expires_on.map(|e| e as i64);

        let mut stmt = conn_aggregate
            .prepare(
                "INSERT INTO share_links 
                 (id, created_by_client_id, created_by_user_id, created_at, access_level, expires_at, is_single_use, use_count, disabled_at, disabled_by_client_id, disabled_by_user_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL)",
            )
            .await?;

        stmt.execute((
            share_id.to_le_bytes().as_slice(),
            client_id.to_le_bytes().as_slice(),
            user_id.map(|id| id.to_le_bytes().to_vec()),
            now,
            access_level,
            expires_at,
            is_single_use,
        ))
        .await?;

        Ok(())
    }

    pub async fn give_owner_access_for_new_aggregate(
        &self,
        client_id: u128,
        user_id: Option<u128>,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> Result<(), MetadataError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let conn_aggregate = self
            .open_aggregate_connection(org_id, aggregate_type_id, aggregate_id)
            .await?;

        const OWNER_ACCESS_LEVEL: u8 = 0; // Owner has highest access (lowest number)

        // If user is logged in, also grant them owner access and update cross-reference tables
        if let Some(user_id) = user_id {
            // Grant owner access to the user in the aggregate database
            let mut stmt = conn_aggregate
                .prepare(
                    "INSERT INTO users_and_clients 
                     (id, is_user, access_level, created_at, modified_at)
                     VALUES (?, ?, ?, ?, ?)",
                )
                .await?;
            stmt.execute((
                user_id.to_le_bytes().as_slice(),
                1i64, // is_user = true
                OWNER_ACCESS_LEVEL,
                now,
                now,
            ))
            .await?;

            // Update user database
            let conn_user = self.open_user_connection(user_id).await?;
            let mut stmt = conn_user
                .prepare(
                    "INSERT INTO user_aggregate_access 
                 (org_id, aggregate_type_id, aggregate_id, access_level, created_at, modified_at)
                 VALUES (?, ?, ?, ?, ?, ?)",
                )
                .await?;
            stmt.execute((
                org_id.to_le_bytes().as_slice(),
                aggregate_type_id.to_le_bytes().as_slice(),
                aggregate_id.to_le_bytes().as_slice(),
                OWNER_ACCESS_LEVEL,
                now,
                now,
            ))
            .await?;

            // Update org database
            let conn_org = self.open_org_connection(org_id).await?;
            let mut stmt = conn_org
                .prepare(
                    "INSERT INTO user_aggregate_access 
                 (user_id, aggregate_type_id, aggregate_id, access_level, created_at, modified_at)
                 VALUES (?, ?, ?, ?, ?, ?)",
                )
                .await?;
            stmt.execute((
                user_id.to_le_bytes().as_slice(),
                aggregate_type_id.to_le_bytes().as_slice(),
                aggregate_id.to_le_bytes().as_slice(),
                OWNER_ACCESS_LEVEL,
                now,
                now,
            ))
            .await?;
        } else {
            // Grant owner access to the client in the aggregate database
            let mut stmt = conn_aggregate
                .prepare(
                    "INSERT INTO users_and_clients 
                 (id, is_user, access_level, created_at, modified_at)
                 VALUES (?, ?, ?, ?, ?)",
                )
                .await?;
            stmt.execute((
                client_id.to_le_bytes().as_slice(),
                0i64, // is_user = false for client
                OWNER_ACCESS_LEVEL,
                now,
                now,
            ))
            .await?;
        }

        Ok(())
    }

    pub async fn delete_aggregate_metadata(
        &self,
        org_id: u128,
        aggregate_type_id: u128,
        aggregate_id: u128,
    ) -> Result<(), MetadataError> {
        // Get aggregate connection to access the metadata
        let conn_aggregate = self
            .open_aggregate_connection(org_id, aggregate_type_id, aggregate_id)
            .await?;

        // First, get all users who have access to this aggregate so we can clean up their user databases
        let mut stmt = conn_aggregate
            .prepare("SELECT id FROM users_and_clients WHERE is_user = 1")
            .await?;
        let mut rows = stmt.query(()).await?;
        let mut user_ids = Vec::new();

        while let Some(row) = rows.next().await? {
            let user_id_bytes: Vec<u8> = row.get(0)?;
            if user_id_bytes.len() == 16 {
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&user_id_bytes);
                let user_id = u128::from_le_bytes(bytes);
                user_ids.push(user_id);
            }
        }

        // Clean up user databases - remove entries for this aggregate
        for user_id in user_ids {
            if let Ok(conn_user) = self.open_user_connection(user_id).await {
                let mut stmt = conn_user
                    .prepare(
                        "DELETE FROM user_aggregate_access 
                         WHERE org_id = ? AND aggregate_type_id = ? AND aggregate_id = ?",
                    )
                    .await?;
                stmt.execute((
                    org_id.to_le_bytes().as_slice(),
                    aggregate_type_id.to_le_bytes().as_slice(),
                    aggregate_id.to_le_bytes().as_slice(),
                ))
                .await?;
            }
        }

        // Clean up org database - remove entries for this aggregate
        let conn_org = self.open_org_connection(org_id).await?;
        let mut stmt = conn_org
            .prepare(
                "DELETE FROM user_aggregate_access 
                 WHERE aggregate_type_id = ? AND aggregate_id = ?",
            )
            .await?;
        stmt.execute((
            aggregate_type_id.to_le_bytes().as_slice(),
            aggregate_id.to_le_bytes().as_slice(),
        ))
        .await?;

        // Note: We don't need to explicitly clean up the aggregate database itself
        // as the entire database file will be deleted when the aggregate is deleted

        Ok(())
    }
}

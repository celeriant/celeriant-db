use crate::{MetadataError, store::MetadataStore};
use async_sqlite::{JournalMode, Pool, rusqlite::params};
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
            existing_access_level = conn_aggregate
                .conn(move |conn| {
                    let mut stmt = conn.prepare(
                        "SELECT access_level FROM users_and_clients WHERE id = ? AND is_user = 1",
                    )?;
                    let mut rows = stmt
                        .query_map(params![user_id.to_le_bytes().as_slice()], |row| {
                            Ok(row.get::<_, i64>(0)? as u8)
                        })?;

                    if let Some(access_level) = rows.next().transpose()? {
                        Ok(Some(access_level))
                    } else {
                        Ok(None)
                    }
                })
                .await?;
        } else {
            // Check if client has direct access
            existing_access_level = conn_aggregate
                .conn(move |conn| {
                    let mut stmt = conn.prepare(
                        "SELECT access_level FROM users_and_clients WHERE id = ? AND is_user = 0",
                    )?;
                    let mut rows = stmt
                        .query_map(params![client_id.to_le_bytes().as_slice()], |row| {
                            Ok(row.get::<_, i64>(0)? as u8)
                        })?;

                    if let Some(access_level) = rows.next().transpose()? {
                        Ok(Some(access_level))
                    } else {
                        Ok(None)
                    }
                })
                .await?;
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
        let share_data = conn_aggregate
            .conn(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT access_level, expires_at, is_single_use, use_count, disabled_at 
                     FROM share_links 
                     WHERE id = ?",
                )?;
                let mut rows =
                    stmt.query_map(params![share_id.to_le_bytes().as_slice()], |row| {
                        Ok((
                            row.get::<_, i64>(0)? as u8,   // access_level
                            row.get::<_, Option<i64>>(1)?, // expires_at
                            row.get::<_, i64>(2)? != 0,    // is_single_use
                            row.get::<_, i64>(3)?,         // use_count
                            row.get::<_, Option<i64>>(4)?, // disabled_at
                        ))
                    })?;

                if let Some(data) = rows.next().transpose()? {
                    Ok(Some(data))
                } else {
                    Ok(None)
                }
            })
            .await?;

        let Some((share_access_level, expires_at, is_single_use, use_count, disabled_at)) =
            share_data
        else {
            return Ok(false); // Share link doesn't exist
        };

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
        conn_aggregate
            .conn(move |conn| {
                let mut stmt =
                    conn.prepare("UPDATE share_links SET use_count = use_count + 1 WHERE id = ?")?;
                stmt.execute(params![share_id.to_le_bytes().as_slice()])?;
                Ok(())
            })
            .await?;

        // Grant access to the user/client
        let is_user = user_id.is_some();
        let entity_id = user_id.unwrap_or(client_id);

        conn_aggregate
            .conn(move |conn| {
                let mut stmt = conn.prepare(
                    "INSERT INTO users_and_clients 
                     (id, is_user, access_level, created_at, modified_at, granted_from_share_id)
                     VALUES (?, ?, ?, ?, ?, ?)",
                )?;
                stmt.execute(params![
                    entity_id.to_le_bytes().as_slice(),
                    is_user,
                    share_access_level,
                    now,
                    now,
                    share_id.to_le_bytes().as_slice(),
                ])?;
                Ok(())
            })
            .await?;

        // If user is logged in, also update user and org databases
        if let Some(user_id) = user_id {
            // Update user database
            let conn_user = self.open_user_connection(user_id).await?;
            conn_user
                .conn(move |conn| {
                    let mut stmt = conn.prepare(
                        "INSERT INTO user_aggregate_access 
                         (org_id, aggregate_type_id, aggregate_id, access_level, created_at, modified_at, granted_from_share_id)
                         VALUES (?, ?, ?, ?, ?, ?, ?)"
                    )?;
                    stmt.execute(params![
                        org_id.to_le_bytes().as_slice(),
                        aggregate_type_id.to_le_bytes().as_slice(),
                        aggregate_id.to_le_bytes().as_slice(),
                        share_access_level,
                        now,
                        now,
                        share_id.to_le_bytes().as_slice(),
                    ])?;
                    Ok(())
                })
                .await?;

            // Update org database
            let conn_org = self.open_org_connection(org_id).await?;
            conn_org
                .conn(move |conn| {
                    let mut stmt = conn.prepare(
                        "INSERT INTO user_aggregate_access 
                         (user_id, aggregate_type_id, aggregate_id, access_level, created_at, modified_at, granted_from_share_id)
                         VALUES (?, ?, ?, ?, ?, ?, ?)"
                    )?;
                    stmt.execute(params![
                        user_id.to_le_bytes().as_slice(),
                        aggregate_type_id.to_le_bytes().as_slice(),
                        aggregate_id.to_le_bytes().as_slice(),
                        share_access_level,
                        now,
                        now,
                        share_id.to_le_bytes().as_slice(),
                    ])?;
                    Ok(())
                })
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

        // Check if share link exists and is not already disabled, then disable it
        let rows_affected = conn_aggregate
            .conn(move |conn| {
                // Check if exists and not disabled
                let mut stmt = conn
                    .prepare("SELECT id FROM share_links WHERE id = ? AND disabled_at IS NULL")?;
                let mut rows =
                    stmt.query_map(params![share_id.to_le_bytes().as_slice()], |_| Ok(()))?;

                if rows.next().is_none() {
                    return Ok(0); // Share link doesn't exist or is already disabled
                }

                // Disable the share link
                let mut stmt = conn.prepare(
                    "UPDATE share_links 
                     SET disabled_at = ?, disabled_by_client_id = ?, disabled_by_user_id = ?
                     WHERE id = ?",
                )?;

                let affected = stmt.execute(params![
                    now,
                    client_id.to_le_bytes().as_slice(),
                    user_id.map(|id| id.to_le_bytes().to_vec()),
                    share_id.to_le_bytes().as_slice(),
                ])?;

                Ok(affected)
            })
            .await?;

        Ok(rows_affected > 0)
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

        let rows_affected = conn_aggregate
            .conn(move |conn| {
                // Check if client exists and is not a user
                let mut stmt =
                    conn.prepare("SELECT id FROM users_and_clients WHERE id = ? AND is_user = 0")?;
                let mut rows =
                    stmt.query_map(params![for_client_id.to_le_bytes().as_slice()], |_| Ok(()))?;

                if rows.next().is_none() {
                    return Ok(0); // Client doesn't exist
                }

                // Remove the client's access by deleting the record
                let mut stmt =
                    conn.prepare("DELETE FROM users_and_clients WHERE id = ? AND is_user = 0")?;
                let affected = stmt.execute(params![for_client_id.to_le_bytes().as_slice()])?;
                Ok(affected)
            })
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

        // Remove the user's access from the aggregate database
        let rows_affected = conn_aggregate
            .conn(move |conn| {
                // Check if user exists
                let mut stmt =
                    conn.prepare("SELECT id FROM users_and_clients WHERE id = ? AND is_user = 1")?;
                let mut rows =
                    stmt.query_map(params![for_user_id.to_le_bytes().as_slice()], |_| Ok(()))?;

                if rows.next().is_none() {
                    return Ok(0); // User doesn't exist in this aggregate
                }

                let mut stmt =
                    conn.prepare("DELETE FROM users_and_clients WHERE id = ? AND is_user = 1")?;
                let affected = stmt.execute(params![for_user_id.to_le_bytes().as_slice()])?;
                Ok(affected)
            })
            .await?;

        if rows_affected == 0 {
            return Ok(false);
        }

        // Also remove from user database if it exists
        if let Ok(conn_user) = self.open_user_connection(for_user_id).await {
            conn_user
                .conn(move |conn| {
                    let mut stmt = conn.prepare(
                        "DELETE FROM user_aggregate_access 
                         WHERE org_id = ? AND aggregate_type_id = ? AND aggregate_id = ?",
                    )?;
                    stmt.execute(params![
                        org_id.to_le_bytes().as_slice(),
                        aggregate_type_id.to_le_bytes().as_slice(),
                        aggregate_id.to_le_bytes().as_slice(),
                    ])?;
                    Ok(())
                })
                .await?;
        }

        // Also remove from org database
        let conn_org = self.open_org_connection(org_id).await?;
        conn_org
            .conn(move |conn| {
                let mut stmt = conn.prepare(
                    "DELETE FROM user_aggregate_access 
                     WHERE user_id = ? AND aggregate_type_id = ? AND aggregate_id = ?",
                )?;
                stmt.execute(params![
                    for_user_id.to_le_bytes().as_slice(),
                    aggregate_type_id.to_le_bytes().as_slice(),
                    aggregate_id.to_le_bytes().as_slice(),
                ])?;
                Ok(())
            })
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

        conn_aggregate
            .conn(move |conn| {
                let mut stmt = conn.prepare(
                    "INSERT INTO share_links 
                     (id, created_by_client_id, created_by_user_id, created_at, access_level, expires_at, is_single_use, use_count, disabled_at, disabled_by_client_id, disabled_by_user_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, NULL)",
                )?;

                stmt.execute(params![
                    share_id.to_le_bytes().as_slice(),
                    client_id.to_le_bytes().as_slice(),
                    user_id.map(|id| id.to_le_bytes().to_vec()),
                    now,
                    access_level,
                    expires_at,
                    is_single_use,
                ])?;
                Ok(())
            })
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
            conn_aggregate
                .conn(move |conn| {
                    let mut stmt = conn.prepare(
                        "INSERT INTO users_and_clients 
                         (id, is_user, access_level, created_at, modified_at)
                         VALUES (?, ?, ?, ?, ?)",
                    )?;
                    stmt.execute(params![
                        user_id.to_le_bytes().as_slice(),
                        1i64, // is_user = true
                        OWNER_ACCESS_LEVEL,
                        now,
                        now,
                    ])?;
                    Ok(())
                })
                .await?;

            // Update user database
            let conn_user = self.open_user_connection(user_id).await?;
            conn_user
                .conn(move |conn| {
                    let mut stmt = conn.prepare(
                        "INSERT INTO user_aggregate_access 
                         (org_id, aggregate_type_id, aggregate_id, access_level, created_at, modified_at)
                         VALUES (?, ?, ?, ?, ?, ?)",
                    )?;
                    stmt.execute(params![
                        org_id.to_le_bytes().as_slice(),
                        aggregate_type_id.to_le_bytes().as_slice(),
                        aggregate_id.to_le_bytes().as_slice(),
                        OWNER_ACCESS_LEVEL,
                        now,
                        now,
                    ])?;
                    Ok(())
                })
                .await?;

            // Update org database
            let conn_org = self.open_org_connection(org_id).await?;
            conn_org
                .conn(move |conn| {
                    let mut stmt = conn.prepare(
                        "INSERT INTO user_aggregate_access 
                         (user_id, aggregate_type_id, aggregate_id, access_level, created_at, modified_at)
                         VALUES (?, ?, ?, ?, ?, ?)",
                    )?;
                    stmt.execute(params![
                        user_id.to_le_bytes().as_slice(),
                        aggregate_type_id.to_le_bytes().as_slice(),
                        aggregate_id.to_le_bytes().as_slice(),
                        OWNER_ACCESS_LEVEL,
                        now,
                        now,
                    ])?;
                    Ok(())
                })
                .await?;
        } else {
            // Grant owner access to the client in the aggregate database
            conn_aggregate
                .conn(move |conn| {
                    let mut stmt = conn.prepare(
                        "INSERT INTO users_and_clients 
                         (id, is_user, access_level, created_at, modified_at)
                         VALUES (?, ?, ?, ?, ?)",
                    )?;
                    stmt.execute(params![
                        client_id.to_le_bytes().as_slice(),
                        0i64, // is_user = false for client
                        OWNER_ACCESS_LEVEL,
                        now,
                        now,
                    ])?;
                    Ok(())
                })
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
        let user_ids = conn_aggregate
            .conn(|conn| {
                let mut stmt =
                    conn.prepare("SELECT id FROM users_and_clients WHERE is_user = 1")?;
                let mut rows = stmt.query_map(params![], |row| {
                    let user_id_bytes: Vec<u8> = row.get(0)?;
                    if user_id_bytes.len() == 16 {
                        let mut bytes = [0u8; 16];
                        bytes.copy_from_slice(&user_id_bytes);
                        Ok(Some(u128::from_le_bytes(bytes)))
                    } else {
                        Ok(None)
                    }
                })?;

                let mut user_ids = Vec::new();
                for row in rows {
                    if let Some(user_id) = row? {
                        user_ids.push(user_id);
                    }
                }
                Ok(user_ids)
            })
            .await?;

        // Clean up user databases - remove entries for this aggregate
        for user_id in user_ids {
            if let Ok(conn_user) = self.open_user_connection(user_id).await {
                conn_user
                    .conn(move |conn| {
                        let mut stmt = conn.prepare(
                            "DELETE FROM user_aggregate_access 
                             WHERE org_id = ? AND aggregate_type_id = ? AND aggregate_id = ?",
                        )?;
                        stmt.execute(params![
                            org_id.to_le_bytes().as_slice(),
                            aggregate_type_id.to_le_bytes().as_slice(),
                            aggregate_id.to_le_bytes().as_slice(),
                        ])?;
                        Ok(())
                    })
                    .await?;
            }
        }

        // Clean up org database - remove entries for this aggregate
        let conn_org = self.open_org_connection(org_id).await?;
        conn_org
            .conn(move |conn| {
                let mut stmt = conn.prepare(
                    "DELETE FROM user_aggregate_access 
                     WHERE aggregate_type_id = ? AND aggregate_id = ?",
                )?;
                stmt.execute(params![
                    aggregate_type_id.to_le_bytes().as_slice(),
                    aggregate_id.to_le_bytes().as_slice(),
                ])?;
                Ok(())
            })
            .await?;

        // Note: We don't need to explicitly clean up the aggregate database itself
        // as the entire database file will be deleted when the aggregate is deleted

        Ok(())
    }
}

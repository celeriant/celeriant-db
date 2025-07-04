use std::{collections::HashMap, io, usize};

use event_storage::{event_batch_item::EventBatchItem, event_item::EventItem, event_storage_cache::{EventStorageCache}};

use crate::{access_level::AccessLevel, project_to_user_access_level::{ProjectToUserAccessLevel}};

pub struct UserAccessCache {
    cache_queue: Vec<String>,
    cache: HashMap<String, ProjectToUserAccessLevel>,
    last_cleared_cache: chrono::DateTime<chrono::Utc>,
    cache_check_time: chrono::Duration,
    cache_max_project_count: usize,
}

impl UserAccessCache {
    pub fn new(
        cache_check_time_hours: u64,
        cache_max_project_count: usize,
    ) -> Self {
        Self {
            cache_queue: Vec::new(),
            cache: HashMap::new(),
            last_cleared_cache: chrono::Utc::now(),
            cache_check_time: chrono::Duration::hours(cache_check_time_hours as i64),
            cache_max_project_count,
        }
    }

    fn clear_cache(&mut self) {
        if chrono::Utc::now().signed_duration_since(self.last_cleared_cache) < self.cache_check_time {
            return;
        }

        if self.cache.len() < self.cache_max_project_count {
            return;
        }

        self.last_cleared_cache = chrono::Utc::now();

        while self.cache.len() > self.cache_max_project_count {
            if let Some(file_path) = self.cache_queue.pop() {
                self.cache.remove(&file_path);
            } else {
                break;
            }
        }
    }

    fn get_or_build_cache(
        &mut self,
        event_storage_cache: &mut EventStorageCache,
        file_path: &str,
    ) -> &mut ProjectToUserAccessLevel {
        self.clear_cache();

        if self.cache.contains_key(file_path) {
            return self.cache.get_mut(file_path).unwrap();
        }

        self.cache.insert(file_path.to_string(), ProjectToUserAccessLevel::new());
        self.cache_queue.push(file_path.to_string());

        self.populate_cache_for_project(event_storage_cache, file_path);

        return self.cache.get_mut(file_path).unwrap();
    }

    fn populate_cache_for_project(&mut self, event_storage_cache: &mut EventStorageCache, file_path: &str) {
        let project_to_user_access_level = self.cache.get_mut(file_path).unwrap();

        match event_storage_cache.read(file_path, 0, usize::MAX, Some(ProjectEventType::ProvideAccess as u64)) {
            Ok(result) => {
                for batch in result.event_batches  {
                    for event in batch.events.iter() {
                        if event.tp != ProjectEventType::ProvideAccess as u64 {
                            continue;
                        }
                        if event.string_values.as_ref().is_none() || event.string_values.as_ref().unwrap().len() < 1 {
                            continue;
                        }
                        if event.string_values.as_ref().unwrap()[1].is_none() {
                            continue;
                        }
                        if event.uint_values.is_none() || event.uint_values.as_ref().unwrap().len() == 0 {
                            continue;
                        }

                        let user_hash = event.string_values.as_ref().unwrap()[0].as_ref().unwrap().clone();
                        let access_level = AccessLevel::from(event.uint_values.as_ref().unwrap()[0]);

                        project_to_user_access_level.update_cache_for_user(&user_hash, access_level, true);
                    }
                }
            },
            Err(_) => { }
        }
    }

    pub fn get_current_access_level(&mut self, event_storage_cache: &mut EventStorageCache, file_path: &str, user_hash: &str) -> AccessLevel {
        let project_to_user_access_level = self.get_or_build_cache(event_storage_cache, file_path);
        project_to_user_access_level.current_access_level_for_user(user_hash)
    }

    pub fn update_access_for_user(&mut self, 
        event_storage_cache: &mut EventStorageCache, 
        file_path: &str, 
        current_user_hash: &str, 
        for_user_hash: &str,
        potential_access_level: AccessLevel,
        allow_downgrade: bool, 
        share_key: Option<&str>, 
        ed_override: Option<u64>) -> io::Result<Option<EventItem>> {

        //Not allowed to downgrade your own permissions
        if allow_downgrade && current_user_hash == for_user_hash
        {
            return Ok(None);
        }

        let current_access_level = self.get_current_access_level(event_storage_cache, file_path, for_user_hash);

        //No op as same permission level or lower level and not downgrading
        if current_access_level == potential_access_level || !allow_downgrade && !AccessLevel::increases_access_level(current_access_level, potential_access_level)
        {
            return Ok(None);
        }

        let current_time = ed_override.unwrap_or(chrono::Utc::now().timestamp_millis() as u64);

        let mut event_item = EventItem::new();
        event_item.ed = current_time;
        event_item.tp = ProjectEventType::ProvideAccess as u64;
        event_item.string_values = Some(vec![Some(for_user_hash.to_string()), share_key.map_or(None,|f| Some(f.to_string()))]);
        event_item.uint_values = Some(vec![potential_access_level as u64]);

        let mut event_batch_item = EventBatchItem::new();
        event_batch_item.events = vec![event_item.clone()];
        event_batch_item.cb = Some(current_user_hash.to_string());
        event_batch_item.sd = current_time;

        event_storage_cache.write(file_path, false, event_batch_item)?;

        let project_to_user_access_level = self.get_or_build_cache(event_storage_cache, file_path);
        project_to_user_access_level.update_cache_for_user(for_user_hash, potential_access_level, allow_downgrade);
        
        Ok(Some(event_item))
    }
    
}

pub enum ProjectEventType
{
    AddTask,
    SetParent,
    SetTaskSummary,
    SetTaskStatus,
    CollapseTask,
    RemoveTask,
    SetDueDate,
    SetAssignedTo,
    SetEstimate,
    UnsetTaskStatus,
    SetLink,
    SetConfidence,
    AddPredecessor,
    AddSuccessor,
    BeginStandup,
    RemovePredecessor,
    RemoveSuccessor,
    SetProjectOwner,
    AddProjectMember,
    AddRole,
    SetRoleName,
    SetRoleIsActive,
    AddTeamMember,
    SetTeamMemberName,
    SetTeamMemberHours,
    AddTeamMemberRoleId,
    RemoveTeamMemberRoleId,
    SetTeamMemberIsActive,
    SetRoleId,
    SetTeamMemberAuthId,
    SetDefaultTaskDuration,
    StandupCompleted,
    StandupItemTime,
    AddItemToStandup,
    StandupNextItem,
    RetroStart,
    RetroCancel,
    RetroEnd,
    RetroDiscussionItemAdd,
    RetroDiscussionItemDelete,
    RetroDiscussionItemGroup,
    RetroMakeVisible,
    RemoveProjectMember,
    AddShareLink,
    AddSingleUseShareLink,
    ProvideAccess,
    DisableShareLink,
    SetProjectDescription,
    SaveNodePositions,
    CreateAssistant,
    DeleteAssistant,
    AddAssistantFile,
    DeleteAssistantFile
}
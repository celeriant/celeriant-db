namespace UtilityDelta.WebAPI.Data
{
    public enum ProjectEventType
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
        AddProjectMember
    }

    public enum ProjectAccess
    {
        NoAccess,
        WriteAccess,
        ReadOnlyAccess,
        NotExists,
    }
}

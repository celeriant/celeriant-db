using UtilityDelta.Projects.Shared;

namespace UtilityDelta.AiTooling.Dtos
{
    public class DtoBreakdownInputs
    {
        public string? system { get; set; }
        public string? task { get; set; }
        public string[] parents { get; set; } = [];
        public string[] siblings { get; set; } = [];
        public string[] yesQuestions { get; set; } = [];
        public string[] noQuestions { get; set; } = [];
        public string[] unsureQuestions { get; set; } = [];
        public string[] unansweredQuestions { get; set; } = [];
        public string? userNotes { get; set; }
        public double minDuration { get; set; }
        public bool skipDependencies { get; set; }
    }

    public record DtoAssistantChanges(List<ProjectEventItem> events);

    public class DtoQuestion
    {
        public string question { get; set; } = string.Empty;
    }

    public class DtoAssignRolesInputs
    {
        public string? system { get; set; }
        public string[] parents { get; set; } = [];
        public string[] tasks { get; set; } = [];
        public string[] roles { get; set; } = [];
    }

    public class DtoOrganiseInputs
    {
        public string? system { get; set; }
        public string[] parents { get; set; } = [];
        public string[] tasks { get; set; } = [];
    }

    public class DtoRolesInputs
    {
        public string? system { get; set; }
        public string[] tasks { get; set; } = [];
    }

    public class DtoBreakdownOutputs
    {
        public string[] subTasks { get; set; } = [];
        public string[] predecessors { get; set; } = [];
        public string[] successors { get; set; } = [];
    }

    public class DtoBreakdownQuestionsOutputs
    {
        public string[] questions { get; set; } = [];
    }

    public class DtoUnknownOutputs
    {
        public int[] unkownTasks { get; set; } = [];
    }

    public class DtoAssignRolesOutputs
    {
        public int[] taskNumbers { get; set; } = [];
        public string[] roles { get; set; } = [];
    }

    public class DtoOrganiseOutputs
    {
        public int[] taskNumbers { get; set; } = [];
        public string[] taskGroups { get; set; } = [];
    }

    public class DtoRolesOutputs
    {
        public string[] roles { get; set; } = [];
    }
}

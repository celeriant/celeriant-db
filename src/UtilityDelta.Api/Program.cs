using System.Text.Json.Serialization;
using UtilityDelta.Api.Service;
using UtilityDelta.Api.ServiceInterface;

internal class Program
{
    private static void Main(string[] args)
    {
        var builder = WebApplication.CreateSlimBuilder(args);

        builder.Services.AddSingleton<IMyTodos, MyTodos>();

        builder.Services.ConfigureHttpJsonOptions(options =>
        {
            options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonSerializerContext.Default);
        });

        var app = builder.Build();

        var myTodos = app.Services.GetService<IMyTodos>();

        var todosApi = app.MapGroup("/todos");

        todosApi.MapGet("/", () => myTodos!.Todos);

        todosApi.MapGet("/{id}", (int id) =>
            myTodos!.Todos.FirstOrDefault(a => a.Id == id) is { } todo
                ? Results.Ok(todo)
                : Results.NotFound());

        app.Run();
    }
}

public record Todo(int Id, string? Title, DateOnly? DueBy = null, bool IsComplete = false);

[JsonSerializable(typeof(Todo[]))]
internal partial class AppJsonSerializerContext : JsonSerializerContext
{

}

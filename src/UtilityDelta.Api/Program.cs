using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using NanoidDotNet;
using System.Net;
using System.Text.Json.Serialization;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Services;
using UtilityDelta.Api.Shared;

[JsonSerializable(typeof(ProjectEventItem[]))]
[JsonSerializable(typeof(List<ProjectEventItem>))]
[JsonSerializable(typeof(DtoRead))]
[JsonSerializable(typeof(DtoShare))]
[JsonSerializable(typeof(DtoWrite))]
public partial class ReadSerializerContext : JsonSerializerContext
{

}

public class Program
{
    private static IResult Read(
        [FromQuery] string pi,
        [FromQuery] string publicKey,
        [FromQuery] string nonce,
        [FromQuery] string sign,
        [FromQuery] long fromTime,
        [FromQuery] bool createIfNotExist,
        [FromQuery] string? shareKey,
        CancellationToken cancellationToken,
        [FromServices] IReadEvents readEvents,
        [FromServices] IAccessLogic accessLogic)
    {
        var (accessResult, currentUserHash) = accessLogic.IsProjectExistAndHasAccess(
            projectId: pi,
            createProjectIfNotExists: createIfNotExist && fromTime == 0,
            shareKey: shareKey,
            publicKey: publicKey, 
            nonce: nonce, 
            sign: sign);

        return accessResult switch
        {
            ProjectAccess.NotExists => Results.NotFound(),
            ProjectAccess.NoAccess => Results.Forbid(),
            _ => Results.Ok(readEvents.Read(pi, fromTime, currentUserHash))
        };
    }

    private static IResult Share(
        [FromQuery] string pi,
        [FromQuery] string publicKey,
        [FromQuery] string nonce,
        [FromQuery] string sign,
        [FromQuery] bool isOwner,
        [FromQuery] bool singleUse,
        [FromQuery] string? description,
        [FromQuery] long expiresOn,
        [FromQuery] bool readOnly,
        CancellationToken cancellationToken,
        [FromServices] IAccessLogic accessLogic)
    {
        var (accessResult, currentUserHash) = accessLogic.IsProjectExistAndHasAccess(
            projectId: pi,
            createProjectIfNotExists: false,
            shareKey: null,
            publicKey: publicKey,
            nonce: nonce,
            sign: sign);

        return accessResult switch
        {
            ProjectAccess.NotExists => Results.NotFound(),
            ProjectAccess.OwnerAccess => Results.Ok(accessLogic.CreateShareLink(pi, currentUserHash, isOwner, singleUse, description, expiresOn, readOnly)),
            _ => Results.Forbid()
        };
    }

    private static IResult Write(
        [FromQuery] string pi,
        [FromQuery] string publicKey,
        [FromQuery] string nonce,
        [FromQuery] string sign,
        [FromQuery] bool createIfNotExist,
        [FromBody] ProjectEventItem[] events,
        CancellationToken cancellationToken,
        [FromServices] IWriteEvents writeEvents,
        [FromServices] IAccessLogic accessLogic)
    {
        var (accessResult, currentUserHash) = accessLogic.IsProjectExistAndHasAccess(
            projectId: pi,
            createProjectIfNotExists: false,
            shareKey: null,
            publicKey: publicKey,
            nonce: nonce,
            sign: sign);

        return accessResult switch
        {
            ProjectAccess.NotExists => Results.NotFound(),
            ProjectAccess.NoAccess => Results.Forbid(),
            ProjectAccess.ReadOnlyAccess => Results.Forbid(),
            _ => Results.Ok(writeEvents.Write(events, currentUserHash, pi))
        };
    }

    private static void Main(string[] args)
    {
        var app = SetupApplication(args);

        var api = app.MapGroup("/api");
        
        api.MapGet("/read", Read);
        api.MapPost("/share", Share);
        api.MapPost("/write", Write);

        Directory.CreateDirectory(Constants.SUB_DIR_CONTAINERS);

        app.Run();
    }

    private static WebApplication SetupApplication(string[] args)
    {
        var builder = WebApplication.CreateSlimBuilder(args);

        builder.Services.ConfigureHttpJsonOptions(options =>
        {
            options.SerializerOptions.TypeInfoResolverChain.Insert(0, ReadSerializerContext.Default);
        });

        builder.Services.AddCors(
            (options) => options.AddPolicy("CorsDevelopment",
                    builder =>
                    {
                        builder
                        .WithOrigins("http://localhost:5173")
                        .AllowAnyMethod()
                        .AllowAnyHeader()
                        .AllowCredentials();

                        builder
                        .WithOrigins("https://app.utilitydelta.io")
                        .AllowAnyMethod()
                        .AllowAnyHeader()
                        .AllowCredentials();

                        builder
                        .WithOrigins("https://test.utilitydelta.io")
                        .AllowAnyMethod()
                        .AllowAnyHeader()
                        .AllowCredentials();
                    }));

        builder.Services.AddSingleton<ICrypto, Crypto>();
        builder.Services.AddSingleton<IReadEvents, ReadEvents>();
        builder.Services.AddSingleton<IWriteEvents, WriteEvents>();
        builder.Services.AddSingleton<IAccessLogic, AccessLogic>();

        var app = builder.Build();
        app.UseCors("CorsDevelopment");
        return app;
    }
}
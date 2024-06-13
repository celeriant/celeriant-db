using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using NanoidDotNet;
using System.Globalization;
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
        var accessInfo = accessLogic.IsProjectExistAndHasAccess(
            projectId: pi,
            createProjectIfNotExists: createIfNotExist && fromTime == 0,
            shareKey: shareKey,
            publicKey: publicKey, 
            nonce: nonce, 
            sign: sign);

        return accessInfo.ProjectAccess switch
        {
            ProjectAccess.NotExists => Results.NotFound(),
            ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
            _ => Results.Ok(readEvents.Read(pi, fromTime, accessInfo.CurrentUserHash))
        };
    }

    private static IResult DisableUser(
        [FromQuery] string pi,
        [FromQuery] string publicKey,
        [FromQuery] string nonce,
        [FromQuery] string sign,
        [FromQuery] string userId,
        CancellationToken cancellationToken,
        [FromServices] IUserAccessCache userAccessCache,
        [FromServices] IAccessLogic accessLogic)
    {
        var accessInfo = accessLogic.IsProjectExistAndHasAccess(
            projectId: pi,
            createProjectIfNotExists: false,
            shareKey: null,
            publicKey: publicKey,
            nonce: nonce,
            sign: sign);

        return accessInfo.ProjectAccess switch
        {
            ProjectAccess.NotExists => Results.NotFound(),
            ProjectAccess.OwnerAccess => Results.Ok(new DtoDisableAccess(userAccessCache.UpdateAccess(pi, accessInfo.CurrentUserHash, userId, null, null, true, null))),
            _ => Results.StatusCode(StatusCodes.Status403Forbidden)
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
        [FromServices] IAccessLogic accessLogic,
        [FromServices] IShareKeyCache shareKeyCache)
    {
        var accessInfo = accessLogic.IsProjectExistAndHasAccess(
            projectId: pi,
            createProjectIfNotExists: false,
            shareKey: null,
            publicKey: publicKey,
            nonce: nonce,
            sign: sign);

        return accessInfo.ProjectAccess switch
        {
            ProjectAccess.NotExists => Results.NotFound(),
            ProjectAccess.OwnerAccess => Results.Ok(shareKeyCache.CreateShareLink(pi, accessInfo.CurrentUserHash, isOwner, singleUse, description, expiresOn, readOnly)),
            _ => Results.StatusCode(StatusCodes.Status403Forbidden)
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
        var accessInfo = accessLogic.IsProjectExistAndHasAccess(
            projectId: pi,
            createProjectIfNotExists: false,
            shareKey: null,
            publicKey: publicKey,
            nonce: nonce,
            sign: sign);

        return accessInfo.ProjectAccess switch
        {
            ProjectAccess.NotExists => Results.NotFound(),
            ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
            ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
            _ => Results.Ok(writeEvents.WriteClientEvents(events, accessInfo.CurrentUserHash, pi))
        };
    }

    private static void Main(string[] args)
    {
        var app = SetupApplication(args);

        var api = app.MapGroup("/api");
        
        api.MapGet("/read", Read);
        api.MapPost("/disableuser", DisableUser);
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
        builder.Services.AddSingleton<IShareKeyCache, ShareKeyCache>();
        builder.Services.AddSingleton<IUserAccessCache, UserAccessCache>();

        var app = builder.Build();
        app.UseCors("CorsDevelopment");
        return app;
    }
}
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using NanoidDotNet;
using System.Globalization;
using System.Net;
using System.Text.Json.Serialization;
using System.Threading.RateLimiting;
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
    private static async Task<IResult> Read(
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
        return await Task.Run(() =>
        {
            var accessInfo = accessLogic.IsProjectExistAndHasAccess(
            projectId: pi,
            createProjectIfNotExists: createIfNotExist && fromTime == 0,
            shareKey: shareKey,
            publicKey: publicKey,
            nonce: nonce,
            sign: sign,
            cancellationToken: cancellationToken);

            return accessInfo.ProjectAccess switch
            {
                ProjectAccess.NotExists => Results.NotFound(),
                ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                _ => Results.Ok(readEvents.Read(pi, fromTime, cancellationToken, accessInfo.CurrentUserHash))
            };
        });
    }

    private static async Task<IResult> DisableUser(
        [FromQuery] string pi,
        [FromQuery] string publicKey,
        [FromQuery] string nonce,
        [FromQuery] string sign,
        [FromQuery] string userId,
        CancellationToken cancellationToken,
        [FromServices] IUserAccessCache userAccessCache,
        [FromServices] IAccessLogic accessLogic)
    {
        return await Task.Run(() =>
        {
            var accessInfo = accessLogic.IsProjectExistAndHasAccess(
            projectId: pi,
            createProjectIfNotExists: false,
            shareKey: null,
            publicKey: publicKey,
            nonce: nonce,
            sign: sign,
            cancellationToken: cancellationToken);

            return accessInfo.ProjectAccess switch
            {
                ProjectAccess.NotExists => Results.NotFound(),
                ProjectAccess.OwnerAccess => Results.Ok(new DtoDisableAccess(userAccessCache.UpdateAccess(pi, accessInfo.CurrentUserHash, userId, null, null, true, null, cancellationToken))),
                _ => Results.StatusCode(StatusCodes.Status403Forbidden)
            };
        });
    }

    private static async Task<IResult> Share(
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
        return await Task.Run(() =>
        {
            var accessInfo = accessLogic.IsProjectExistAndHasAccess(
            projectId: pi,
            createProjectIfNotExists: false,
            shareKey: null,
            publicKey: publicKey,
            nonce: nonce,
            sign: sign,
            cancellationToken: cancellationToken);

            return accessInfo.ProjectAccess switch
            {
                ProjectAccess.NotExists => Results.NotFound(),
                ProjectAccess.OwnerAccess => Results.Ok(shareKeyCache.CreateShareLink(pi, accessInfo.CurrentUserHash, isOwner, singleUse, description, expiresOn, readOnly, cancellationToken)),
                _ => Results.StatusCode(StatusCodes.Status403Forbidden)
            };
        });
    }

    private static async Task<IResult> Write(
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
        return await Task.Run(() =>
        {
            var accessInfo = accessLogic.IsProjectExistAndHasAccess(
                projectId: pi,
                createProjectIfNotExists: false,
                shareKey: null,
                publicKey: publicKey,
                nonce: nonce,
                sign: sign,
                cancellationToken: cancellationToken);

            return accessInfo.ProjectAccess switch
            {
                ProjectAccess.NotExists => Results.NotFound(),
                ProjectAccess.NoAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                ProjectAccess.ReadOnlyAccess => Results.StatusCode(StatusCodes.Status403Forbidden),
                _ => Results.Ok(writeEvents.WriteClientEvents(events, accessInfo.CurrentUserHash, pi, cancellationToken))
            };
        });
    }

    private static void Main(string[] args)
    {
        var app = SetupApplication(args);

        var api = app.MapGroup("/api");
        
        api.MapGet("/read", Read);
        api.MapPost("/disableuser", DisableUser);
        api.MapPost("/share", Share);
        api.MapPost("/write", Write);

        var udConfig = app.Services.GetService<IOptions<ConfigurationEntry>>()!;
        Directory.CreateDirectory(udConfig.Value.SUB_DIR_CONTAINERS);

        app.Run();
    }

    public class MyRateLimitOptions
    {
        public const string MyRateLimit = "MyRateLimit";
        public int PermitLimit { get; set; } = 100;
        public int Window { get; set; } = 10;
        public int ReplenishmentPeriod { get; set; } = 2;
        public int QueueLimit { get; set; } = 2;
        public int SegmentsPerWindow { get; set; } = 8;
        public int TokenLimit { get; set; } = 10;
        public int TokenLimit2 { get; set; } = 20;
        public int TokensPerPeriod { get; set; } = 4;
        public bool AutoReplenishment { get; set; } = false;
    }

    private static WebApplication SetupApplication(string[] args)
    {
        var builder = WebApplication.CreateSlimBuilder(args);

        builder.Services.ConfigureHttpJsonOptions(options =>
        {
            options.SerializerOptions.TypeInfoResolverChain.Insert(0, ReadSerializerContext.Default);
        });

        var isDevelopment = builder.Environment.IsDevelopment();

        if (!isDevelopment)
        {
            builder.Services.AddRateLimiter((limiterOptions) =>
            {
                limiterOptions.GlobalLimiter = PartitionedRateLimiter.Create<HttpContext, IPAddress>(context =>
                {
                    var myOptions = new MyRateLimitOptions();
                    IPAddress? remoteIpAddress = context.Connection.RemoteIpAddress;

                    if (!IPAddress.IsLoopback(remoteIpAddress!))
                    {
                        return RateLimitPartition.GetTokenBucketLimiter
                        (remoteIpAddress!, _ =>
                            new TokenBucketRateLimiterOptions
                            {
                                TokenLimit = myOptions.TokenLimit2,
                                QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
                                QueueLimit = myOptions.QueueLimit,
                                ReplenishmentPeriod = TimeSpan.FromSeconds(myOptions.ReplenishmentPeriod),
                                TokensPerPeriod = myOptions.TokensPerPeriod,
                                AutoReplenishment = myOptions.AutoReplenishment
                            });
                    }

                    return RateLimitPartition.GetNoLimiter(IPAddress.Loopback);
                });
            });
        }

        builder.Services.AddCors(
            (options) => options.AddPolicy("CorsDevelopment",
                    builder =>
                    {
                        if (isDevelopment)
                        {
                            builder
                                .WithOrigins("http://localhost:5173")
                                .AllowAnyMethod()
                                .AllowAnyHeader()
                                .AllowCredentials();
                        }

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
        builder.Services.AddSingleton<IFileHandlesManager, FileHandlesManager>();

        var utilityDeltaConfiguration = builder.Configuration.GetSection("UtilityDelta");
        builder.Services.Configure<ConfigurationEntry>(utilityDeltaConfiguration);

        var app = builder.Build();
        app.UseCors("CorsDevelopment");

        if (!isDevelopment)
        {
            app.UseRateLimiter();
        }
        return app;
    }
}
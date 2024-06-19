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
[JsonSerializable(typeof(DtoDisableAccess))]
public partial class ReadSerializerContext : JsonSerializerContext
{

}

public class Program
{
    private static void Main(string[] args)
    {
        var app = SetupApplication(args);

        var api = app.MapGroup("/api");

        var endpoints = app.Services.GetService<IEndpoints>()!;

        api.MapGet("/read", endpoints.Read);
        api.MapPost("/disableuser", endpoints.DisableUser);
        api.MapPost("/disableshare", endpoints.DisableShare);
        api.MapPost("/share", endpoints.Share);
        api.MapPost("/write", endpoints.Write);

        var udConfig = app.Services.GetService<IOptions<ConfigurationEntry>>()!;
        Directory.CreateDirectory(udConfig.Value.SUB_DIR_CONTAINERS);

        var writeAndBackup = app.Services.GetService<IWriteAndBackup>()!;
        _ = Task.Run(writeAndBackup.ProcessQueue);

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
        builder.Services.AddSingleton<IWriteAndBackup, WriteAndBackup>();
        builder.Services.AddSingleton<IAccessLogic, AccessLogic>();
        builder.Services.AddSingleton<IShareKeyCache, ShareKeyCache>();
        builder.Services.AddSingleton<IUserAccessCache, UserAccessCache>();
        builder.Services.AddSingleton<IFileHandlesManager, FileHandlesManager>();
        builder.Services.AddSingleton<IEndpoints, Endpoints>();

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
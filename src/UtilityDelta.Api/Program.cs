using Microsoft.AspNetCore.Mvc;
using System.Text.Json.Serialization;
using UtilityDelta.Api.Interfaces;
using UtilityDelta.Api.Services;
using UtilityDelta.Api.Shared;

[JsonSerializable(typeof(ProjectEventItem[]))]
[JsonSerializable(typeof(List<ProjectEventItem>))]
[JsonSerializable(typeof(DtoShareProject))]
[JsonSerializable(typeof(DtoSyncEvents))]
public partial class ReadSerializerContext : JsonSerializerContext
{

}

public class Program
{
    private static List<ProjectEventItem> Read(
        [FromQuery] string pi,
        [FromQuery] string publicKey,
        [FromQuery] string nonce,
        [FromQuery] string sign,
        [FromQuery] long fromTime,
        [FromQuery] bool createIfNotExist,
        [FromQuery] string? shareKey,
        CancellationToken cancellationToken,
        [FromServices] IReadEvents readEvents,
        [FromServices] ICrypto crypto)
    {
        crypto.ValidateWithPublicKey(publicKey, nonce, sign);

        //TODO: Verify access to project

        var createdBy = publicKey.CalculateHash();

        return readEvents.Read(pi, fromTime, createdBy);
    }

    private static DtoShareProject Share(
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
        [FromServices] ICrypto crypto)
    {
        crypto.ValidateWithPublicKey(publicKey, nonce, sign);
        //TODO: Verify access to project

        return new DtoShareProject("jlksdjlksdfjkl");
    }

    private static DtoSyncEvents Write(
        [FromQuery] string pi,
        [FromQuery] string publicKey,
        [FromQuery] string nonce,
        [FromQuery] string sign,
        [FromQuery] bool createIfNotExist,
        [FromBody] ProjectEventItem[] events,
        CancellationToken cancellationToken,
        [FromServices] IWriteEvents writeEvents,
        [FromServices] ICrypto crypto)
    {
        crypto.ValidateWithPublicKey(publicKey, nonce, sign);

        var createdBy = publicKey.CalculateHash();

        //TODO: Verify access to project

        return new DtoSyncEvents(writeEvents.Write(events, createdBy, pi));
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

        var app = builder.Build();
        app.UseCors("CorsDevelopment");
        return app;
    }
}